from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import httpx
from aether_contracts import AircraftPositionEvent
from aiokafka import AIOKafkaProducer  # type: ignore[import-untyped]
from prometheus_client import start_http_server

from aether_ingestor.config import BoundingBox, IngestorSettings
from aether_ingestor.fixture_source import FixtureSource
from aether_ingestor.logging import configure_logging, get_logger
from aether_ingestor.metrics import (
    ingestor_events_published_total,
    ingestor_poll_duration_seconds,
    ingestor_polls_total,
    ingestor_rows_rejected_total,
)
from aether_ingestor.normalizer import normalize_response
from aether_ingestor.opensky import OpenSkyClient, OpenSkyRateLimited, OpenSkyResponse
from aether_ingestor.producer import AioKafkaEventProducer, KafkaException


class Clock(Protocol):
    def now(self) -> datetime: ...


class PollClient(Protocol):
    async def fetch_states(self, bbox: BoundingBox) -> OpenSkyResponse: ...


class PollProducer(Protocol):
    async def publish(self, event: AircraftPositionEvent) -> None: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(tz=UTC)


@dataclass(slots=True)
class PollResult:
    fetched: int
    published: int
    rejected: int


class Backoff:
    def __init__(
        self,
        *,
        initial_seconds: float,
        max_seconds: float,
        jitter_ratio: float = 0.2,
        random_fn: Callable[[], float] = random.random,
    ) -> None:
        self._initial = initial_seconds
        self._max = max_seconds
        self._jitter_ratio = jitter_ratio
        self._random = random_fn
        self._current = initial_seconds

    def next_delay_with_jitter(self) -> float:
        base = self._current
        self._current = min(self._current * 2, self._max)
        jitter_span = base * self._jitter_ratio
        jitter = (self._random() * 2 - 1) * jitter_span
        return max(0.0, base + jitter)

    def reset(self) -> None:
        self._current = self._initial


class FixturePollClient:
    def __init__(self, fixture_source: FixtureSource, clock: Clock) -> None:
        self._fixture_source = fixture_source
        self._clock = clock

    async def fetch_states(self, _bbox: BoundingBox) -> OpenSkyResponse:
        return self._fixture_source.fetch_states(self._clock.now())


async def run_poll_cycle(
    client: PollClient,
    producer: PollProducer,
    clock: Clock,
    bbox: BoundingBox,
) -> PollResult:
    ingestor_polls_total.inc()
    start = time.perf_counter()
    try:
        response = await client.fetch_states(bbox)
        normalized = normalize_response(response, clock.now())
        for event in normalized.events:
            await producer.publish(event)
    finally:
        ingestor_poll_duration_seconds.observe(time.perf_counter() - start)

    ingestor_rows_rejected_total.inc(len(normalized.rejections))
    ingestor_events_published_total.inc(len(normalized.events))

    return PollResult(
        fetched=len(response.states or []),
        published=len(normalized.events),
        rejected=len(normalized.rejections),
    )


async def run_forever(
    *,
    settings: IngestorSettings,
    client: PollClient,
    producer: PollProducer,
    clock: Clock,
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    backoff: Backoff | None = None,
) -> None:
    retry = backoff or Backoff(
        initial_seconds=settings.backoff_initial_seconds,
        max_seconds=settings.backoff_max_seconds,
    )

    while True:
        try:
            await run_poll_cycle(client, producer, clock, settings.europe_bbox)
        except OpenSkyRateLimited as exc:
            delay = max(float(exc.retry_after_seconds), float(settings.poll_interval_seconds))
        except (httpx.TransportError, KafkaException):
            delay = retry.next_delay_with_jitter()
        else:
            retry.reset()
            delay = float(settings.poll_interval_seconds)

        await sleeper(delay)


async def run_service(settings: IngestorSettings) -> None:
    configure_logging()
    start_http_server(settings.metrics_port)
    logger = get_logger("aether.ingestor")
    clock = SystemClock()

    kafka = AIOKafkaProducer(
        bootstrap_servers=settings.redpanda_brokers,
        acks="all",
        enable_idempotence=True,
    )
    await kafka.start()
    producer = AioKafkaEventProducer(kafka, topic=settings.redpanda_topic)

    client: PollClient
    opensky_client: OpenSkyClient | None = None
    if settings.source_mode == "fixture":
        client = FixturePollClient(FixtureSource(settings), clock)
    else:
        opensky_client = OpenSkyClient(settings)
        client = opensky_client

    logger.info("ingestor.start", source_mode=settings.source_mode, topic=settings.redpanda_topic)
    try:
        await run_forever(settings=settings, client=client, producer=producer, clock=clock)
    finally:
        if opensky_client is not None:
            await opensky_client.aclose()
        await kafka.stop()
        logger.info("ingestor.stop")


async def main() -> None:
    await run_service(IngestorSettings.from_env())


if __name__ == "__main__":
    asyncio.run(main())
