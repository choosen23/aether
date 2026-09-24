from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import httpx
import pytest
from aether_contracts import AircraftPositionEvent
from aether_ingestor.config import BoundingBox, IngestorSettings
from aether_ingestor.main import Backoff, PollResult, run_forever, run_poll_cycle
from aether_ingestor.opensky import OpenSkyRateLimited, OpenSkyResponse

RESPONSE = OpenSkyResponse(
    time=1_798_000_000,
    states=[
        [
            "3c6444",
            "DLH4YA",
            "Germany",
            1_798_000_000,
            1_798_000_000,
            8.56,
            50.04,
            10668.0,
            False,
            232.1,
            271.4,
            -1.2,
            None,
            None,
            None,
            False,
            0,
        ],
        ["bad"],
        [
            "a1b2c3",
            None,
            "France",
            1_798_000_005,
            1_798_000_005,
            2.35,
            48.85,
            None,
            False,
            180.0,
            90.0,
            0.0,
            None,
            None,
            None,
            False,
            0,
        ],
    ],
)


class FakeOpenSky:
    def __init__(self, response: OpenSkyResponse) -> None:
        self._response = response

    async def fetch_states(self, _bbox: BoundingBox) -> OpenSkyResponse:
        return self._response


class FakeProducer:
    def __init__(self) -> None:
        self.published: list[AircraftPositionEvent] = []

    async def publish(self, event: AircraftPositionEvent) -> None:
        self.published.append(event)


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 24, 9, 10, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_cycle_publishes_each_valid_event_once() -> None:
    result = await run_poll_cycle(
        FakeOpenSky(RESPONSE),
        FakeProducer(),
        FixedClock(),
        BoundingBox(min_lat=34, min_lon=-11, max_lat=72, max_lon=32),
    )
    assert result == PollResult(fetched=3, published=2, rejected=1)


@pytest.mark.asyncio
async def test_rate_limit_delay_honors_retry_after() -> None:
    class RateLimitedClient:
        async def fetch_states(self, _bbox: BoundingBox) -> OpenSkyResponse:
            raise OpenSkyRateLimited(37)

    class NoopProducer:
        async def publish(self, _event: AircraftPositionEvent) -> None:
            raise AssertionError("publish should not be called")

    sleeps: list[float] = []

    async def fake_sleep(value: float) -> None:
        sleeps.append(value)
        raise asyncio.CancelledError

    settings = IngestorSettings(poll_interval_seconds=10)
    with pytest.raises(asyncio.CancelledError):
        await run_forever(
            settings=settings,
            client=RateLimitedClient(),
            producer=NoopProducer(),
            clock=FixedClock(),
            sleeper=fake_sleep,
        )

    assert sleeps == [37.0]


@pytest.mark.asyncio
async def test_transient_failure_uses_backoff_jitter() -> None:
    class FlakyClient:
        async def fetch_states(self, _bbox: BoundingBox) -> OpenSkyResponse:
            raise httpx.ConnectError("network down")

    class NoopProducer:
        async def publish(self, _event: AircraftPositionEvent) -> None:
            raise AssertionError("publish should not be called")

    sleeps: list[float] = []

    async def fake_sleep(value: float) -> None:
        sleeps.append(value)
        raise asyncio.CancelledError

    settings = IngestorSettings(
        poll_interval_seconds=10,
        backoff_initial_seconds=2,
        backoff_max_seconds=10,
    )
    deterministic = Backoff(
        initial_seconds=2,
        max_seconds=10,
        jitter_ratio=0,
        random_fn=lambda: 0.5,
    )

    with pytest.raises(asyncio.CancelledError):
        await run_forever(
            settings=settings,
            client=FlakyClient(),
            producer=NoopProducer(),
            clock=FixedClock(),
            sleeper=fake_sleep,
            backoff=deterministic,
        )

    assert sleeps == [2.0]
