from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Protocol

from aether_contracts import AircraftPositionEvent
from aiokafka import AIOKafkaProducer  # type: ignore[import-untyped]


class KafkaException(Exception):  # noqa: N818
    """Kafka-compatible producer exception for retry handling."""


class BrokerProducer(Protocol):
    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        on_delivery: Callable[[Exception | None, object | None], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...


def delivery_callback(
    loop: asyncio.AbstractEventLoop,
    delivery_future: asyncio.Future[None],
) -> Callable[[Exception | None, object | None], None]:
    def _callback(error: Exception | None, _message: object | None) -> None:
        if delivery_future.done():
            return
        if error is None:
            loop.call_soon_threadsafe(delivery_future.set_result, None)
            return
        wrapped = KafkaException(str(error))
        loop.call_soon_threadsafe(delivery_future.set_exception, wrapped)

    return _callback


class EventProducer:
    def __init__(
        self,
        producer: BrokerProducer,
        *,
        topic: str,
        delivery_timeout_seconds: float = 5.0,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._delivery_timeout = delivery_timeout_seconds

    async def publish(self, event: AircraftPositionEvent) -> None:
        loop = asyncio.get_running_loop()
        delivery: asyncio.Future[None] = loop.create_future()
        self._producer.produce(
            self._topic,
            key=event.aircraft.icao24.encode(),
            value=event.model_dump_json().encode(),
            on_delivery=delivery_callback(loop, delivery),
        )
        self._producer.poll(0)
        await asyncio.wait_for(delivery, timeout=self._delivery_timeout)


class AioKafkaEventProducer:
    def __init__(
        self,
        producer: AIOKafkaProducer,
        *,
        topic: str,
    ) -> None:
        self._producer = producer
        self._topic = topic

    async def publish(self, event: AircraftPositionEvent) -> None:
        try:
            await self._producer.send_and_wait(
                topic=self._topic,
                key=event.aircraft.icao24.encode(),
                value=event.model_dump_json().encode(),
            )
        except Exception as exc:
            raise KafkaException(str(exc)) from exc
