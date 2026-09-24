from __future__ import annotations

import asyncio

from aiokafka import AIOKafkaConsumer  # type: ignore[import-untyped]
from prometheus_client import start_http_server
from redis import asyncio as redis_async

from aether_state_builder.config import StateBuilderSettings
from aether_state_builder.consumer import ConsumerRecord, consume_one
from aether_state_builder.mobility_consumer import (
    MobilityConsumerRecord,
    consume_mobility_one,
)
from aether_state_builder.mobility_projection import MobilityProjection
from aether_state_builder.projection import AircraftProjection


def create_settings() -> StateBuilderSettings:
    return StateBuilderSettings()


class AioKafkaConsumerAdapter:
    def __init__(self, consumer: AIOKafkaConsumer) -> None:
        self._consumer = consumer
        self._pending_record: object | None = None

    async def getone(self) -> ConsumerRecord | None:
        record = await self._consumer.getone()
        self._pending_record = record
        return ConsumerRecord(key=record.key, value=record.value)

    async def commit(self, _record: ConsumerRecord) -> None:
        await self._consumer.commit()
        self._pending_record = None


class AioKafkaMobilityConsumerAdapter:
    def __init__(self, consumer: AIOKafkaConsumer) -> None:
        self._consumer = consumer

    async def getone(self) -> MobilityConsumerRecord | None:
        record = await self._consumer.getone()
        return MobilityConsumerRecord(key=record.key, value=record.value)

    async def commit(self, _record: MobilityConsumerRecord) -> None:
        await self._consumer.commit()


async def run_service(settings: StateBuilderSettings) -> None:
    start_http_server(settings.metrics_port)
    redis = redis_async.from_url(settings.redis_url)  # type: ignore[no-untyped-call]
    projection = AircraftProjection(
        redis=redis,
        namespace=settings.redis_namespace,
        source_name=settings.source_name,
    )
    mobility_projection = MobilityProjection(
        redis=redis,
        namespace=settings.redis_namespace,
    )
    consumer = AIOKafkaConsumer(
        settings.redpanda_topic,
        bootstrap_servers=settings.redpanda_brokers,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    mobility_consumer = AIOKafkaConsumer(
        settings.redpanda_mobility_density_topic,
        settings.redpanda_demand_region_topic,
        bootstrap_servers=settings.redpanda_brokers,
        group_id=settings.mobility_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    await mobility_consumer.start()
    adapter = AioKafkaConsumerAdapter(consumer)
    mobility_adapter = AioKafkaMobilityConsumerAdapter(mobility_consumer)
    try:
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(_run_aircraft_consumer(adapter, projection))
            task_group.create_task(_run_mobility_consumer(mobility_adapter, mobility_projection))
    finally:
        await consumer.stop()
        await mobility_consumer.stop()
        await redis.aclose()


async def _run_aircraft_consumer(
    adapter: AioKafkaConsumerAdapter,
    projection: AircraftProjection,
) -> None:
    while True:
        await consume_one(adapter, projection)


async def _run_mobility_consumer(
    adapter: AioKafkaMobilityConsumerAdapter,
    projection: MobilityProjection,
) -> None:
    while True:
        await consume_mobility_one(adapter, projection)


async def main() -> None:
    await run_service(StateBuilderSettings.from_env())


if __name__ == "__main__":
    asyncio.run(main())
