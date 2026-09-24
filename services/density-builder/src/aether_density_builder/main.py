from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # type: ignore[import-untyped]
from prometheus_client import start_http_server

from aether_density_builder.aggregate import DensityAggregator
from aether_density_builder.config import DensityBuilderSettings
from aether_density_builder.consumer import consume_one, replay_start_ms, seek_to_window
from aether_density_builder.producer import publish_batch


async def run_service(settings: DensityBuilderSettings) -> None:
    start_http_server(settings.metrics_port)
    aggregator = DensityAggregator(settings)

    consumer = AIOKafkaConsumer(
        settings.redpanda_aircraft_topic,
        bootstrap_servers=settings.redpanda_brokers,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.redpanda_brokers)

    await consumer.start()
    await producer.start()

    # Ensure assignment exists before seeking.
    while not consumer.assignment():
        await asyncio.sleep(0.05)
    await seek_to_window(
        consumer,
        settings.redpanda_aircraft_topic,
        replay_start_ms(settings, datetime.now(tz=UTC)),
    )

    async def consume_loop() -> None:
        while True:
            await consume_one(consumer, aggregator)

    async def emit_loop() -> None:
        while True:
            await asyncio.sleep(settings.emit_interval_seconds)
            now = datetime.now(tz=UTC)
            batch = aggregator.prepare(now)
            if batch.is_empty:
                continue
            await publish_batch(producer, aggregator, batch, settings)
            aggregator.mark_published(batch)

    try:
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(consume_loop())
            task_group.create_task(emit_loop())
    finally:
        await consumer.stop()
        await producer.stop()


async def main() -> None:
    await run_service(DensityBuilderSettings.from_env())


if __name__ == "__main__":
    asyncio.run(main())
