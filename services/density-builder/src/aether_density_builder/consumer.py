from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol

from aether_contracts import AircraftPositionEvent
from aiokafka import TopicPartition  # type: ignore[import-untyped]
from pydantic import ValidationError

from aether_density_builder.aggregate import DensityAggregator
from aether_density_builder.config import DensityBuilderSettings
from aether_density_builder.metrics import records_consumed_total, working_set_size


@dataclass(slots=True)
class OffsetAndTimestamp:
    offset: int


@dataclass(slots=True)
class ConsumerRecord:
    value: bytes


class ConsumerProtocol(Protocol):
    def assignment(self) -> set[TopicPartition]: ...

    async def offsets_for_times(
        self, partitions: dict[TopicPartition, int]
    ) -> dict[TopicPartition, OffsetAndTimestamp | None]: ...

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict[TopicPartition, int]: ...

    def seek(self, partition: TopicPartition, offset: int) -> None: ...

    async def getone(self) -> ConsumerRecord: ...

    async def commit(self) -> None: ...


class ConsumeResult(StrEnum):
    APPLIED = "applied"
    REJECTED = "rejected"


async def seek_to_window(consumer: ConsumerProtocol, topic: str, start_ms: int) -> None:
    partitions = {
        TopicPartition(topic=topic, partition=part.partition): start_ms
        for part in consumer.assignment()
    }
    if not partitions:
        return
    by_time = await consumer.offsets_for_times(partitions)
    ends = await consumer.end_offsets(list(partitions.keys()))
    for partition in sorted(partitions.keys(), key=lambda item: item.partition):
        lookup = by_time.get(partition)
        if lookup is None:
            consumer.seek(partition, ends[partition])
            continue
        consumer.seek(partition, lookup.offset)


async def consume_one(
    consumer: ConsumerProtocol,
    aggregator: DensityAggregator,
) -> ConsumeResult:
    raw = await consumer.getone()
    value = raw.value
    try:
        event = AircraftPositionEvent.model_validate_json(value)
    except ValidationError:
        await consumer.commit()
        records_consumed_total.labels(result="rejected").inc()
        return ConsumeResult.REJECTED

    aggregator.apply(event)
    working_set_size.set(aggregator.sample_count)
    await consumer.commit()
    records_consumed_total.labels(result="applied").inc()
    return ConsumeResult.APPLIED


def replay_start_ms(settings: DensityBuilderSettings, now: datetime | None = None) -> int:
    value = now or datetime.now(tz=UTC)
    start = value - timedelta(seconds=settings.aggregation_window_seconds)
    return int(start.timestamp() * 1000)
