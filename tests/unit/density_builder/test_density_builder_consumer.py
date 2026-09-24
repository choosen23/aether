from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_contracts import AircraftPositionEvent
from aether_density_builder.aggregate import DensityAggregator
from aether_density_builder.config import DensityBuilderSettings
from aether_density_builder.consumer import OffsetAndTimestamp, seek_to_window
from aiokafka import TopicPartition  # type: ignore[import-untyped]


class FakeConsumer:
    def __init__(self) -> None:
        self.seeks: list[tuple[int, int]] = []
        self._assignment = {
            TopicPartition("aircraft.position.v1", 0),
            TopicPartition("aircraft.position.v1", 1),
        }

    def assignment(self) -> set[TopicPartition]:
        return self._assignment

    async def offsets_for_times(self, _partitions: dict[TopicPartition, int]):
        return {
            TopicPartition("aircraft.position.v1", 0): OffsetAndTimestamp(offset=21),
            TopicPartition("aircraft.position.v1", 1): OffsetAndTimestamp(offset=34),
        }

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict[TopicPartition, int]:
        return {partition: 0 for partition in partitions}

    def seek(self, partition: TopicPartition, offset: int) -> None:
        self.seeks.append((partition.partition, offset))


@pytest.mark.asyncio
async def test_seek_uses_window_start_for_every_partition() -> None:
    consumer = FakeConsumer()
    await seek_to_window(consumer, "aircraft.position.v1", start_ms=1_000)
    assert consumer.seeks == [(0, 21), (1, 34)]


def test_replay_discards_events_older_than_the_working_window() -> None:
    now = datetime(2026, 9, 24, 9, 10, tzinfo=UTC)
    aggregator = DensityAggregator(DensityBuilderSettings(aggregation_window_seconds=180))
    old = AircraftPositionEvent.model_validate(
        {
            "event_id": "old",
            "event_type": "aircraft.position",
            "schema_version": 1,
            "observed_at": now,
            "ingested_at": now,
            "source": "opensky",
            "aircraft": {
                "icao24": "abc123",
                "origin_country": "Germany",
                "position": {
                    "latitude": 50,
                    "longitude": 8,
                    "barometric_altitude_m": 1000,
                    "on_ground": False,
                },
            },
        }
    )
    aggregator.apply(old)
    first = aggregator.prepare(now)
    aggregator.mark_published(first)
    assert aggregator.prepare(now).is_empty
