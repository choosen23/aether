from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_contracts import MobilityDensityEvent
from aether_density_builder.aggregate import AggregateBatch, DensityAggregator
from aether_density_builder.config import DensityBuilderSettings
from aether_density_builder.producer import publish_batch


class FakeProducer:
    def __init__(self) -> None:
        self.records: list[tuple[str, bytes, bytes]] = []

    async def send_and_wait(self, topic: str, value: bytes, key: bytes) -> object:
        self.records.append((topic, value, key))
        return object()


@pytest.mark.asyncio
async def test_publish_batch_sends_density_before_matching_demand() -> None:
    now = datetime(2026, 9, 24, 9, 10, tzinfo=UTC)
    density = MobilityDensityEvent(
        event_id="density-1",
        generated_at=now,
        window_started_at=now,
        window_ended_at=now,
        source_event_watermark=now,
        cell_id="841f1d7ffffffff",
        h3_resolution=4,
        aircraft_count=1,
        airborne_count=1,
        on_ground_count=0,
        average_altitude_m=1000,
        average_ground_speed_mps=200,
    )
    batch = AggregateBatch(
        densities=[density],
        removals=[],
        expired_icao24=[],
        signatures={density.cell_id: "sig"},
        windows={density.cell_id: now},
    )

    producer = FakeProducer()
    settings = DensityBuilderSettings()
    await publish_batch(
        producer,
        DensityAggregator(settings),
        batch,
        settings,
    )

    assert [record[0] for record in producer.records] == ["mobility.density.v1", "demand.region.v1"]
    assert producer.records[0][2] == producer.records[1][2]
