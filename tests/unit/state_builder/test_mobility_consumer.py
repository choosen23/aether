from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from aether_contracts import MobilityDensityEvent
from aether_state_builder.mobility_consumer import (
    MobilityConsumeResult,
    MobilityConsumerRecord,
    consume_mobility_one,
)


def density_payload(cell_id: str = "841f1d7ffffffff") -> bytes:
    event = MobilityDensityEvent(
        event_id="density-1",
        generated_at=datetime(2026, 9, 24, 9, 10, tzinfo=UTC),
        window_started_at=datetime(2026, 9, 24, 9, 7, tzinfo=UTC),
        window_ended_at=datetime(2026, 9, 24, 9, 10, tzinfo=UTC),
        source_event_watermark=datetime(2026, 9, 24, 9, 10, tzinfo=UTC),
        cell_id=cell_id,
        h3_resolution=4,
        aircraft_count=1,
        airborne_count=1,
        on_ground_count=0,
        average_altitude_m=1000,
        average_ground_speed_mps=200,
    )
    return event.model_dump_json().encode("utf-8")


@dataclass
class FakeConsumer:
    records: list[MobilityConsumerRecord]
    commit_count: int = 0

    async def getone(self) -> MobilityConsumerRecord | None:
        if not self.records:
            return None
        return self.records.pop(0)

    async def commit(self, _record: MobilityConsumerRecord) -> None:
        self.commit_count += 1


class FakeProjection:
    async def apply_density(self, density: MobilityDensityEvent) -> object:
        if density.cell_id == "not-h3":
            raise ValueError("invalid h3 cell")
        return "updated"

    async def apply_demand(self, _demand) -> object:
        return "updated"

    async def remove(self, _cell_id: str, _sent_at: datetime) -> None:
        return None


@pytest.mark.asyncio
async def test_invalid_h3_cell_is_committed_and_rejected() -> None:
    consumer = FakeConsumer(
        records=[MobilityConsumerRecord(key=b"not-h3", value=density_payload("not-h3"))]
    )
    result = await consume_mobility_one(consumer, FakeProjection())
    assert result is MobilityConsumeResult.REJECTED
    assert consumer.commit_count == 1
