from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from aether_contracts import AircraftPositionEvent
from aether_ingestor.producer import EventProducer

EVENT = AircraftPositionEvent.model_validate(
    {
        "event_id": "e0d9ce8f95a0f9f3383a90d043",
        "event_type": "aircraft.position",
        "schema_version": 1,
        "observed_at": datetime(2026, 9, 24, 9, 10, tzinfo=UTC),
        "ingested_at": datetime(2026, 9, 24, 9, 10, 1, tzinfo=UTC),
        "source": "opensky",
        "aircraft": {
            "icao24": "3c6444",
            "callsign": "DLH4YA",
            "origin_country": "Germany",
        },
    }
)


@dataclass(slots=True)
class FakeRecord:
    key: bytes
    value: bytes


class FakeBroker:
    def __init__(self) -> None:
        self.records: list[FakeRecord] = []

    def produce(
        self,
        _topic: str,
        *,
        key: bytes,
        value: bytes,
        on_delivery: Callable[[Exception | None, object | None], None],
    ) -> None:
        self.records.append(FakeRecord(key=key, value=value))
        on_delivery(None, None)

    def poll(self, _timeout: float) -> int:
        return 0


@pytest.mark.asyncio
async def test_publish_keys_event_by_icao24() -> None:
    broker = FakeBroker()
    await EventProducer(broker, topic="aircraft.position.v1").publish(EVENT)
    assert broker.records[0].key == b"3c6444"
    assert json.loads(broker.records[0].value)["schema_version"] == 1
