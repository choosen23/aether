from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from aether_contracts import AircraftPositionEvent
from aether_state_builder.consumer import ConsumeResult, ConsumerRecord, consume_one

RECORD = ConsumerRecord(
    key=b"3c6444",
    value=AircraftPositionEvent.model_validate(
        {
            "event_id": "event-110",
            "event_type": "aircraft.position",
            "schema_version": 1,
            "observed_at": datetime(2026, 9, 24, 9, 10, 10, tzinfo=UTC),
            "ingested_at": datetime(2026, 9, 24, 9, 10, 11, tzinfo=UTC),
            "source": "opensky",
            "aircraft": {
                "icao24": "3c6444",
                "callsign": "DLH4YA",
                "origin_country": "Germany",
                "position": {
                    "latitude": 50.04,
                    "longitude": 8.56,
                    "barometric_altitude_m": 10668.0,
                    "on_ground": False,
                },
                "motion": {
                    "ground_speed_mps": 232.1,
                    "track_degrees": 271.4,
                    "vertical_rate_mps": -1.2,
                },
                "last_contact_at": datetime(2026, 9, 24, 9, 10, 10, tzinfo=UTC),
                "position_source": "ads_b",
            },
        }
    ).model_dump_json().encode("utf-8"),
)
MALFORMED_RECORD = ConsumerRecord(key=b"3c6444", value=b"{not-json")


class FakeConsumer:
    def __init__(self, records: list[ConsumerRecord]) -> None:
        self._records = records
        self.committed: list[ConsumerRecord] = []

    async def getone(self) -> ConsumerRecord | None:
        if not self._records:
            return None
        return self._records.pop(0)

    async def commit(self, record: ConsumerRecord) -> None:
        self.committed.append(record)


class SuccessfulProjection:
    async def apply(self, _event: AircraftPositionEvent) -> object:
        return None


@dataclass(slots=True)
class RetryProjection:
    async def apply(self, _event: AircraftPositionEvent) -> object:
        raise ConnectionError("redis unavailable")


@pytest.mark.asyncio
async def test_offset_is_committed_only_after_projection_success() -> None:
    consumer = FakeConsumer([RECORD])
    result = await consume_one(consumer, SuccessfulProjection())
    assert result is ConsumeResult.APPLIED
    assert consumer.committed == [RECORD]


@pytest.mark.asyncio
async def test_invalid_payload_is_counted_and_committed() -> None:
    consumer = FakeConsumer([MALFORMED_RECORD])
    result = await consume_one(consumer, SuccessfulProjection())
    assert result is ConsumeResult.REJECTED
    assert consumer.committed == [MALFORMED_RECORD]


@pytest.mark.asyncio
async def test_connection_failure_retries_without_commit() -> None:
    consumer = FakeConsumer([RECORD])
    result = await consume_one(consumer, RetryProjection())
    assert result is ConsumeResult.RETRY
    assert consumer.committed == []
