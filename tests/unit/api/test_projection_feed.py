from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_api.projection_feed import ProjectionFeed
from aether_contracts import AircraftRemove, AircraftUpsert, ProjectedAircraft


def projected() -> ProjectedAircraft:
    return ProjectedAircraft(
        icao24="3c6444",
        callsign="DLH4YA",
        origin_country="Germany",
        event_id="event-110",
        observed_at=datetime(2026, 9, 24, 9, 10, tzinfo=UTC),
    )


class RecordingBroadcaster:
    def __init__(self) -> None:
        self.messages: list[object] = []

    async def publish(self, message: object) -> None:
        self.messages.append(message)


class ExpiringRepository:
    async def remove_expired(self, _now: datetime) -> list[str]:
        return ["3c6444", "dead00"]


@pytest.mark.asyncio
async def test_projection_payload_becomes_aircraft_upsert() -> None:
    broadcaster = RecordingBroadcaster()
    feed = ProjectionFeed(
        redis=object(),
        broadcaster=broadcaster,
        repository=ExpiringRepository(),
        namespace="aether",
        removal_interval_seconds=1,
    )

    await feed.handle_update(projected().model_dump_json())

    assert len(broadcaster.messages) == 1
    message = broadcaster.messages[0]
    assert isinstance(message, AircraftUpsert)
    assert message.aircraft[0].event_id == "event-110"


@pytest.mark.asyncio
async def test_expired_projection_becomes_remove_message() -> None:
    broadcaster = RecordingBroadcaster()
    feed = ProjectionFeed(
        redis=object(),
        broadcaster=broadcaster,
        repository=ExpiringRepository(),
        namespace="aether",
        removal_interval_seconds=1,
    )

    await feed.publish_expired(datetime(2026, 9, 24, 9, 15, tzinfo=UTC))

    message = broadcaster.messages[0]
    assert isinstance(message, AircraftRemove)
    assert message.icao24 == ["3c6444", "dead00"]
