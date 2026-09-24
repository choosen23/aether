from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from aether_contracts import ProjectedAircraft


def aircraft(*, event_id: str, observed_at: datetime) -> ProjectedAircraft:
    return ProjectedAircraft(
        icao24="3c6444",
        callsign="DLH4YA",
        origin_country="Germany",
        position={
            "latitude": 50.04,
            "longitude": 8.56,
            "barometric_altitude_m": 10668.0,
            "on_ground": False,
        },
        motion={
            "ground_speed_mps": 232.1,
            "track_degrees": 271.4,
            "vertical_rate_mps": -1.2,
        },
        last_contact_at=observed_at,
        position_source="ads_b",
        event_id=event_id,
        observed_at=observed_at,
    )


NOW = datetime.now(tz=UTC)
EVENT_100 = aircraft(event_id="event-100", observed_at=NOW)
EVENT_110 = aircraft(event_id="event-110", observed_at=NOW + timedelta(seconds=10))


@pytest.mark.asyncio
async def test_publish_project_snapshot_and_stream(pipeline) -> None:
    await pipeline.publish(EVENT_100)
    await pipeline.wait_until_projected("3c6444")
    assert (await pipeline.snapshot()).aircraft[0].event_id == EVENT_100.event_id

    async with pipeline.websocket() as socket:
        assert (await socket.receive_json())["message_type"] == "stream.ready"
        await pipeline.publish(EVENT_110)
        update = await socket.receive_until("aircraft.upsert")
        assert update["aircraft"][0]["event_id"] == EVENT_110.event_id

    await pipeline.publish(EVENT_100)
    assert (await pipeline.snapshot()).aircraft[0].event_id == EVENT_110.event_id
