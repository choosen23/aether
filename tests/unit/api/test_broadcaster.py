from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_api.broadcaster import AircraftBroadcaster
from aether_contracts import AircraftUpsert, ProjectedAircraft


def upsert(event_id: str) -> AircraftUpsert:
    return AircraftUpsert(
        sent_at=datetime(2026, 9, 24, 9, 10, tzinfo=UTC),
        aircraft=[
            ProjectedAircraft(
                icao24="3c6444",
                callsign="DLH4YA",
                origin_country="Germany",
                event_id=event_id,
                observed_at=datetime(2026, 9, 24, 9, 10, tzinfo=UTC),
            )
        ],
    )


@pytest.mark.asyncio
async def test_slow_client_is_evicted_when_queue_is_full() -> None:
    broadcaster = AircraftBroadcaster(queue_size=2)
    client_id, _queue = broadcaster.subscribe()
    await broadcaster.publish(upsert("u1"))
    await broadcaster.publish(upsert("u2"))
    await broadcaster.publish(upsert("u3"))
    assert client_id not in broadcaster.client_ids
    assert broadcaster.evictions == 1
