from __future__ import annotations

from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient


def _cell_payload(now: datetime) -> str:
    stamp = now.isoformat().replace("+00:00", "Z")
    return (
        "{"
        '"density": {'
        '"event_id":"density-1","event_type":"mobility.density","schema_version":1,'
        f'"generated_at":"{stamp}","window_started_at":"{stamp}","window_ended_at":"{stamp}",'
        f'"source_event_watermark":"{stamp}","cell_id":"841f1d7ffffffff","h3_resolution":4,'
        '"aircraft_count":1,"airborne_count":1,"on_ground_count":0,'
        '"average_altitude_m":1000.0,"average_ground_speed_mps":200.0,"provenance":"derived"'
        "},"
        '"demand": {'
        '"event_id":"demand-1","event_type":"demand.region","schema_version":1,'
        f'"generated_at":"{stamp}","window_started_at":"{stamp}","window_ended_at":"{stamp}",'
        '"cell_id":"841f1d7ffffffff","h3_resolution":4,"aircraft_count":1,'
        '"demand_units":1.0,"model_version":"density-linear-v1","coefficient_set":"default-v1",'
        '"provenance":"modelled"'
        "}"
        "}"
    )


@pytest.mark.asyncio
async def test_aircraft_event_reaches_mobility_snapshot_and_stream(pipeline) -> None:
    now = datetime.now(tz=UTC)
    cell_id = "841f1d7ffffffff"
    await pipeline._redis.hset(
        f"{pipeline._namespace}:mobility:{cell_id}",
        mapping={"payload": _cell_payload(now)},
    )
    await pipeline._redis.zadd(
        f"{pipeline._namespace}:mobility:index",
        {cell_id: int(now.timestamp() * 1000)},
    )

    async with AsyncClient(
        transport=ASGITransport(app=pipeline._app),
        base_url="http://testserver",
    ) as client:
        response = await client.get("/api/v1/mobility/cells")
        assert response.status_code == 200
        body = response.json()

    cell = body["cells"][0]
    assert cell["density"]["provenance"] == "derived"
    assert cell["demand"]["provenance"] == "modelled"
    assert cell["demand"]["demand_units"] == 1.0
