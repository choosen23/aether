from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_fixture_trace_reaches_experiment_api(pipeline) -> None:
    now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
    run_id = "run-fixture-1"
    namespace = pipeline._namespace

    await pipeline._redis.zadd(f"{namespace}:simulation:runs", {run_id: now_ms})
    await pipeline._redis.hset(
        f"{namespace}:simulation:run:{run_id}",
        mapping={
            "last_sequence": "8",
            "status": "completed",
            "simulated_time_ms": "3600000",
            "configuration": json.dumps({"scheduler_policy": "balanced"}),
            "summary": json.dumps(
                {
                    "component_metrics": {
                        "completion_rate": 1.0,
                        "deadline_miss_rate": 0.0,
                    },
                    "combined_score": 0.84,
                }
            ),
        },
    )

    async with AsyncClient(
        transport=ASGITransport(app=pipeline._app),
        base_url="http://testserver",
    ) as client:
        snapshot = await client.get(f"/api/v1/experiments/{run_id}")
        assert snapshot.status_code == 200
        body = snapshot.json()
        assert body["status"] == "completed"
        assert body["summary"]["component_metrics"]["completion_rate"] == 1.0

        listed = await client.get("/api/v1/experiments")
        assert listed.status_code == 200
        assert any(item["run_id"] == run_id for item in listed.json()["runs"])
