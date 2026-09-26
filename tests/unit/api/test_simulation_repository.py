from __future__ import annotations

import json

import pytest
from aether_api.simulation_repository import SimulationRepository


class FakeRedis:
    def __init__(self) -> None:
        self.zsets: dict[str, list[str]] = {
            "aether:simulation:runs": ["run-1", "run-2"],
        }
        self.hashes: dict[str, dict[str, str]] = {
            "aether:simulation:run:run-1": {
                "last_sequence": "5",
                "simulated_time_ms": "50",
                "status": "completed",
                "configuration": json.dumps({"scheduler_policy": "balanced"}),
                "summary": json.dumps(
                    {
                        "component_metrics": {"completion_rate": 1.0},
                        "combined_score": 0.1,
                    }
                ),
            },
            "aether:simulation:run:run-2": {
                "last_sequence": "2",
                "simulated_time_ms": "20",
                "last_event_type": "workload_arrived",
                "scheduler_policy": "cloud_only",
            },
        }

    async def zrange(self, key: str, start: int, stop: int) -> list[object]:
        values = self.zsets.get(key, [])
        if stop == -1:
            return [item.encode("utf-8") for item in values[start:]]
        return [item.encode("utf-8") for item in values[start : stop + 1]]

    async def hgetall(self, key: str) -> dict[object, object]:
        payload = self.hashes.get(key, {})
        return {k.encode("utf-8"): v.encode("utf-8") for k, v in payload.items()}


@pytest.mark.asyncio
async def test_repository_reads_live_and_completed_shapes() -> None:
    repository = SimulationRepository(FakeRedis())
    run = await repository.get_experiment("run-1")
    assert run is not None
    assert run["run_id"] == "run-1"
    assert run["configuration"]["scheduler_policy"] == "balanced"
    assert run["status"] == "completed"


@pytest.mark.asyncio
async def test_repository_lists_runs_in_reverse_order() -> None:
    repository = SimulationRepository(FakeRedis())
    runs = await repository.list_experiments()
    assert [item["run_id"] for item in runs] == ["run-2", "run-1"]
