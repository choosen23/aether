from __future__ import annotations

from datetime import datetime

from aether_api.app import create_app
from aether_api.config import ApiSettings
from aether_api.simulation_routes import get_simulation_repository
from fastapi.testclient import TestClient


class ReadyRepository:
    async def ready(self) -> bool:
        return True

    async def snapshot(self, now: datetime):
        return {
            "generated_at": now,
            "source_status": {
                "source": "opensky",
                "status": "live",
                "last_observation_at": now,
                "age_seconds": 0.0,
            },
            "aircraft": [],
        }

    async def source_status(self, now: datetime):
        return {
            "source": "opensky",
            "status": "live",
            "last_observation_at": now,
            "age_seconds": 0.0,
        }


class FakeSimulationRepository:
    async def list_experiments(self) -> list[dict[str, object]]:
        return [
            {
                "run_id": "run-1",
                "status": "completed",
                "sequence": 5,
                "scheduler_policy": "balanced",
            }
        ]

    async def get_experiment(self, run_id: str) -> dict[str, object] | None:
        if run_id != "run-1":
            return None
        return {
            "run_id": "run-1",
            "sequence": 5,
            "status": "completed",
            "simulated_time_ms": 50,
            "configuration": {"scheduler_policy": "balanced"},
            "summary": {
                "component_metrics": {"completion_rate": 1.0},
                "combined_score": 0.1,
            },
        }

    async def get_results(self, run_id: str) -> dict[str, object] | None:
        if run_id != "run-1":
            return None
        return {
            "run_id": "run-1",
            "status": "completed",
            "summary": {
                "component_metrics": {"completion_rate": 1.0},
                "combined_score": 0.1,
            },
        }


def test_experiment_snapshot_returns_live_and_completed_shape() -> None:
    app = create_app(ApiSettings(), ReadyRepository())
    app.dependency_overrides[get_simulation_repository] = lambda: FakeSimulationRepository()
    client = TestClient(app)

    response = client.get("/api/v1/experiments/run-1")
    assert response.status_code == 200
    assert response.json()["run_id"] == "run-1"
    assert response.json()["configuration"]["scheduler_policy"] == "balanced"


def test_mutating_experiment_route_does_not_exist() -> None:
    app = create_app(ApiSettings(), ReadyRepository())
    client = TestClient(app)
    assert client.post("/api/v1/experiments", json={}).status_code == 405
