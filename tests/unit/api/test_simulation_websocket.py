from __future__ import annotations

from datetime import datetime

import pytest
from aether_api.app import create_app
from aether_api.config import ApiSettings
from aether_api.simulation_broadcaster import SimulationBroadcaster
from aether_api.simulation_websocket import (
    get_settings as get_simulation_settings,
)
from aether_api.simulation_websocket import (
    get_simulation_broadcaster,
    get_simulation_repository,
)
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect


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
    async def get_experiment(self, run_id: str) -> dict[str, object] | None:
        if run_id != "run-1":
            return None
        return {
            "run_id": "run-1",
            "sequence": 1,
            "status": "running",
            "simulated_time_ms": 10,
            "configuration": {"scheduler_policy": "balanced"},
            "summary": {"component_metrics": {}, "combined_score": None},
        }


def _app() -> tuple[TestClient, SimulationBroadcaster]:
    app = create_app(ApiSettings(allowed_origins=["*"], heartbeat_seconds=0.001), ReadyRepository())
    broadcaster = SimulationBroadcaster(queue_size=1)
    app.dependency_overrides[get_simulation_repository] = lambda: FakeSimulationRepository()
    app.dependency_overrides[get_simulation_settings] = lambda: ApiSettings(
        allowed_origins=["*"],
        heartbeat_seconds=0.001,
    )
    app.dependency_overrides[get_simulation_broadcaster] = lambda: broadcaster
    return TestClient(app), broadcaster


def test_websocket_ready_update_and_heartbeat() -> None:
    client, _ = _app()
    with client.websocket_connect("/ws/v1/experiments/run-1") as socket:
        assert socket.receive_json()["message_type"] == "simulation.stream.ready"
        assert socket.receive_json()["message_type"] == "simulation.run.updated"
        heartbeat = socket.receive_json()
        assert heartbeat["message_type"] == "simulation.heartbeat"


def test_websocket_ignores_stale_sequence_and_sends_newer_update() -> None:
    client, broadcaster = _app()
    with client.websocket_connect("/ws/v1/experiments/run-1") as socket:
        socket.receive_json()
        socket.receive_json()

        broadcaster.publish_nowait(
            "run-1",
            {
                "message_type": "simulation.run.updated",
                "run": {"run_id": "run-1", "sequence": 1},
            },
        )
        broadcaster.publish_nowait(
            "run-1",
            {
                "message_type": "simulation.run.updated",
                "run": {"run_id": "run-1", "sequence": 2},
            },
        )

        message = socket.receive_json()
        assert message["message_type"] == "simulation.run.updated"
        assert message["run"]["sequence"] == 2


def test_unknown_run_closes_with_4404() -> None:
    client, _ = _app()
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect("/ws/v1/experiments/missing"):
            pass
    assert exc_info.value.code == 4404
