from __future__ import annotations

from datetime import datetime

from aether_api.app import create_app
from aether_api.config import ApiSettings
from aether_contracts import AircraftSnapshot, SourceStatus
from fastapi.testclient import TestClient


class ReadyRepository:
    async def ready(self) -> bool:
        return True

    async def snapshot(self, now: datetime) -> AircraftSnapshot:
        return AircraftSnapshot(
            generated_at=now,
            source_status=SourceStatus(
                source="opensky",
                status="live",
                last_observation_at=now,
                age_seconds=0.0,
            ),
            aircraft=[],
        )

    async def source_status(self, now: datetime) -> SourceStatus:
        return SourceStatus(
            source="opensky",
            status="live",
            last_observation_at=now,
            age_seconds=0.0,
        )


def test_websocket_starts_with_ready_message() -> None:
    app = create_app(ApiSettings(allowed_origins=["*"]), ReadyRepository())
    client = TestClient(app)

    with client.websocket_connect("/api/v1/stream/aircraft") as socket:
        assert socket.receive_json()["message_type"] == "stream.ready"


def test_websocket_heartbeat_reports_repository_freshness() -> None:
    app = create_app(
        ApiSettings(allowed_origins=["*"], heartbeat_seconds=0.001),
        ReadyRepository(),
    )
    client = TestClient(app)

    with client.websocket_connect("/api/v1/stream/aircraft") as socket:
        assert socket.receive_json()["message_type"] == "stream.ready"
        heartbeat = socket.receive_json()

    assert heartbeat["message_type"] == "heartbeat"
    assert heartbeat["source_status"]["status"] == "live"
