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


class FailedRepository:
    async def ready(self) -> bool:
        raise ConnectionError("redis down")

    async def snapshot(self, now: datetime) -> AircraftSnapshot:
        return AircraftSnapshot(
            generated_at=now,
            source_status=SourceStatus(
                source="opensky",
                status="offline",
                last_observation_at=None,
                age_seconds=None,
            ),
            aircraft=[],
        )


def test_ready_is_503_when_redis_is_unavailable() -> None:
    app = create_app(ApiSettings(), FailedRepository())
    client = TestClient(app)

    response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"


def test_snapshot_route_returns_snapshot_payload() -> None:
    app = create_app(ApiSettings(), ReadyRepository())
    client = TestClient(app)

    response = client.get("/api/v1/aircraft")
    assert response.status_code == 200
    assert response.json()["source_status"]["status"] == "live"


def test_live_endpoint_reports_alive() -> None:
    app = create_app(ApiSettings(), ReadyRepository())
    client = TestClient(app)

    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


def test_cors_allows_configured_frontend_origin() -> None:
    app = create_app(
        ApiSettings(allowed_origins=["http://localhost:3000"]),
        ReadyRepository(),
    )
    client = TestClient(app)

    response = client.options(
        "/api/v1/aircraft",
        headers={
            "origin": "http://localhost:3000",
            "access-control-request-method": "GET",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://localhost:3000"
