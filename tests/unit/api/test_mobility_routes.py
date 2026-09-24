from __future__ import annotations

from datetime import datetime

from aether_api.app import create_app
from aether_api.config import ApiSettings
from aether_contracts import SourceStatus
from fastapi.testclient import TestClient


class ReadyRepository:
    async def ready(self) -> bool:
        return True

    async def snapshot(self, now: datetime):
        return {
            "generated_at": now,
            "source_status": SourceStatus(
                source="opensky",
                status="live",
                last_observation_at=now,
                age_seconds=0.0,
            ),
            "aircraft": [],
        }

    async def source_status(self, now: datetime) -> SourceStatus:
        return SourceStatus(
            source="opensky",
            status="live",
            last_observation_at=now,
            age_seconds=0.0,
        )


def test_mobility_snapshot_route_returns_snapshot_payload() -> None:
    app = create_app(ApiSettings(), ReadyRepository())
    client = TestClient(app)

    response = client.get("/api/v1/mobility/cells")
    assert response.status_code == 200
    assert response.json()["source_status"]["status"] == "live"
