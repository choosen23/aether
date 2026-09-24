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


def test_mobility_websocket_starts_with_ready_message() -> None:
    app = create_app(ApiSettings(allowed_origins=["*"]), ReadyRepository())
    client = TestClient(app)

    with client.websocket_connect("/api/v1/stream/mobility") as socket:
        assert socket.receive_json()["message_type"] == "mobility.stream.ready"


def test_source_freshness_matches_sixty_second_polling() -> None:
    settings = ApiSettings()
    assert settings.source_live_max_age_seconds == 75
    assert settings.source_offline_max_age_seconds == 180
