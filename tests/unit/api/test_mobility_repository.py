from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_api.config import ApiSettings
from aether_api.mobility_repository import MobilityRepository
from aether_api.repository import epoch_ms
from aether_contracts import SourceStatus


class FakeRedis:
    def __init__(self) -> None:
        self.sorted_index: dict[str, int] = {}
        self.hashes: dict[str, dict[str, object]] = {}

    async def zrangebyscore(self, _key: str, min_score: int, _max_score: str) -> list[object]:
        return [
            cell.encode("utf-8")
            for cell, score in self.sorted_index.items()
            if score >= min_score
        ]

    async def hget(self, key: str, field: str) -> object | None:
        return self.hashes.get(key, {}).get(field)

    async def eval(self, _script: str, _numkeys: int, _index_key: str, cutoff: int, prefix: str):
        expired = [cell for cell, score in self.sorted_index.items() if score <= cutoff]
        for cell in expired:
            self.sorted_index.pop(cell)
            self.hashes.pop(f"{prefix}{cell}", None)
        return [cell.encode("utf-8") for cell in expired]


class FakeSourceRepository:
    async def source_status(self, now: datetime) -> SourceStatus:
        return SourceStatus(
            source="opensky",
            status="live",
            last_observation_at=now,
            age_seconds=0.0,
        )


def _cell_payload(window_ended_at: str = "2026-09-24T09:10:00Z") -> str:
    return (
        "{"
        '"density": {'
        '"event_id":"density-1","event_type":"mobility.density","schema_version":1,'
        '"generated_at":"2026-09-24T09:10:00Z","window_started_at":"2026-09-24T09:07:00Z",'
        f'"window_ended_at":"{window_ended_at}","source_event_watermark":"2026-09-24T09:10:00Z",'
        '"cell_id":"841f1d7ffffffff","h3_resolution":4,"aircraft_count":1,'
        '"airborne_count":1,"on_ground_count":0,"average_altitude_m":1000.0,'
        '"average_ground_speed_mps":200.0,"provenance":"derived"'
        "},"
        '"demand": {'
        '"event_id":"demand-1","event_type":"demand.region","schema_version":1,'
        '"generated_at":"2026-09-24T09:10:00Z","window_started_at":"2026-09-24T09:07:00Z",'
        f'"window_ended_at":"{window_ended_at}","cell_id":"841f1d7ffffffff",'
        '"h3_resolution":4,"aircraft_count":1,"demand_units":1.0,'
        '"model_version":"density-linear-v1","coefficient_set":"default-v1","provenance":"modelled"'
        "}"
        "}"
    )


@pytest.mark.asyncio
async def test_snapshot_excludes_expired_cells() -> None:
    now = datetime(2026, 9, 24, 9, 12, 0, tzinfo=UTC)
    redis = FakeRedis()
    redis.sorted_index = {
        "841f1d7ffffffff": epoch_ms(datetime(2026, 9, 24, 9, 11, 0, tzinfo=UTC)),
        "841f1d5ffffffff": epoch_ms(datetime(2026, 9, 24, 9, 8, 0, tzinfo=UTC)),
    }
    redis.hashes["aether:mobility:841f1d7ffffffff"] = {"payload": _cell_payload()}
    redis.hashes["aether:mobility:841f1d5ffffffff"] = {
        "payload": _cell_payload("2026-09-24T09:08:00Z")
    }
    repository = MobilityRepository(redis, FakeSourceRepository(), ApiSettings())

    snapshot = await repository.snapshot(now)

    assert [cell.density.cell_id for cell in snapshot.cells] == ["841f1d7ffffffff"]
