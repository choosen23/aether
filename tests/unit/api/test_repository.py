from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_api.config import ApiSettings
from aether_api.repository import AircraftRepository, epoch_ms


class FakeRedis:
    def __init__(self) -> None:
        self.sorted_index: dict[str, int] = {}
        self.hashes: dict[str, dict[str, object]] = {}

    async def zrangebyscore(self, _key: str, min_score: int, _max_score: str) -> list[object]:
        return [
            icao.encode("utf-8")
            for icao, score in self.sorted_index.items()
            if score >= min_score
        ]

    async def hget(self, key: str, field: str) -> object | None:
        return self.hashes.get(key, {}).get(field)

    async def hgetall(self, key: str) -> dict[object, object]:
        return {k.encode("utf-8"): v for k, v in self.hashes.get(key, {}).items()}

    async def ping(self) -> object:
        return b"PONG"

    async def eval(self, _script: str, _numkeys: int, _index: str, _prefix: str, cutoff: int):
        expired = [icao for icao, score in self.sorted_index.items() if score <= cutoff]
        for icao in expired:
            self.sorted_index.pop(icao)
            self.hashes.pop(f"aether:aircraft:{icao}", None)
        return [icao.encode("utf-8") for icao in expired]


def projected_payload(observed_at: datetime) -> str:
    return (
        "{"
        f'"icao24":"3c6444","callsign":"DLH4YA","origin_country":"Germany",'
        '"position":{"latitude":50.04,"longitude":8.56,"barometric_altitude_m":10668.0,"on_ground":false},'
        '"motion":{"ground_speed_mps":232.1,"track_degrees":271.4,"vertical_rate_mps":-1.2},'
        f'"last_contact_at":"{observed_at.isoformat().replace("+00:00", "Z")}",'
        '"position_source":"ads_b",'
        '"event_id":"event-110",'
        f'"observed_at":"{observed_at.isoformat().replace("+00:00", "Z")}"'
        "}"
    )


@pytest.mark.asyncio
async def test_snapshot_excludes_removed_but_keeps_stale_aircraft() -> None:
    now = datetime(2026, 9, 24, 9, 12, 0, tzinfo=UTC)
    stale_observed_at = datetime(2026, 9, 24, 9, 10, 30, tzinfo=UTC)
    removed_observed_at = datetime(2026, 9, 24, 9, 7, 0, tzinfo=UTC)

    redis = FakeRedis()
    redis.sorted_index = {
        "3c6444": epoch_ms(stale_observed_at),
        "dead00": epoch_ms(removed_observed_at),
    }
    redis.hashes["aether:aircraft:3c6444"] = {"payload": projected_payload(stale_observed_at)}
    redis.hashes["aether:aircraft:dead00"] = {"payload": projected_payload(removed_observed_at)}
    redis.hashes["aether:source:opensky"] = {
        "last_observation_epoch_ms": str(epoch_ms(stale_observed_at)),
    }

    settings = ApiSettings(
        aircraft_removal_seconds=180,
        source_live_max_age_seconds=30,
        source_offline_max_age_seconds=180,
    )
    repository = AircraftRepository(redis, settings)
    snapshot = await repository.snapshot(now)

    assert [item.icao24 for item in snapshot.aircraft] == ["3c6444"]
    assert snapshot.source_status.status == "stale"


@pytest.mark.asyncio
async def test_remove_expired_deletes_and_returns_aircraft_ids() -> None:
    now = datetime(2026, 9, 24, 9, 12, 0, tzinfo=UTC)
    redis = FakeRedis()
    redis.sorted_index = {
        "live00": epoch_ms(datetime(2026, 9, 24, 9, 11, 0, tzinfo=UTC)),
        "dead00": epoch_ms(datetime(2026, 9, 24, 9, 8, 0, tzinfo=UTC)),
    }
    redis.hashes["aether:aircraft:dead00"] = {"payload": "discarded"}
    repository = AircraftRepository(redis, ApiSettings(aircraft_removal_seconds=180))

    removed = await repository.remove_expired(now)

    assert removed == ["dead00"]
    assert "dead00" not in redis.sorted_index
    assert "aether:aircraft:dead00" not in redis.hashes
