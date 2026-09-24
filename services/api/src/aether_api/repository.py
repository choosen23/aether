from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol

from aether_contracts import AircraftSnapshot, ProjectedAircraft, SourceStatus

from aether_api.config import ApiSettings


class RedisReadProtocol(Protocol):
    async def zrangebyscore(self, key: str, min_score: int, max_score: str) -> list[object]: ...

    async def hget(self, key: str, field: str) -> object | None: ...

    async def hgetall(self, key: str) -> dict[object, object]: ...

    async def ping(self) -> object: ...

    async def eval(self, script: str, numkeys: int, *keys_and_args: object) -> object: ...


LUA_REMOVE_EXPIRED = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
for _, id in ipairs(ids) do
  redis.call('DEL', ARGV[1] .. id)
end
if #ids > 0 then
  redis.call('ZREM', KEYS[1], unpack(ids))
end
return ids
"""


def epoch_ms(value: datetime) -> int:
    return int(value.astimezone(UTC).timestamp() * 1000)


class AircraftRepository:
    def __init__(self, redis: RedisReadProtocol, settings: ApiSettings) -> None:
        self._redis = redis
        self._namespace = settings.redis_namespace
        self._source_name = settings.source_name
        self._live_seconds = settings.source_live_max_age_seconds
        self._offline_seconds = settings.source_offline_max_age_seconds
        self._removal_seconds = settings.aircraft_removal_seconds
        self._max_snapshot = settings.max_snapshot_aircraft

    @property
    def _index_key(self) -> str:
        return f"{self._namespace}:aircraft:index"

    def _aircraft_key(self, icao24: str) -> str:
        return f"{self._namespace}:aircraft:{icao24}"

    @property
    def _source_key(self) -> str:
        return f"{self._namespace}:source:{self._source_name}"

    async def ready(self) -> bool:
        await self._redis.ping()
        return True

    async def snapshot(self, now: datetime) -> AircraftSnapshot:
        minimum_ms = epoch_ms(now - timedelta(seconds=self._removal_seconds))
        ids_raw = await self._redis.zrangebyscore(self._index_key, minimum_ms, "+inf")
        ids = sorted(self._decode_string(value) for value in ids_raw)[: self._max_snapshot]
        aircraft = await self._load_states(ids)
        source = await self.source_status(now)
        return AircraftSnapshot(generated_at=now, source_status=source, aircraft=aircraft)

    async def remove_expired(self, now: datetime) -> list[str]:
        cutoff = epoch_ms(now - timedelta(seconds=self._removal_seconds))
        values = await self._redis.eval(
            LUA_REMOVE_EXPIRED,
            1,
            self._index_key,
            f"{self._namespace}:aircraft:",
            cutoff,
        )
        if not isinstance(values, list):
            msg = "redis expiry script returned a non-list response"
            raise RuntimeError(msg)
        return sorted(self._decode_string(value) for value in values)

    async def _load_states(self, ids: list[str]) -> list[ProjectedAircraft]:
        output: list[ProjectedAircraft] = []
        for icao24 in ids:
            payload = await self._redis.hget(self._aircraft_key(icao24), "payload")
            if payload is None:
                continue
            output.append(ProjectedAircraft.model_validate_json(self._decode_string(payload)))
        return output

    async def source_status(self, now: datetime) -> SourceStatus:
        source_map = await self._redis.hgetall(self._source_key)
        raw_epoch = source_map.get(b"last_observation_epoch_ms") or source_map.get(
            "last_observation_epoch_ms"
        )
        if raw_epoch is None:
            return SourceStatus(
                source="opensky",
                status="offline",
                last_observation_at=None,
                age_seconds=None,
            )

        observed_epoch_ms = int(self._decode_string(raw_epoch))
        observed_at = datetime.fromtimestamp(observed_epoch_ms / 1000, tz=UTC)
        age_seconds = max(0.0, (now - observed_at).total_seconds())

        status: Literal["live", "stale", "offline"]
        if age_seconds <= self._live_seconds:
            status = "live"
        elif age_seconds <= self._offline_seconds:
            status = "stale"
        else:
            status = "offline"

        return SourceStatus(
            source="opensky",
            status=status,
            last_observation_at=observed_at,
            age_seconds=age_seconds,
        )

    @staticmethod
    def _decode_string(value: object) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)
