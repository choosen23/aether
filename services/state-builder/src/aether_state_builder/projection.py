from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from aether_contracts import AircraftPositionEvent, ProjectedAircraft

LUA_APPLY_PROJECTION = """
local current = redis.call('HGET', KEYS[1], 'observed_at_epoch_ms')
if current and tonumber(current) > tonumber(ARGV[1]) then return 2 end
if current and tonumber(current) == tonumber(ARGV[1]) then return 1 end
redis.call('HSET', KEYS[1],
  'observed_at_epoch_ms', ARGV[1], 'event_id', ARGV[2], 'payload', ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[4])
redis.call('PUBLISH', KEYS[3], ARGV[3])
return 0
"""

LUA_UPDATE_SOURCE = """
local current = redis.call('HGET', KEYS[1], 'last_observation_epoch_ms')
if current and tonumber(current) >= tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1],
  'last_observation_epoch_ms', ARGV[1], 'event_id', ARGV[2])
return 1
"""


class RedisProtocol(Protocol):
    async def eval(self, script: str, numkeys: int, *keys_and_args: object) -> object: ...

    async def hget(self, name: str, key: str) -> object | None: ...


class ApplyResult(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    STALE = "stale"
    COORDINATE_MISSING = "coordinate_missing"


def epoch_ms(value: datetime) -> int:
    return int(value.astimezone(UTC).timestamp() * 1000)


def _int_from_redis(value: object) -> int:
    if isinstance(value, bool):
        msg = "redis lua response must be an integer"
        raise RuntimeError(msg)
    if isinstance(value, int):
        return value
    if isinstance(value, bytes):
        return int(value.decode("utf-8"))
    if isinstance(value, str):
        return int(value)
    msg = "redis lua response must be an integer"
    raise RuntimeError(msg)


@dataclass(slots=True)
class AircraftProjection:
    redis: RedisProtocol
    namespace: str = "aether"
    source_name: str = "opensky"

    def _aircraft_key(self, icao24: str) -> str:
        return f"{self.namespace}:aircraft:{icao24}"

    @property
    def _index_key(self) -> str:
        return f"{self.namespace}:aircraft:index"

    @property
    def _updates_channel(self) -> str:
        return f"{self.namespace}:aircraft:updates"

    @property
    def _source_key(self) -> str:
        return f"{self.namespace}:source:{self.source_name}"

    async def apply(self, event: AircraftPositionEvent) -> ApplyResult:
        observed_ms = epoch_ms(event.observed_at)
        await self.redis.eval(
            LUA_UPDATE_SOURCE,
            1,
            self._source_key,
            observed_ms,
            event.event_id,
        )

        if event.aircraft.position is None:
            return ApplyResult.COORDINATE_MISSING

        projected = ProjectedAircraft(
            **event.aircraft.model_dump(mode="python"),
            event_id=event.event_id,
            observed_at=event.observed_at,
        )
        aircraft_key = self._aircraft_key(event.aircraft.icao24)
        result_code = await self.redis.eval(
            LUA_APPLY_PROJECTION,
            3,
            aircraft_key,
            self._index_key,
            self._updates_channel,
            observed_ms,
            event.event_id,
            projected.model_dump_json(),
            event.aircraft.icao24,
        )
        code = _int_from_redis(result_code)
        if code == 0:
            return ApplyResult.ACCEPTED
        if code == 1:
            return ApplyResult.DUPLICATE
        if code == 2:
            return ApplyResult.STALE
        msg = f"unexpected lua apply result code: {code}"
        raise RuntimeError(msg)

    async def get(self, icao24: str) -> ProjectedAircraft | None:
        payload = await self.redis.hget(self._aircraft_key(icao24), "payload")
        if payload is None:
            return None
        if isinstance(payload, bytes):
            decoded = payload.decode("utf-8")
        else:
            decoded = str(payload)
        return ProjectedAircraft.model_validate_json(decoded)
