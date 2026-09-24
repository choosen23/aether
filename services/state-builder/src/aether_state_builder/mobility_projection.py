from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

import h3  # type: ignore[import-untyped]
from aether_contracts import (
    MobilityCellRemove,
    MobilityDensityEvent,
    RegionalDemandEvent,
)

LUA_APPLY_DENSITY = """
local current = redis.call('HGET', KEYS[1], 'density_window_epoch_ms')
if current and tonumber(current) > tonumber(ARGV[1]) then return 2 end
redis.call('HSET', KEYS[1],
  'density_window_epoch_ms', ARGV[1],
  'density_payload', ARGV[2])
local demand_window = redis.call('HGET', KEYS[1], 'demand_window_epoch_ms')
if demand_window and tonumber(demand_window) == tonumber(ARGV[1]) then
  local demand_payload = redis.call('HGET', KEYS[1], 'demand_payload')
  local combined = cjson.encode({
    density = cjson.decode(ARGV[2]),
    demand = cjson.decode(demand_payload)
  })
  redis.call('HSET', KEYS[1], 'payload', combined, 'window_epoch_ms', ARGV[1])
  redis.call('ZADD', KEYS[2], ARGV[1], ARGV[3])
  redis.call('PUBLISH', KEYS[3], combined)
  return 1
end
return 0
"""

LUA_APPLY_DEMAND = """
local current = redis.call('HGET', KEYS[1], 'demand_window_epoch_ms')
if current and tonumber(current) > tonumber(ARGV[1]) then return 2 end
redis.call('HSET', KEYS[1],
  'demand_window_epoch_ms', ARGV[1],
  'demand_payload', ARGV[2])
local density_window = redis.call('HGET', KEYS[1], 'density_window_epoch_ms')
if density_window and tonumber(density_window) == tonumber(ARGV[1]) then
  local density_payload = redis.call('HGET', KEYS[1], 'density_payload')
  local combined = cjson.encode({
    density = cjson.decode(density_payload),
    demand = cjson.decode(ARGV[2])
  })
  redis.call('HSET', KEYS[1], 'payload', combined, 'window_epoch_ms', ARGV[1])
  redis.call('ZADD', KEYS[2], ARGV[1], ARGV[3])
  redis.call('PUBLISH', KEYS[3], combined)
  return 1
end
return 0
"""

LUA_REMOVE_MOBILITY = """
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('PUBLISH', KEYS[3], ARGV[2])
return 1
"""


class RedisProtocol(Protocol):
    async def eval(self, script: str, numkeys: int, *keys_and_args: object) -> object: ...


class MobilityProjectionResult(StrEnum):
    UPDATED = "updated"
    JOINED = "joined"
    STALE = "stale"


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
class MobilityProjection:
    redis: RedisProtocol
    namespace: str = "aether"

    def _mobility_key(self, cell_id: str) -> str:
        return f"{self.namespace}:mobility:{cell_id}"

    @property
    def _index_key(self) -> str:
        return f"{self.namespace}:mobility:index"

    @property
    def _updates_channel(self) -> str:
        return f"{self.namespace}:mobility:updates"

    async def apply_density(self, density: MobilityDensityEvent) -> MobilityProjectionResult:
        if not h3.is_valid_cell(density.cell_id):
            msg = "invalid h3 cell"
            raise ValueError(msg)
        code = _int_from_redis(
            await self.redis.eval(
                LUA_APPLY_DENSITY,
                3,
                self._mobility_key(density.cell_id),
                self._index_key,
                self._updates_channel,
                epoch_ms(density.window_ended_at),
                density.model_dump_json(),
                density.cell_id,
            )
        )
        if code == 0:
            return MobilityProjectionResult.UPDATED
        if code == 1:
            return MobilityProjectionResult.JOINED
        if code == 2:
            return MobilityProjectionResult.STALE
        msg = f"unexpected mobility density lua result code: {code}"
        raise RuntimeError(msg)

    async def apply_demand(self, demand: RegionalDemandEvent) -> MobilityProjectionResult:
        if not h3.is_valid_cell(demand.cell_id):
            msg = "invalid h3 cell"
            raise ValueError(msg)
        code = _int_from_redis(
            await self.redis.eval(
                LUA_APPLY_DEMAND,
                3,
                self._mobility_key(demand.cell_id),
                self._index_key,
                self._updates_channel,
                epoch_ms(demand.window_ended_at),
                demand.model_dump_json(),
                demand.cell_id,
            )
        )
        if code == 0:
            return MobilityProjectionResult.UPDATED
        if code == 1:
            return MobilityProjectionResult.JOINED
        if code == 2:
            return MobilityProjectionResult.STALE
        msg = f"unexpected mobility demand lua result code: {code}"
        raise RuntimeError(msg)

    async def remove(self, cell_id: str, sent_at: datetime) -> None:
        if not h3.is_valid_cell(cell_id):
            msg = "invalid h3 cell"
            raise ValueError(msg)
        payload = MobilityCellRemove(sent_at=sent_at, cell_ids=[cell_id]).model_dump_json()
        await self.redis.eval(
            LUA_REMOVE_MOBILITY,
            3,
            self._mobility_key(cell_id),
            self._index_key,
            self._updates_channel,
            cell_id,
            payload,
        )
