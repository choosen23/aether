from __future__ import annotations

from datetime import datetime, timedelta
from typing import Protocol

from aether_contracts import MobilityCell, MobilitySnapshot, SourceStatus

from aether_api.config import ApiSettings
from aether_api.repository import epoch_ms

LUA_REMOVE_EXPIRED_MOBILITY = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, id in ipairs(ids) do
  redis.call('DEL', ARGV[2] .. id)
end
if #ids > 0 then
  redis.call('ZREM', KEYS[1], unpack(ids))
end
return ids
"""


class RedisMobilityReadProtocol(Protocol):
    async def zrangebyscore(self, key: str, min_score: int, max_score: str) -> list[object]: ...

    async def hget(self, key: str, field: str) -> object | None: ...

    async def eval(self, script: str, numkeys: int, *keys_and_args: object) -> object: ...


class SourceRepositoryProtocol(Protocol):
    async def source_status(self, now: datetime) -> SourceStatus: ...


class MobilityRepository:
    def __init__(
        self,
        redis: RedisMobilityReadProtocol,
        source_repository: SourceRepositoryProtocol,
        settings: ApiSettings,
    ) -> None:
        self._redis = redis
        self._source_repository = source_repository
        self._namespace = settings.redis_namespace
        self._offline_seconds = settings.source_offline_max_age_seconds
        self._max_snapshot = settings.max_snapshot_aircraft

    @property
    def _index_key(self) -> str:
        return f"{self._namespace}:mobility:index"

    def _cell_key(self, cell_id: str) -> str:
        return f"{self._namespace}:mobility:{cell_id}"

    async def snapshot(self, now: datetime) -> MobilitySnapshot:
        minimum_ms = epoch_ms(now - timedelta(seconds=self._offline_seconds))
        ids_raw = await self._redis.zrangebyscore(self._index_key, minimum_ms, "+inf")
        ids = sorted(self._decode_string(value) for value in ids_raw)[: self._max_snapshot]
        cells = await self._load_cells(ids)
        source = await self._source_repository.source_status(now)
        return MobilitySnapshot(generated_at=now, source_status=source, cells=cells)

    async def source_status(self, now: datetime) -> SourceStatus:
        return await self._source_repository.source_status(now)

    async def remove_expired(self, now: datetime) -> list[str]:
        cutoff = epoch_ms(now - timedelta(seconds=self._offline_seconds))
        values = await self._redis.eval(
            LUA_REMOVE_EXPIRED_MOBILITY,
            1,
            self._index_key,
            cutoff,
            f"{self._namespace}:mobility:",
        )
        if not isinstance(values, list):
            msg = "redis mobility expiry script returned a non-list response"
            raise RuntimeError(msg)
        return sorted(self._decode_string(value) for value in values)

    async def _load_cells(self, ids: list[str]) -> list[MobilityCell]:
        output: list[MobilityCell] = []
        for cell_id in ids:
            payload = await self._redis.hget(self._cell_key(cell_id), "payload")
            if payload is None:
                continue
            output.append(MobilityCell.model_validate_json(self._decode_string(payload)))
        return sorted(output, key=lambda cell: cell.density.cell_id)

    @staticmethod
    def _decode_string(value: object) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)
