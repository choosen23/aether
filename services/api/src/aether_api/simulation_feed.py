from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime
from typing import Protocol


class RedisStreamProtocol(Protocol):
    async def xread(
        self,
        streams: dict[str, str],
        count: int = 100,
        block: int = 1000,
    ) -> object: ...


class SimulationRepositoryProtocol(Protocol):
    async def get_experiment(self, run_id: str) -> dict[str, object] | None: ...


class SimulationBroadcasterProtocol(Protocol):
    async def publish(self, run_id: str, message: dict[str, object]) -> None: ...


class SimulationFeed:
    def __init__(
        self,
        *,
        redis: RedisStreamProtocol,
        broadcaster: SimulationBroadcasterProtocol,
        repository: SimulationRepositoryProtocol,
        namespace: str,
    ) -> None:
        self._redis = redis
        self._broadcaster = broadcaster
        self._repository = repository
        self._stream_key = f"{namespace}:simulation:changes"
        self._cursor = "0-0"

    async def run(self) -> None:
        with suppress(asyncio.CancelledError):
            while True:
                records = await self._redis.xread(
                    {self._stream_key: self._cursor},
                    count=100,
                    block=1000,
                )
                if not records:
                    continue
                await self._process_records(records)

    async def _process_records(self, records: object) -> None:
        if not isinstance(records, list):
            return
        for stream_chunk in records:
            if not (isinstance(stream_chunk, list | tuple) and len(stream_chunk) == 2):
                continue
            entries = stream_chunk[1]
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not (isinstance(entry, list | tuple) and len(entry) == 2):
                    continue
                entry_id = _decode(entry[0])
                fields = _normalize_fields(entry[1])
                run_id = fields.get("run_id")
                if run_id is None:
                    self._cursor = entry_id
                    continue
                snapshot = await self._repository.get_experiment(run_id)
                if snapshot is not None:
                    await self._broadcaster.publish(
                        run_id,
                        {
                            "message_type": "simulation.run.updated",
                            "sent_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                            "run": snapshot,
                        },
                    )
                self._cursor = entry_id


def _normalize_fields(raw: object) -> dict[str, str]:
    if isinstance(raw, dict):
        return {_decode(k): _decode(v) for k, v in raw.items()}
    if isinstance(raw, list | tuple):
        values = list(raw)
        output: dict[str, str] = {}
        for index in range(0, len(values) - 1, 2):
            output[_decode(values[index])] = _decode(values[index + 1])
        return output
    return {}


def _decode(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)
