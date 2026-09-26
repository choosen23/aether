from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from aether_contracts import SimulationEvent


class RedisProtocol(Protocol):
    async def hget(self, name: str, key: str) -> object | None: ...

    async def hset(self, name: str, mapping: dict[str, object]) -> object: ...

    async def zadd(self, name: str, mapping: dict[str, float]) -> object: ...

    async def xadd(self, name: str, fields: dict[str, object]) -> object: ...


class SimulationApplyResult(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"


def _to_int(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, bytes):
        return int(value.decode("utf-8"))
    return int(str(value))


@dataclass(slots=True)
class SimulationProjection:
    redis: RedisProtocol
    namespace: str = "aether"

    @property
    def _runs_key(self) -> str:
        return f"{self.namespace}:simulation:runs"

    @property
    def _changes_key(self) -> str:
        return f"{self.namespace}:simulation:changes"

    def _run_key(self, run_id: str) -> str:
        return f"{self.namespace}:simulation:run:{run_id}"

    async def apply(self, event: SimulationEvent) -> SimulationApplyResult:
        run_key = self._run_key(event.run_id)
        current_sequence = _to_int(await self.redis.hget(run_key, "last_sequence"))

        if current_sequence is not None:
            if event.sequence == current_sequence:
                return SimulationApplyResult.DUPLICATE
            if event.sequence <= current_sequence:
                return SimulationApplyResult.OUT_OF_ORDER
            if event.sequence > current_sequence + 1:
                return SimulationApplyResult.OUT_OF_ORDER

        await self.redis.hset(
            run_key,
            mapping={
                "run_id": event.run_id,
                "last_sequence": str(event.sequence),
                "simulated_time_ms": str(event.simulated_time_ms),
                "last_event_type": event.event_type,
                "last_event_payload": event.model_dump_json(),
            },
        )
        await self.redis.zadd(self._runs_key, {event.run_id: float(event.simulated_time_ms)})
        await self.redis.xadd(
            self._changes_key,
            {
                "run_id": event.run_id,
                "sequence": str(event.sequence),
                "event_type": event.event_type,
                "simulated_time_ms": str(event.simulated_time_ms),
            },
        )
        return SimulationApplyResult.ACCEPTED
