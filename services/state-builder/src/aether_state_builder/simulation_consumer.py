from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from aether_contracts import SimulationEvent
from pydantic import ValidationError

from aether_state_builder.simulation_projection import SimulationProjection


@dataclass(slots=True)
class SimulationConsumerRecord:
    key: bytes | None
    value: bytes


class SimulationConsumerProtocol(Protocol):
    async def getone(self) -> SimulationConsumerRecord | None: ...

    async def commit(self, record: SimulationConsumerRecord) -> None: ...


class SimulationConsumeResult(StrEnum):
    APPLIED = "applied"
    REJECTED = "rejected"
    RETRY = "retry"
    EMPTY = "empty"


async def consume_simulation_one(
    consumer: SimulationConsumerProtocol,
    projection: SimulationProjection,
) -> SimulationConsumeResult:
    record = await consumer.getone()
    if record is None:
        return SimulationConsumeResult.EMPTY

    try:
        event = SimulationEvent.model_validate_json(record.value)
    except ValidationError:
        await consumer.commit(record)
        return SimulationConsumeResult.REJECTED

    try:
        await projection.apply(event)
    except ConnectionError:
        return SimulationConsumeResult.RETRY

    await consumer.commit(record)
    return SimulationConsumeResult.APPLIED
