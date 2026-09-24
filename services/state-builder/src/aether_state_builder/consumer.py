from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from aether_contracts import AircraftPositionEvent
from pydantic import ValidationError

from aether_state_builder.metrics import consumer_records_total, projection_events_total


class ProjectionProtocol(Protocol):
    async def apply(self, event: AircraftPositionEvent) -> object: ...


@dataclass(slots=True)
class ConsumerRecord:
    key: bytes | None
    value: bytes


class ConsumerProtocol(Protocol):
    async def getone(self) -> ConsumerRecord | None: ...

    async def commit(self, record: ConsumerRecord) -> None: ...


class ConsumeResult(StrEnum):
    APPLIED = "applied"
    REJECTED = "rejected"
    RETRY = "retry"
    EMPTY = "empty"


async def consume_one(consumer: ConsumerProtocol, projection: ProjectionProtocol) -> ConsumeResult:
    record = await consumer.getone()
    if record is None:
        consumer_records_total.labels(result="empty").inc()
        return ConsumeResult.EMPTY

    try:
        event = AircraftPositionEvent.model_validate_json(record.value)
    except ValidationError:
        await consumer.commit(record)
        consumer_records_total.labels(result="rejected").inc()
        return ConsumeResult.REJECTED

    try:
        result = await projection.apply(event)
    except ConnectionError:
        consumer_records_total.labels(result="retry").inc()
        return ConsumeResult.RETRY

    await consumer.commit(record)
    consumer_records_total.labels(result="applied").inc()
    projection_events_total.labels(result=str(result)).inc()
    return ConsumeResult.APPLIED
