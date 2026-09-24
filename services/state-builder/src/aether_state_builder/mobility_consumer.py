from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from aether_contracts import MobilityDensityEvent, MobilityDensityRemoved, RegionalDemandEvent
from pydantic import ValidationError

from aether_state_builder.metrics import (
    mobility_consumer_records_total,
    mobility_projection_events_total,
)


class MobilityProjectionProtocol(Protocol):
    async def apply_density(self, density: MobilityDensityEvent) -> object: ...

    async def apply_demand(self, demand: RegionalDemandEvent) -> object: ...

    async def remove(self, cell_id: str, sent_at: datetime) -> None: ...


@dataclass(slots=True)
class MobilityConsumerRecord:
    key: bytes | None
    value: bytes


class MobilityConsumerProtocol(Protocol):
    async def getone(self) -> MobilityConsumerRecord | None: ...

    async def commit(self, record: MobilityConsumerRecord) -> None: ...


class MobilityConsumeResult(StrEnum):
    APPLIED = "applied"
    REJECTED = "rejected"
    RETRY = "retry"
    EMPTY = "empty"


def _parse_mobility_record(
    value: bytes,
) -> MobilityDensityEvent | MobilityDensityRemoved | RegionalDemandEvent:
    try:
        return MobilityDensityEvent.model_validate_json(value)
    except ValidationError:
        pass
    try:
        return MobilityDensityRemoved.model_validate_json(value)
    except ValidationError:
        pass
    return RegionalDemandEvent.model_validate_json(value)


async def consume_mobility_one(
    consumer: MobilityConsumerProtocol,
    projection: MobilityProjectionProtocol,
) -> MobilityConsumeResult:
    record = await consumer.getone()
    if record is None:
        mobility_consumer_records_total.labels(result="empty").inc()
        return MobilityConsumeResult.EMPTY

    try:
        event = _parse_mobility_record(record.value)
    except ValidationError:
        await consumer.commit(record)
        mobility_consumer_records_total.labels(result="rejected").inc()
        return MobilityConsumeResult.REJECTED

    try:
        if isinstance(event, MobilityDensityEvent):
            result = await projection.apply_density(event)
            mobility_projection_events_total.labels(result=f"density_{result}").inc()
        elif isinstance(event, RegionalDemandEvent):
            result = await projection.apply_demand(event)
            mobility_projection_events_total.labels(result=f"demand_{result}").inc()
        else:
            await projection.remove(event.cell_id, datetime.now(tz=UTC))
            mobility_projection_events_total.labels(result="removed").inc()
    except ConnectionError:
        mobility_consumer_records_total.labels(result="retry").inc()
        return MobilityConsumeResult.RETRY
    except ValueError:
        await consumer.commit(record)
        mobility_consumer_records_total.labels(result="rejected").inc()
        return MobilityConsumeResult.REJECTED

    await consumer.commit(record)
    mobility_consumer_records_total.labels(result="applied").inc()
    return MobilityConsumeResult.APPLIED
