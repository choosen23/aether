from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, Field, field_serializer, field_validator

from aether_contracts.api import SourceStatus
from aether_contracts.mobility import MobilityDensityEvent, RegionalDemandEvent


def _serialize_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _normalize_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


class MobilityCell(BaseModel):
    density: MobilityDensityEvent
    demand: RegionalDemandEvent


class MobilitySnapshot(BaseModel):
    generated_at: AwareDatetime
    source_status: SourceStatus
    cells: list[MobilityCell]

    @field_validator("generated_at")
    @classmethod
    def normalize_generated_at(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("generated_at")
    def serialize_generated_at(self, value: datetime) -> str:
        return _serialize_utc(value)


class MobilityCellUpsert(BaseModel):
    message_type: Literal["mobility.cell.upsert"] = "mobility.cell.upsert"
    schema_version: Literal[1] = 1
    sent_at: AwareDatetime
    cells: list[MobilityCell]

    @field_validator("sent_at")
    @classmethod
    def normalize_sent_at(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("sent_at")
    def serialize_sent_at(self, value: datetime) -> str:
        return _serialize_utc(value)


class MobilityCellRemove(BaseModel):
    message_type: Literal["mobility.cell.remove"] = "mobility.cell.remove"
    schema_version: Literal[1] = 1
    sent_at: AwareDatetime
    cell_ids: list[str]

    @field_validator("sent_at")
    @classmethod
    def normalize_sent_at(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("sent_at")
    def serialize_sent_at(self, value: datetime) -> str:
        return _serialize_utc(value)


class MobilityHeartbeat(BaseModel):
    message_type: Literal["mobility.heartbeat"] = "mobility.heartbeat"
    schema_version: Literal[1] = 1
    sent_at: AwareDatetime
    source_status: SourceStatus

    @field_validator("sent_at")
    @classmethod
    def normalize_sent_at(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("sent_at")
    def serialize_sent_at(self, value: datetime) -> str:
        return _serialize_utc(value)


class MobilityStreamReady(BaseModel):
    message_type: Literal["mobility.stream.ready"] = "mobility.stream.ready"
    schema_version: Literal[1] = 1
    sent_at: AwareDatetime

    @field_validator("sent_at")
    @classmethod
    def normalize_sent_at(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("sent_at")
    def serialize_sent_at(self, value: datetime) -> str:
        return _serialize_utc(value)


MobilityStreamMessage = Annotated[
    MobilityStreamReady | MobilityCellUpsert | MobilityCellRemove | MobilityHeartbeat,
    Field(discriminator="message_type"),
]
