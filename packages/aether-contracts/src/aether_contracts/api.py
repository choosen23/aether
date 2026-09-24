from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, Field, field_serializer, field_validator

from aether_contracts.aircraft import ProjectedAircraft


def _serialize_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _normalize_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


class SourceStatus(BaseModel):
    source: Literal["opensky"]
    status: Literal["live", "stale", "offline"]
    last_observation_at: AwareDatetime | None
    age_seconds: float | None

    @field_validator("last_observation_at")
    @classmethod
    def normalize_last_observation_at(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return _normalize_utc(value)

    @field_serializer("last_observation_at")
    def serialize_last_observation_at(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return _serialize_utc(value)


class AircraftSnapshot(BaseModel):
    generated_at: AwareDatetime
    source_status: SourceStatus
    aircraft: list[ProjectedAircraft]

    @field_validator("generated_at")
    @classmethod
    def normalize_generated_at(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("generated_at")
    def serialize_generated_at(self, value: datetime) -> str:
        return _serialize_utc(value)


class StreamReady(BaseModel):
    message_type: Literal["stream.ready"] = "stream.ready"
    schema_version: Literal[1] = 1
    sent_at: AwareDatetime

    @field_validator("sent_at")
    @classmethod
    def normalize_sent_at(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("sent_at")
    def serialize_sent_at(self, value: datetime) -> str:
        return _serialize_utc(value)


class AircraftUpsert(BaseModel):
    message_type: Literal["aircraft.upsert"] = "aircraft.upsert"
    schema_version: Literal[1] = 1
    sent_at: AwareDatetime
    aircraft: list[ProjectedAircraft]

    @field_validator("sent_at")
    @classmethod
    def normalize_sent_at(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("sent_at")
    def serialize_sent_at(self, value: datetime) -> str:
        return _serialize_utc(value)


class AircraftRemove(BaseModel):
    message_type: Literal["aircraft.remove"] = "aircraft.remove"
    schema_version: Literal[1] = 1
    sent_at: AwareDatetime
    icao24: list[str]

    @field_validator("sent_at")
    @classmethod
    def normalize_sent_at(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("sent_at")
    def serialize_sent_at(self, value: datetime) -> str:
        return _serialize_utc(value)


class Heartbeat(BaseModel):
    message_type: Literal["heartbeat"] = "heartbeat"
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


StreamMessage = Annotated[
    StreamReady | AircraftUpsert | AircraftRemove | Heartbeat,
    Field(discriminator="message_type"),
]
