from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from typing import Annotated, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)


def _serialize_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _normalize_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


class MobilityDensityEvent(BaseModel):
    event_id: str
    event_type: Literal["mobility.density"] = "mobility.density"
    schema_version: Literal[1] = 1
    generated_at: AwareDatetime
    window_started_at: AwareDatetime
    window_ended_at: AwareDatetime
    source_event_watermark: AwareDatetime
    cell_id: str
    h3_resolution: Annotated[int, Field(ge=0, le=15)]
    aircraft_count: Annotated[int, Field(ge=0)]
    airborne_count: Annotated[int, Field(ge=0)]
    on_ground_count: Annotated[int, Field(ge=0)]
    average_altitude_m: float | None = None
    average_ground_speed_mps: float | None = None
    provenance: Literal["derived"] = "derived"

    @field_validator(
        "generated_at",
        "window_started_at",
        "window_ended_at",
        "source_event_watermark",
    )
    @classmethod
    def normalize_datetimes(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer(
        "generated_at",
        "window_started_at",
        "window_ended_at",
        "source_event_watermark",
    )
    def serialize_datetimes(self, value: datetime) -> str:
        return _serialize_utc(value)

    @model_validator(mode="after")
    def counts_match(self) -> Self:
        if self.airborne_count + self.on_ground_count != self.aircraft_count:
            msg = "airborne_count plus on_ground_count must equal aircraft_count"
            raise ValueError(msg)
        return self


class MobilityDensityRemoved(BaseModel):
    event_id: str
    event_type: Literal["mobility.density.removed"] = "mobility.density.removed"
    schema_version: Literal[1] = 1
    removed_at: AwareDatetime
    cell_id: str
    h3_resolution: Annotated[int, Field(ge=0, le=15)]
    last_window_ended_at: AwareDatetime
    provenance: Literal["derived"] = "derived"

    @field_validator("removed_at", "last_window_ended_at")
    @classmethod
    def normalize_datetimes(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("removed_at", "last_window_ended_at")
    def serialize_datetimes(self, value: datetime) -> str:
        return _serialize_utc(value)


class RegionalDemandEvent(BaseModel):
    event_id: str
    event_type: Literal["demand.region"] = "demand.region"
    schema_version: Literal[1] = 1
    generated_at: AwareDatetime
    window_started_at: AwareDatetime
    window_ended_at: AwareDatetime
    cell_id: str
    h3_resolution: Annotated[int, Field(ge=0, le=15)]
    aircraft_count: Annotated[int, Field(ge=0)]
    demand_units: Annotated[float, Field(ge=0)]
    model_version: Literal["density-linear-v1"] = "density-linear-v1"
    coefficient_set: Literal["default-v1"] = "default-v1"
    provenance: Literal["modelled"] = "modelled"

    @field_validator("generated_at", "window_started_at", "window_ended_at")
    @classmethod
    def normalize_datetimes(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("generated_at", "window_started_at", "window_ended_at")
    def serialize_datetimes(self, value: datetime) -> str:
        return _serialize_utc(value)


def deterministic_mobility_event_id(
    kind: str,
    cell_id: str,
    window_ended_at: datetime,
    variant: str,
) -> str:
    stable = ":".join(
        [
            kind.strip().lower(),
            cell_id.strip().lower(),
            _serialize_utc(window_ended_at),
            variant.strip().lower(),
        ]
    )
    return sha256(stable.encode("utf-8")).hexdigest()[:26]
