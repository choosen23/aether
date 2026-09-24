from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, Field, field_serializer, field_validator


def _serialize_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _normalize_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


class Position(BaseModel):
    latitude: Annotated[float, Field(ge=-90, le=90)]
    longitude: Annotated[float, Field(ge=-180, le=180)]
    barometric_altitude_m: float | None = None
    on_ground: bool


class Motion(BaseModel):
    ground_speed_mps: float | None = None
    track_degrees: Annotated[float, Field(ge=0, lt=360)] | None = None
    vertical_rate_mps: float | None = None

    @field_validator("track_degrees")
    @classmethod
    def normalize_track(cls, value: float | None) -> float | None:
        if value is None:
            return None
        return value % 360


class AircraftState(BaseModel):
    icao24: Annotated[str, Field(min_length=6, max_length=6)]
    callsign: str | None = None
    origin_country: str
    position: Position | None = None
    motion: Motion | None = None
    last_contact_at: AwareDatetime | None = None
    position_source: Literal["ads_b", "asterix", "mlat", "other"] | None = None

    @field_validator("icao24")
    @classmethod
    def normalize_icao24(cls, value: str) -> str:
        normalized = value.strip().lower()
        if len(normalized) != 6 or any(ch not in "0123456789abcdef" for ch in normalized):
            msg = "icao24 must be a six character lowercase hexadecimal string"
            raise ValueError(msg)
        return normalized

    @field_validator("callsign")
    @classmethod
    def normalize_callsign(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().upper()
        return normalized or None

    @field_validator("last_contact_at")
    @classmethod
    def normalize_last_contact(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return _normalize_utc(value)

    @field_serializer("last_contact_at")
    def serialize_last_contact(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return _serialize_utc(value)


class AircraftPositionEvent(BaseModel):
    event_id: str
    event_type: Literal["aircraft.position"] = "aircraft.position"
    schema_version: Literal[1] = 1
    observed_at: AwareDatetime
    ingested_at: AwareDatetime
    source: Literal["opensky"] = "opensky"
    aircraft: AircraftState

    @field_validator("observed_at", "ingested_at")
    @classmethod
    def normalize_datetimes(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("observed_at", "ingested_at")
    def serialize_datetimes(self, value: datetime) -> str:
        return _serialize_utc(value)


class ProjectedAircraft(AircraftState):
    event_id: str
    observed_at: AwareDatetime

    @field_validator("observed_at")
    @classmethod
    def normalize_observed_at(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("observed_at")
    def serialize_observed_at(self, value: datetime) -> str:
        return _serialize_utc(value)


def deterministic_event_id(source: str, icao24: str, observed_at: datetime) -> str:
    stable = f"{source.strip().lower()}:{icao24.strip().lower()}:{_serialize_utc(observed_at)}"
    return sha256(stable.encode("utf-8")).hexdigest()[:26]
