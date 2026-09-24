from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from aether_contracts import (
    AircraftPositionEvent,
    AircraftState,
    Motion,
    Position,
    deterministic_event_id,
)
from pydantic import BaseModel, ValidationError

from aether_ingestor.opensky import OpenSkyResponse

Numeric = int | float
PositionSource = Literal["ads_b", "asterix", "mlat", "other"]

OPEN_SKY_FIELDS = {
    "icao24": 0,
    "callsign": 1,
    "origin_country": 2,
    "time_position": 3,
    "last_contact": 4,
    "longitude": 5,
    "latitude": 6,
    "baro_altitude": 7,
    "on_ground": 8,
    "velocity": 9,
    "true_track": 10,
    "vertical_rate": 11,
    "position_source": 16,
}

POSITION_SOURCE_MAP: dict[int, PositionSource] = {
    0: "ads_b",
    1: "asterix",
    2: "mlat",
}

class RowRejection(BaseModel):
    index: int
    reason: str


class NormalizationResult(BaseModel):
    events: list[AircraftPositionEvent]
    rejections: list[RowRejection]


def _epoch_seconds_to_utc(value: Numeric | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value), tz=UTC)


def _row_value(row: list[object | None], field_name: str) -> object | None:
    return row[OPEN_SKY_FIELDS[field_name]]


def _as_numeric(value: object | None, *, field_name: str) -> Numeric | None:
    if value is None:
        return None
    if isinstance(value, bool):
        msg = f"{field_name} must be numeric"
        raise ValueError(msg)
    if isinstance(value, int | float):
        return value
    msg = f"{field_name} must be numeric"
    raise ValueError(msg)


def _as_position_source(value: object | None) -> PositionSource | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        msg = "position_source must be numeric"
        raise ValueError(msg)
    return POSITION_SOURCE_MAP.get(int(value), "other")


def normalize_row(
    row: list[object | None],
    ingested_at: datetime,
    response_time: int,
) -> AircraftPositionEvent:
    if len(row) < 17:
        msg = f"state vector has {len(row)} fields; expected at least 17"
        raise ValueError(msg)

    observed_at_seconds = (
        _as_numeric(_row_value(row, "time_position"), field_name="time_position")
        or _as_numeric(_row_value(row, "last_contact"), field_name="last_contact")
        or response_time
    )
    observed_at = _epoch_seconds_to_utc(observed_at_seconds)
    if observed_at is None:
        msg = "state vector has no usable observation timestamp"
        raise ValueError(msg)

    latitude = _as_numeric(_row_value(row, "latitude"), field_name="latitude")
    longitude = _as_numeric(_row_value(row, "longitude"), field_name="longitude")
    barometric_altitude = _as_numeric(_row_value(row, "baro_altitude"), field_name="baro_altitude")
    velocity = _as_numeric(_row_value(row, "velocity"), field_name="velocity")
    true_track = _as_numeric(_row_value(row, "true_track"), field_name="true_track")
    vertical_rate = _as_numeric(_row_value(row, "vertical_rate"), field_name="vertical_rate")
    last_contact = _as_numeric(_row_value(row, "last_contact"), field_name="last_contact")

    position: Position | None = None
    if latitude is not None and longitude is not None:
        position = Position(
            latitude=float(latitude),
            longitude=float(longitude),
            barometric_altitude_m=float(barometric_altitude)
            if barometric_altitude is not None
            else None,
            on_ground=bool(_row_value(row, "on_ground") or False),
        )

    motion = Motion(
        ground_speed_mps=float(velocity) if velocity is not None else None,
        track_degrees=float(true_track) if true_track is not None else None,
        vertical_rate_mps=float(vertical_rate) if vertical_rate is not None else None,
    )

    callsign_raw = _row_value(row, "callsign")
    position_source = _as_position_source(_row_value(row, "position_source"))

    aircraft = AircraftState(
        icao24=str(_row_value(row, "icao24") or ""),
        callsign=str(callsign_raw) if callsign_raw is not None else None,
        origin_country=str(_row_value(row, "origin_country") or "Unknown"),
        position=position,
        motion=motion,
        last_contact_at=_epoch_seconds_to_utc(last_contact),
        position_source=position_source,
    )

    return AircraftPositionEvent(
        event_id=deterministic_event_id("opensky", aircraft.icao24, observed_at),
        observed_at=observed_at,
        ingested_at=ingested_at,
        source="opensky",
        aircraft=aircraft,
    )


def normalize_response(response: OpenSkyResponse, ingested_at: datetime) -> NormalizationResult:
    events: list[AircraftPositionEvent] = []
    rejections: list[RowRejection] = []
    for index, row in enumerate(response.states or []):
        try:
            events.append(normalize_row(row, ingested_at, response.time))
        except (TypeError, ValueError, ValidationError) as exc:
            rejections.append(RowRejection(index=index, reason=str(exc)))
    return NormalizationResult(events=events, rejections=rejections)
