from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_contracts import AircraftPositionEvent, AircraftState, deterministic_event_id
from pydantic import ValidationError

EXAMPLE = {
    "event_id": "e0d9ce8f95a0f9f3383a90d043",
    "event_type": "aircraft.position",
    "schema_version": 1,
    "observed_at": "2026-09-24T09:10:00Z",
    "ingested_at": "2026-09-24T09:10:01.234Z",
    "source": "opensky",
    "aircraft": {
        "icao24": "3C6444",
        "callsign": " dlh4ya ",
        "origin_country": "Germany",
        "position": {
            "latitude": 50.04,
            "longitude": 8.56,
            "barometric_altitude_m": 10668.0,
            "on_ground": False,
        },
        "motion": {
            "ground_speed_mps": 232.1,
            "track_degrees": 271.4,
            "vertical_rate_mps": -1.2,
        },
        "last_contact_at": "2026-09-24T09:10:00Z",
        "position_source": "ads_b",
    },
}


def test_event_normalizes_identity_and_serializes_utc() -> None:
    event = AircraftPositionEvent.model_validate(EXAMPLE)
    assert event.aircraft.icao24 == "3c6444"
    assert event.aircraft.callsign == "DLH4YA"
    assert event.schema_version == 1
    assert event.model_dump(mode="json")["observed_at"].endswith("Z")


def test_event_id_is_stable() -> None:
    observed = datetime(2026, 9, 24, 9, 10, tzinfo=UTC)
    assert deterministic_event_id("opensky", "3c6444", observed) == deterministic_event_id(
        "opensky", "3C6444", observed
    )


@pytest.mark.parametrize("icao24", ["", "XYZ123", "abc12", "abcdef0"])
def test_invalid_icao24_is_rejected(icao24: str) -> None:
    with pytest.raises(ValidationError):
        AircraftState(icao24=icao24, origin_country="Unknown")
