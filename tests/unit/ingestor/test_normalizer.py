from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from aether_ingestor.normalizer import RowRejection, normalize_response
from aether_ingestor.opensky import OpenSkyResponse

FIXED_NOW = datetime(2026, 9, 24, 9, 10, 1, tzinfo=UTC)
FIXED_EPOCH = 1_798_000_000
VALID_ROW: list[object | None] = [
    "3c6444",
    "DLH4YA",
    "Germany",
    FIXED_EPOCH,
    FIXED_EPOCH,
    8.56,
    50.04,
    10668.0,
    False,
    232.1,
    271.4,
    -1.2,
    None,
    None,
    None,
    False,
    0,
]


@pytest.fixture
def load_fixture() -> callable:
    def _load(relative_path: str) -> dict[str, object]:
        fixture_path = Path("tests/fixtures") / relative_path
        return json.loads(fixture_path.read_text())

    return _load


def test_normalizes_fixture_and_preserves_nullable_values(load_fixture: callable) -> None:
    raw = load_fixture("opensky/states-europe.json")
    result = normalize_response(OpenSkyResponse.model_validate(raw), FIXED_NOW)
    assert result.events[0].aircraft.icao24 == "3c6444"
    assert result.events[0].aircraft.callsign == "DLH4YA"
    assert result.events[0].aircraft.position_source == "ads_b"
    assert result.events[1].aircraft.callsign is None


def test_bad_row_does_not_discard_valid_siblings() -> None:
    response = OpenSkyResponse(time=FIXED_EPOCH, states=[VALID_ROW, ["bad"]])
    result = normalize_response(response, FIXED_NOW)
    assert len(result.events) == 1
    assert result.rejections == [
        RowRejection(index=1, reason="state vector has 1 fields; expected at least 17")
    ]
