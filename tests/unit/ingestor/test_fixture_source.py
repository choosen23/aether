from datetime import UTC, datetime
from pathlib import Path

import pytest
from aether_ingestor.config import IngestorSettings
from aether_ingestor.fixture_source import FixtureSource

FIXTURE = Path("tests/fixtures/opensky/states-europe.json")


def test_fixture_source_retimestamps_recorded_rows() -> None:
    now = datetime(2026, 9, 24, 9, 10, tzinfo=UTC)
    source = FixtureSource(
        IngestorSettings(source_mode="fixture", environment="test"),
        fixture_path=FIXTURE,
    )

    response = source.fetch_states(now)

    assert response.time == int(now.timestamp())
    assert response.states is not None
    assert len(response.states) == 2
    assert response.states[0][3] == int(now.timestamp())
    assert response.states[1][3] == int(now.timestamp()) + 10


def test_fixture_source_is_forbidden_in_production() -> None:
    source = FixtureSource(
        IngestorSettings(source_mode="fixture", environment="production"),
        fixture_path=FIXTURE,
    )

    with pytest.raises(RuntimeError, match="forbidden in production"):
        source.fetch_states(datetime(2026, 9, 24, 9, 10, tzinfo=UTC))
