from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from aether_ingestor.config import IngestorSettings
from aether_ingestor.opensky import OpenSkyResponse


class FixtureSource:
    def __init__(self, settings: IngestorSettings, fixture_path: Path | None = None) -> None:
        self._settings = settings
        self._fixture_path = fixture_path or Path("tests/fixtures/opensky/states-europe.json")

    def fetch_states(self, now: datetime) -> OpenSkyResponse:
        if self._settings.source_mode != "fixture":
            msg = "fixture source requested while source mode is not fixture"
            raise RuntimeError(msg)
        if self._settings.environment == "production":
            msg = "fixture source mode is forbidden in production"
            raise RuntimeError(msg)

        raw = json.loads(self._fixture_path.read_text())
        response = OpenSkyResponse.model_validate(raw)

        first_ts = int(now.astimezone(UTC).timestamp())
        second_ts = int((now.astimezone(UTC) + timedelta(seconds=10)).timestamp())
        adjusted = []
        for index, row in enumerate(response.states or []):
            copy = list(row)
            copy[3] = first_ts if index == 0 else second_ts
            copy[4] = copy[3]
            adjusted.append(copy)

        return OpenSkyResponse(time=first_ts, states=adjusted[:2])
