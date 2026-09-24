from __future__ import annotations

from pathlib import Path

import yaml


def asyncapi_document() -> dict[str, object]:
    return yaml.safe_load(Path("contracts/asyncapi.yaml").read_text())


def test_asyncapi_defines_topic_and_all_stream_messages() -> None:
    document = asyncapi_document()
    assert "aircraft.position.v1" in document["channels"]
    assert "mobility.density.v1" in document["channels"]
    assert "demand.region.v1" in document["channels"]
    schemas = document["components"]["schemas"]
    assert {
        "AircraftPositionEvent",
        "StreamReady",
        "AircraftUpsert",
        "AircraftRemove",
        "Heartbeat",
        "MobilityDensityEvent",
        "MobilityDensityRemoved",
        "RegionalDemandEvent",
        "MobilityCell",
        "MobilityStreamReady",
        "MobilityCellUpsert",
        "MobilityCellRemove",
        "MobilityHeartbeat",
    } <= schemas.keys()
