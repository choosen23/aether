from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_contracts import (
    MobilityDensityEvent,
    RegionalDemandEvent,
    deterministic_mobility_event_id,
)
from pydantic import ValidationError

DENSITY = {
    "event_id": "0f4f644e1ca9d72ad0b7dfec2c",
    "event_type": "mobility.density",
    "schema_version": 1,
    "generated_at": "2026-09-24T09:10:10Z",
    "window_started_at": "2026-09-24T09:07:10Z",
    "window_ended_at": "2026-09-24T09:10:10Z",
    "source_event_watermark": "2026-09-24T09:10:00Z",
    "cell_id": "841f1d7ffffffff",
    "h3_resolution": 4,
    "aircraft_count": 4,
    "airborne_count": 3,
    "on_ground_count": 1,
    "average_altitude_m": 9144.0,
    "average_ground_speed_mps": 214.2,
    "provenance": "derived",
}

DEMAND = {
    "event_id": "f0f8b07c7dcf4fdd8bf5bcf7bd",
    "event_type": "demand.region",
    "schema_version": 1,
    "generated_at": "2026-09-24T09:10:10Z",
    "window_started_at": "2026-09-24T09:07:10Z",
    "window_ended_at": "2026-09-24T09:10:10Z",
    "cell_id": "841f1d7ffffffff",
    "h3_resolution": 4,
    "aircraft_count": 4,
    "demand_units": 3.35,
    "model_version": "density-linear-v1",
    "coefficient_set": "default-v1",
    "provenance": "modelled",
}


def test_density_counts_are_consistent() -> None:
    with pytest.raises(ValidationError):
        MobilityDensityEvent.model_validate(
            {**DENSITY, "aircraft_count": 3, "airborne_count": 2, "on_ground_count": 2}
        )


def test_contracts_preserve_provenance() -> None:
    assert MobilityDensityEvent.model_validate(DENSITY).provenance == "derived"
    assert RegionalDemandEvent.model_validate(DEMAND).provenance == "modelled"


def test_mobility_event_id_is_stable() -> None:
    window = datetime(2026, 9, 24, 9, 10, tzinfo=UTC)
    assert deterministic_mobility_event_id(
        "demand.region", "841F1D7FFFFFFFF", window, "Density-Linear-V1"
    ) == deterministic_mobility_event_id(
        "Demand.Region", "841f1d7ffffffff", window, "density-linear-v1"
    )
