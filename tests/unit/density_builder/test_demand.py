from __future__ import annotations

from datetime import UTC, datetime

from aether_contracts import MobilityDensityEvent
from aether_density_builder.config import DensityBuilderSettings
from aether_density_builder.demand import build_demand


def test_build_demand_uses_default_coefficients() -> None:
    density = MobilityDensityEvent(
        event_id="density-1",
        generated_at=datetime(2026, 9, 24, 9, 10, tzinfo=UTC),
        window_started_at=datetime(2026, 9, 24, 9, 7, tzinfo=UTC),
        window_ended_at=datetime(2026, 9, 24, 9, 10, tzinfo=UTC),
        source_event_watermark=datetime(2026, 9, 24, 9, 10, tzinfo=UTC),
        cell_id="841f1d7ffffffff",
        h3_resolution=4,
        aircraft_count=4,
        airborne_count=3,
        on_ground_count=1,
        average_altitude_m=1000,
        average_ground_speed_mps=200,
    )

    demand = build_demand(density, DensityBuilderSettings())

    assert demand.demand_units == 3.35
    assert demand.provenance == "modelled"
