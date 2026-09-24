from __future__ import annotations

from aether_contracts import (
    MobilityDensityEvent,
    RegionalDemandEvent,
    deterministic_mobility_event_id,
)

from aether_density_builder.config import DensityBuilderSettings


def build_demand(
    density: MobilityDensityEvent,
    settings: DensityBuilderSettings,
) -> RegionalDemandEvent:
    units = (
        density.airborne_count * settings.airborne_weight
        + density.on_ground_count * settings.on_ground_weight
    )
    return RegionalDemandEvent(
        event_id=deterministic_mobility_event_id(
            "demand.region",
            density.cell_id,
            density.window_ended_at,
            settings.demand_model_version,
        ),
        generated_at=density.generated_at,
        window_started_at=density.window_started_at,
        window_ended_at=density.window_ended_at,
        cell_id=density.cell_id,
        h3_resolution=density.h3_resolution,
        aircraft_count=density.aircraft_count,
        demand_units=units,
        model_version=settings.demand_model_version,
        coefficient_set=settings.demand_coefficient_set,
    )
