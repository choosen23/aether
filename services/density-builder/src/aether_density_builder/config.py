from __future__ import annotations

import os
from typing import Literal

from pydantic import BaseModel, Field


class DensityBuilderSettings(BaseModel):
    redpanda_brokers: str = "localhost:19092"
    redpanda_aircraft_topic: str = "aircraft.position.v1"
    redpanda_mobility_density_topic: str = "mobility.density.v1"
    redpanda_demand_region_topic: str = "demand.region.v1"
    consumer_group: str = "aether-density-builder-v1"
    metrics_port: int = 9103

    h3_resolution: int = Field(default=4, ge=0, le=15)
    aggregation_window_seconds: int = Field(default=180, gt=0)
    emit_interval_seconds: int = Field(default=10, gt=0)
    airborne_weight: float = Field(default=1.0, ge=0)
    on_ground_weight: float = Field(default=0.35, ge=0)
    demand_model_version: Literal["density-linear-v1"] = "density-linear-v1"
    demand_coefficient_set: Literal["default-v1"] = "default-v1"

    @classmethod
    def from_env(cls) -> DensityBuilderSettings:
        return cls(
            redpanda_brokers=os.getenv(
                "REDPANDA_BROKERS", cls.model_fields["redpanda_brokers"].default
            ),
            redpanda_aircraft_topic=os.getenv(
                "REDPANDA_AIRCRAFT_TOPIC",
                cls.model_fields["redpanda_aircraft_topic"].default,
            ),
            redpanda_mobility_density_topic=os.getenv(
                "REDPANDA_MOBILITY_DENSITY_TOPIC",
                cls.model_fields["redpanda_mobility_density_topic"].default,
            ),
            redpanda_demand_region_topic=os.getenv(
                "REDPANDA_DEMAND_REGION_TOPIC",
                cls.model_fields["redpanda_demand_region_topic"].default,
            ),
            consumer_group=os.getenv(
                "DENSITY_BUILDER_GROUP", cls.model_fields["consumer_group"].default
            ),
            metrics_port=int(
                os.getenv(
                    "DENSITY_BUILDER_METRICS_PORT",
                    cls.model_fields["metrics_port"].default,
                )
            ),
            h3_resolution=int(
                os.getenv("H3_RESOLUTION", cls.model_fields["h3_resolution"].default)
            ),
            aggregation_window_seconds=int(
                os.getenv(
                    "AGGREGATION_WINDOW_SECONDS",
                    cls.model_fields["aggregation_window_seconds"].default,
                )
            ),
            emit_interval_seconds=int(
                os.getenv(
                    "EMIT_INTERVAL_SECONDS", cls.model_fields["emit_interval_seconds"].default
                )
            ),
            airborne_weight=float(
                os.getenv("AIRBORNE_WEIGHT", cls.model_fields["airborne_weight"].default)
            ),
            on_ground_weight=float(
                os.getenv("ON_GROUND_WEIGHT", cls.model_fields["on_ground_weight"].default)
            ),
        )
