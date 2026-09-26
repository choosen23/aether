from __future__ import annotations

import os

from pydantic import BaseModel


class StateBuilderSettings(BaseModel):
    redis_url: str = "redis://localhost:6379/0"
    redis_namespace: str = "aether"
    source_name: str = "opensky"
    redpanda_brokers: str = "localhost:19092"
    redpanda_topic: str = "aircraft.position.v1"
    redpanda_simulation_topic: str = "simulation.event.v1"
    redpanda_mobility_density_topic: str = "mobility.density.v1"
    redpanda_demand_region_topic: str = "demand.region.v1"
    consumer_group: str = "aether-state-builder-v1"
    mobility_consumer_group: str = "aether-mobility-state-builder-v1"
    simulation_consumer_group: str = "aether-simulation-state-builder-v1"
    metrics_port: int = 9102

    @classmethod
    def from_env(cls) -> StateBuilderSettings:
        return cls(
            redis_url=os.getenv("REDIS_URL", cls.model_fields["redis_url"].default),
            redis_namespace=os.getenv(
                "REDIS_NAMESPACE", cls.model_fields["redis_namespace"].default
            ),
            source_name=os.getenv("SOURCE_NAME", cls.model_fields["source_name"].default),
            redpanda_brokers=os.getenv(
                "REDPANDA_BROKERS", cls.model_fields["redpanda_brokers"].default
            ),
            redpanda_topic=os.getenv(
                "REDPANDA_AIRCRAFT_TOPIC", cls.model_fields["redpanda_topic"].default
            ),
            redpanda_simulation_topic=os.getenv(
                "REDPANDA_SIMULATION_TOPIC",
                cls.model_fields["redpanda_simulation_topic"].default,
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
                "STATE_BUILDER_GROUP", cls.model_fields["consumer_group"].default
            ),
            mobility_consumer_group=os.getenv(
                "MOBILITY_STATE_BUILDER_GROUP",
                cls.model_fields["mobility_consumer_group"].default,
            ),
            simulation_consumer_group=os.getenv(
                "SIMULATION_STATE_BUILDER_GROUP",
                cls.model_fields["simulation_consumer_group"].default,
            ),
            metrics_port=int(
                os.getenv(
                    "STATE_BUILDER_METRICS_PORT",
                    cls.model_fields["metrics_port"].default,
                )
            ),
        )
