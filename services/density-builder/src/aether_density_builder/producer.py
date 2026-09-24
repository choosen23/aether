from __future__ import annotations

from typing import Protocol

from aether_contracts import MobilityDensityRemoved, deterministic_mobility_event_id

from aether_density_builder.aggregate import AggregateBatch, DensityAggregator
from aether_density_builder.config import DensityBuilderSettings
from aether_density_builder.demand import build_demand
from aether_density_builder.metrics import records_published_total


class ProducerProtocol(Protocol):
    async def send_and_wait(self, topic: str, value: bytes, key: bytes) -> object: ...


async def publish_batch(
    producer: ProducerProtocol,
    aggregator: DensityAggregator,
    batch: AggregateBatch,
    settings: DensityBuilderSettings,
) -> None:
    for density in batch.densities:
        cell_key = density.cell_id.encode("utf-8")
        await producer.send_and_wait(
            settings.redpanda_mobility_density_topic,
            density.model_dump_json().encode("utf-8"),
            key=cell_key,
        )
        records_published_total.labels(topic=settings.redpanda_mobility_density_topic).inc()

        demand = build_demand(density, settings)
        await producer.send_and_wait(
            settings.redpanda_demand_region_topic,
            demand.model_dump_json().encode("utf-8"),
            key=cell_key,
        )
        records_published_total.labels(topic=settings.redpanda_demand_region_topic).inc()

    for cell_id in batch.removals:
        last_window = aggregator.last_window_for_cell(cell_id)
        if last_window is None:
            continue
        removed = MobilityDensityRemoved(
            event_id=deterministic_mobility_event_id(
                "mobility.density.removed",
                cell_id,
                last_window,
                "derived-v1",
            ),
            removed_at=last_window,
            cell_id=cell_id,
            h3_resolution=settings.h3_resolution,
            last_window_ended_at=last_window,
        )
        await producer.send_and_wait(
            settings.redpanda_mobility_density_topic,
            removed.model_dump_json().encode("utf-8"),
            key=cell_id.encode("utf-8"),
        )
        records_published_total.labels(topic=settings.redpanda_mobility_density_topic).inc()
