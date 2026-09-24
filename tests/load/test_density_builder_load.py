from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

from aether_contracts import AircraftPositionEvent
from aether_density_builder.aggregate import DensityAggregator
from aether_density_builder.config import DensityBuilderSettings


def generated_event(icao24: str, observed_at: datetime) -> AircraftPositionEvent:
    return AircraftPositionEvent.model_validate(
        {
            "event_id": f"event-{icao24}-{observed_at.isoformat()}",
            "event_type": "aircraft.position",
            "schema_version": 1,
            "observed_at": observed_at,
            "ingested_at": observed_at,
            "source": "opensky",
            "aircraft": {
                "icao24": icao24,
                "origin_country": "Germany",
                "position": {
                    "latitude": 50.0,
                    "longitude": 8.0,
                    "barometric_altitude_m": 10000.0,
                    "on_ground": False,
                },
            },
        }
    )


def test_ten_thousand_updates_keep_one_sample_per_aircraft() -> None:
    now = datetime(2026, 9, 24, 9, 10, tzinfo=UTC)
    started = time.perf_counter()
    aggregator = DensityAggregator(DensityBuilderSettings())
    for index in range(10_000):
        aggregator.apply(
            generated_event(
                icao24=f"{index % 1000:06x}",
                observed_at=now + timedelta(milliseconds=index),
            )
        )
    batch = aggregator.prepare(now + timedelta(seconds=10))
    assert aggregator.sample_count == 1_000
    assert sum(cell.aircraft_count for cell in batch.densities) == 1_000
    assert len(batch.densities) <= 1_000
    assert time.perf_counter() - started < 2.0
