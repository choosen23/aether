from __future__ import annotations

from datetime import UTC, datetime, timedelta

from aether_contracts import AircraftPositionEvent
from aether_density_builder.aggregate import ApplyOutcome, DensityAggregator
from aether_density_builder.config import DensityBuilderSettings

NOW = datetime(2026, 9, 24, 9, 10, 0, tzinfo=UTC)


def event(icao24: str, latitude: float, longitude: float, *, at: datetime) -> AircraftPositionEvent:
    return AircraftPositionEvent.model_validate(
        {
            "event_id": f"event-{icao24}-{at.isoformat()}",
            "event_type": "aircraft.position",
            "schema_version": 1,
            "observed_at": at,
            "ingested_at": at,
            "source": "opensky",
            "aircraft": {
                "icao24": icao24,
                "origin_country": "Germany",
                "position": {
                    "latitude": latitude,
                    "longitude": longitude,
                    "barometric_altitude_m": 10668.0,
                    "on_ground": False,
                },
                "motion": {
                    "ground_speed_mps": 220.0,
                    "track_degrees": 90,
                    "vertical_rate_mps": 0,
                },
            },
        }
    )


def settings(window_seconds: int = 180) -> DensityBuilderSettings:
    return DensityBuilderSettings(aggregation_window_seconds=window_seconds)


def test_crossing_cells_moves_aircraft_exactly_once() -> None:
    aggregator = DensityAggregator(settings())
    aggregator.apply(event("abc123", 50.0, 8.0, at=NOW))
    first_batch = aggregator.prepare(NOW)
    aggregator.mark_published(first_batch)
    first = first_batch.densities
    aggregator.apply(event("abc123", 52.0, 13.0, at=NOW + timedelta(seconds=60)))
    second = aggregator.prepare(NOW + timedelta(seconds=60))
    assert sum(cell.aircraft_count for cell in second.densities) == 1
    assert second.removals == [first[0].cell_id]


def test_duplicate_and_equal_timestamp_do_not_change_output() -> None:
    aggregator = DensityAggregator(settings())
    item = event("abc123", 50.0, 8.0, at=NOW)
    assert aggregator.apply(item) is ApplyOutcome.ACCEPTED
    first = aggregator.prepare(NOW)
    aggregator.mark_published(first)
    assert aggregator.apply(item) is ApplyOutcome.DUPLICATE
    assert aggregator.prepare(NOW + timedelta(seconds=10)).is_empty


def test_expiry_emits_removal_without_new_source_events() -> None:
    aggregator = DensityAggregator(settings(window_seconds=180))
    aggregator.apply(event("abc123", 50.0, 8.0, at=NOW))
    first = aggregator.prepare(NOW)
    aggregator.mark_published(first)
    cell = first.densities[0].cell_id
    assert aggregator.prepare(NOW + timedelta(seconds=181)).removals == [cell]
