from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_contracts import MobilityCell, MobilityDensityEvent, RegionalDemandEvent
from aether_state_builder.mobility_projection import MobilityProjection

WINDOW_1 = datetime(2026, 9, 24, 9, 10, tzinfo=UTC)
WINDOW_2 = datetime(2026, 9, 24, 9, 10, 10, tzinfo=UTC)
CELL = "841f1d7ffffffff"


def density(window: datetime) -> MobilityDensityEvent:
    return MobilityDensityEvent(
        event_id=f"density-{window.isoformat()}",
        generated_at=window,
        window_started_at=window,
        window_ended_at=window,
        source_event_watermark=window,
        cell_id=CELL,
        h3_resolution=4,
        aircraft_count=1,
        airborne_count=1,
        on_ground_count=0,
        average_altitude_m=1000,
        average_ground_speed_mps=200,
    )


def demand(window: datetime) -> RegionalDemandEvent:
    return RegionalDemandEvent(
        event_id=f"demand-{window.isoformat()}",
        generated_at=window,
        window_started_at=window,
        window_ended_at=window,
        cell_id=CELL,
        h3_resolution=4,
        aircraft_count=1,
        demand_units=1.0,
    )


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}
        self.zsets: dict[str, dict[str, int]] = {}
        self.published: list[str] = []

    async def eval(self, script: str, _numkeys: int, *values: object) -> object:
        key = str(values[0])
        index_key = str(values[1])
        self.zsets.setdefault(index_key, {})
        self.hashes.setdefault(key, {})

        if "'density_payload', ARGV[2]" in script:
            window = int(values[3])
            density_payload = str(values[4])
            cell_id = str(values[5])
            current = self.hashes[key].get("density_window_epoch_ms")
            if current is not None and int(current) > window:
                return 2
            self.hashes[key]["density_window_epoch_ms"] = window
            self.hashes[key]["density_payload"] = density_payload
            demand_window = self.hashes[key].get("demand_window_epoch_ms")
            if demand_window is not None and int(demand_window) == window:
                payload = (
                    '{"density":'
                    + str(self.hashes[key]["density_payload"])
                    + ',"demand":'
                    + str(self.hashes[key]["demand_payload"])
                    + "}"
                )
                self.hashes[key]["payload"] = payload
                self.zsets[index_key][cell_id] = window
                self.published.append(payload)
                return 1
            return 0

        if "'demand_payload', ARGV[2]" in script:
            window = int(values[3])
            demand_payload = str(values[4])
            cell_id = str(values[5])
            current = self.hashes[key].get("demand_window_epoch_ms")
            if current is not None and int(current) > window:
                return 2
            self.hashes[key]["demand_window_epoch_ms"] = window
            self.hashes[key]["demand_payload"] = demand_payload
            density_window = self.hashes[key].get("density_window_epoch_ms")
            if density_window is not None and int(density_window) == window:
                payload = (
                    '{"density":'
                    + str(self.hashes[key]["density_payload"])
                    + ',"demand":'
                    + str(self.hashes[key]["demand_payload"])
                    + "}"
                )
                self.hashes[key]["payload"] = payload
                self.zsets[index_key][cell_id] = window
                self.published.append(payload)
                return 1
            return 0

        cell_id = str(values[3])
        payload = str(values[4])
        self.hashes.pop(key, None)
        self.zsets[index_key].pop(cell_id, None)
        self.published.append(payload)
        return 1


@pytest.mark.asyncio
async def test_projection_publishes_only_matching_windows() -> None:
    redis = FakeRedis()
    projection = MobilityProjection(redis)
    await projection.apply_demand(demand(window=WINDOW_2))
    await projection.apply_density(density(window=WINDOW_1))
    assert redis.published == []
    await projection.apply_density(density(window=WINDOW_2))
    assert MobilityCell.model_validate_json(redis.published[-1]).density.window_ended_at == WINDOW_2
