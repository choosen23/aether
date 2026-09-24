from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

import h3  # type: ignore[import-untyped]
from aether_contracts import (
    AircraftPositionEvent,
    MobilityDensityEvent,
    deterministic_mobility_event_id,
)

from aether_density_builder.config import DensityBuilderSettings


class ApplyOutcome(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    STALE = "stale"
    COORDINATE_MISSING = "coordinate_missing"


@dataclass(frozen=True, slots=True)
class AircraftSample:
    event_id: str
    observed_at: datetime
    cell_id: str
    on_ground: bool
    altitude_m: float | None
    ground_speed_mps: float | None


@dataclass(frozen=True, slots=True)
class AggregateBatch:
    densities: list[MobilityDensityEvent]
    removals: list[str]
    expired_icao24: list[str]
    signatures: dict[str, str]
    windows: dict[str, datetime]

    @property
    def is_empty(self) -> bool:
        return not self.densities and not self.removals


class DensityAggregator:
    def __init__(self, settings: DensityBuilderSettings) -> None:
        self._settings = settings
        self._samples: dict[str, AircraftSample] = {}
        self._acked_signatures: dict[str, str] = {}
        self._acked_windows: dict[str, datetime] = {}

    @property
    def sample_count(self) -> int:
        return len(self._samples)

    def apply(self, event: AircraftPositionEvent) -> ApplyOutcome:
        if event.aircraft.position is None:
            return ApplyOutcome.COORDINATE_MISSING

        icao24 = event.aircraft.icao24
        existing = self._samples.get(icao24)
        if existing is not None:
            if event.observed_at < existing.observed_at:
                return ApplyOutcome.STALE
            if event.observed_at == existing.observed_at:
                return ApplyOutcome.DUPLICATE

        position = event.aircraft.position
        cell_id = h3.latlng_to_cell(
            position.latitude,
            position.longitude,
            self._settings.h3_resolution,
        )
        self._samples[icao24] = AircraftSample(
            event_id=event.event_id,
            observed_at=event.observed_at,
            cell_id=cell_id,
            on_ground=position.on_ground,
            altitude_m=position.barometric_altitude_m,
            ground_speed_mps=(
                event.aircraft.motion.ground_speed_mps if event.aircraft.motion else None
            ),
        )
        return ApplyOutcome.ACCEPTED

    def prepare(self, now: datetime) -> AggregateBatch:
        now = now.astimezone(UTC)
        window = timedelta(seconds=self._settings.aggregation_window_seconds)
        cutoff = now - window

        expired_icao24 = sorted(
            icao24
            for icao24, sample in self._samples.items()
            if sample.observed_at < cutoff
        )
        expired_icao24_set = set(expired_icao24)

        groups: dict[str, list[AircraftSample]] = {}
        for icao24, sample in self._samples.items():
            if icao24 in expired_icao24_set:
                continue
            groups.setdefault(sample.cell_id, []).append(sample)

        signatures: dict[str, str] = {}
        windows: dict[str, datetime] = {}
        densities: list[MobilityDensityEvent] = []

        for cell_id in sorted(groups.keys()):
            members = groups[cell_id]
            aircraft_count = len(members)
            airborne_count = sum(1 for item in members if not item.on_ground)
            on_ground_count = aircraft_count - airborne_count
            altitudes = [item.altitude_m for item in members if item.altitude_m is not None]
            speeds = [
                item.ground_speed_mps for item in members if item.ground_speed_mps is not None
            ]

            watermark = max(item.observed_at for item in members)
            signature = self._signature_for(
                members,
                airborne_count,
                on_ground_count,
                altitudes,
                speeds,
            )
            signatures[cell_id] = signature
            windows[cell_id] = now

            if self._acked_signatures.get(cell_id) == signature:
                continue

            densities.append(
                MobilityDensityEvent(
                    event_id=deterministic_mobility_event_id(
                        "mobility.density",
                        cell_id,
                        now,
                        "derived-v1",
                    ),
                    generated_at=now,
                    window_started_at=cutoff,
                    window_ended_at=now,
                    source_event_watermark=watermark,
                    cell_id=cell_id,
                    h3_resolution=self._settings.h3_resolution,
                    aircraft_count=aircraft_count,
                    airborne_count=airborne_count,
                    on_ground_count=on_ground_count,
                    average_altitude_m=(sum(altitudes) / len(altitudes)) if altitudes else None,
                    average_ground_speed_mps=(sum(speeds) / len(speeds)) if speeds else None,
                )
            )

        current_cells = set(groups.keys())
        removed_cells = sorted(cell for cell in self._acked_signatures if cell not in current_cells)

        return AggregateBatch(
            densities=densities,
            removals=removed_cells,
            expired_icao24=expired_icao24,
            signatures=signatures,
            windows=windows,
        )

    def mark_published(self, batch: AggregateBatch) -> None:
        for icao24 in batch.expired_icao24:
            self._samples.pop(icao24, None)

        self._acked_signatures = dict(batch.signatures)
        self._acked_windows = dict(batch.windows)

    def last_window_for_cell(self, cell_id: str) -> datetime | None:
        return self._acked_windows.get(cell_id)

    @staticmethod
    def _signature_for(
        members: list[AircraftSample],
        airborne_count: int,
        on_ground_count: int,
        altitudes: list[float],
        speeds: list[float],
    ) -> str:
        event_ids = ",".join(sorted(item.event_id for item in members))
        average_altitude = (sum(altitudes) / len(altitudes)) if altitudes else None
        average_speed = (sum(speeds) / len(speeds)) if speeds else None
        return "|".join(
            [
                event_ids,
                str(len(members)),
                str(airborne_count),
                str(on_ground_count),
                str(round(average_altitude, 6) if average_altitude is not None else "none"),
                str(round(average_speed, 6) if average_speed is not None else "none"),
            ]
        )
