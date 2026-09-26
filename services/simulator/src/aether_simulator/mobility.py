from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

import h3  # type: ignore[import-untyped]
from aether_contracts import CanonicalWorkload, MobilityDensityEvent


class MobilityExpiredError(RuntimeError):
    pass


@dataclass(frozen=True)
class MobilityDistribution:
    window_ended_at_ms: int
    weights: tuple[tuple[str, int], ...]


class MobilityTimeline:
    def __init__(self, distributions: tuple[MobilityDistribution, ...]) -> None:
        if not distributions:
            msg = "mobility timeline requires at least one distribution"
            raise ValueError(msg)
        self._distributions = tuple(sorted(distributions, key=lambda item: item.window_ended_at_ms))

    @classmethod
    def from_jsonl(cls, path: Path) -> MobilityTimeline:
        snapshots_abs: dict[int, dict[str, int]] = {}

        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            event = MobilityDensityEvent.model_validate_json(text)
            window_ended_at_ms = int(event.window_ended_at.timestamp() * 1000)
            bucket = snapshots_abs.setdefault(window_ended_at_ms, {})
            bucket[event.cell_id] = bucket.get(event.cell_id, 0) + event.aircraft_count

        if not snapshots_abs:
            msg = "mobility timeline requires at least one valid event"
            raise ValueError(msg)

        base_window_ms = min(snapshots_abs)
        distributions: list[MobilityDistribution] = []
        for window_ms, cell_counts in snapshots_abs.items():
            weights = tuple(sorted(cell_counts.items(), key=lambda item: item[0]))
            relative_ms = window_ms - base_window_ms
            distributions.append(
                MobilityDistribution(window_ended_at_ms=relative_ms, weights=weights)
            )

        return cls(tuple(distributions))

    def distribution_at(self, simulated_ms: int, max_age_ms: int) -> MobilityDistribution:
        selected: MobilityDistribution | None = None
        for distribution in self._distributions:
            if distribution.window_ended_at_ms <= simulated_ms:
                selected = distribution
                continue
            break

        if selected is None:
            msg = "no mobility snapshot available at or before requested simulated timestamp"
            raise MobilityExpiredError(msg)

        age = simulated_ms - selected.window_ended_at_ms
        if age > max_age_ms:
            msg = f"last valid snapshot exceeded {max_age_ms} ms"
            raise MobilityExpiredError(msg)

        if not selected.weights:
            msg = "mobility distribution is empty"
            raise ValueError(msg)
        if any(weight <= 0 for _, weight in selected.weights):
            msg = "mobility distribution weights must be positive"
            raise ValueError(msg)

        return selected


def deterministic_unit_interval(seed: int, workload_id: str, window_ms: int) -> float:
    digest = sha256(f"{seed}:{workload_id}:{window_ms}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def assign_origin(
    workload: CanonicalWorkload,
    distribution: MobilityDistribution,
    seed: int,
) -> CanonicalWorkload:
    threshold = deterministic_unit_interval(
        seed,
        workload.workload_id,
        distribution.window_ended_at_ms,
    )
    total = sum(weight for _, weight in distribution.weights)

    cumulative = 0.0
    chosen_cell = distribution.weights[-1][0]
    for cell_id, weight in distribution.weights:
        cumulative += weight / total
        if threshold <= cumulative:
            chosen_cell = cell_id
            break

    latitude, longitude = h3.cell_to_latlng(chosen_cell)
    updated_provenance = dict(workload.field_provenance)
    updated_provenance["origin_h3_cell"] = "modelled"
    updated_provenance["origin_latitude"] = "modelled"
    updated_provenance["origin_longitude"] = "modelled"

    return workload.model_copy(
        update={
            "origin_h3_cell": chosen_cell,
            "origin_latitude": latitude,
            "origin_longitude": longitude,
            "field_provenance": updated_provenance,
        }
    )
