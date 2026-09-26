from __future__ import annotations

from pathlib import Path

import pytest
from aether_contracts import CanonicalWorkload
from aether_simulator.mobility import (
    MobilityExpiredError,
    MobilityTimeline,
    assign_origin,
)

FIXTURE = Path("tests/fixtures/simulation/mobility.jsonl")

WORKLOAD = CanonicalWorkload.model_validate(
    {
        "schema_version": 1,
        "workload_id": "workload-001",
        "scenario_id": "scenario-1",
        "source_trace": "azure_functions",
        "source_record_id": "row-1",
        "arrival_time_ms": 0,
        "expected_duration_ms": 100,
        "deadline_ms": 200,
        "priority": 1,
        "requested_cpu_millicores": 100,
        "requested_memory_mb": 128,
        "requested_gpu_units": 0,
        "input_data_bytes": 1,
        "output_data_bytes": 1,
        "movable_state_bytes": 1,
        "preemptible": True,
        "restartable": True,
        "migration_allowed": True,
        "field_provenance": {
            "requested_cpu_millicores": "modelled",
            "requested_memory_mb": "trace_derived",
        },
        "source_trace_version": "fixture-v1",
        "adapter_version": "adapter-v1",
    }
)


def test_assignment_is_stable_and_marked_modelled() -> None:
    timeline = MobilityTimeline.from_jsonl(FIXTURE)
    distribution = timeline.distribution_at(simulated_ms=90_000, max_age_ms=30_000)

    first = assign_origin(WORKLOAD, distribution, seed=42)
    second = assign_origin(WORKLOAD, distribution, seed=42)

    assert first == second
    assert first.origin_h3_cell in {"85194ad3fffffff", "85194e67fffffff"}
    assert first.field_provenance["origin_h3_cell"] == "modelled"


def test_distribution_pauses_after_last_valid_bound() -> None:
    timeline = MobilityTimeline.from_jsonl(FIXTURE)
    with pytest.raises(MobilityExpiredError, match="last valid snapshot exceeded 30000 ms"):
        timeline.distribution_at(simulated_ms=90_001, max_age_ms=30_000)
