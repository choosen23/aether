from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_contracts import CanonicalWorkload, RunConfiguration, TopologyDocument
from pydantic import ValidationError

WORKLOAD = {
    "schema_version": 1,
    "workload_id": "workload-001",
    "scenario_id": "scenario-1",
    "source_trace": "azure_functions",
    "source_record_id": "row-1",
    "arrival_time_ms": 0,
    "expected_duration_ms": 1_000,
    "deadline_ms": 2_000,
    "priority": 1,
    "requested_cpu_millicores": 250,
    "requested_memory_mb": 512,
    "requested_gpu_units": 0,
    "input_data_bytes": 10,
    "output_data_bytes": 20,
    "movable_state_bytes": 30,
    "preemptible": True,
    "restartable": True,
    "migration_allowed": True,
    "field_provenance": {
        "requested_memory_mb": "trace_derived",
        "requested_cpu_millicores": "modelled",
    },
    "source_trace_version": "azure-2026-09",
    "adapter_version": "azure-v1",
}

TOPOLOGY = {
    "schema_version": 1,
    "topology_id": "eu-small-v1",
    "generated_at": "2026-09-25T00:00:00Z",
    "nodes": [
        {
            "node_id": "node-a",
            "site_id": "site-a",
            "tier": "far_edge",
            "latitude": 40.0,
            "longitude": 20.0,
            "capacity_cpu_millicores": 1000,
            "capacity_memory_mb": 2048,
            "capacity_gpu_units": 0,
            "capacity_slots": 4,
            "operating_cost_per_hour": 1.2,
            "operating_energy_watts": 95.0,
            "provenance": "simulated",
        },
        {
            "node_id": "node-b",
            "site_id": "site-b",
            "tier": "edge",
            "latitude": 41.0,
            "longitude": 21.0,
            "capacity_cpu_millicores": 2000,
            "capacity_memory_mb": 4096,
            "capacity_gpu_units": 1,
            "capacity_slots": 8,
            "operating_cost_per_hour": 2.0,
            "operating_energy_watts": 140.0,
            "provenance": "simulated",
        },
    ],
    "links": [
        {
            "link_id": "ab",
            "source": "node-a",
            "destination": "node-b",
            "bandwidth_bps": 1_000_000,
            "propagation_latency_ms": 4,
            "transfer_cost_per_gb": 0.2,
            "energy_joules_per_gb": 5,
            "provenance": "simulated",
        }
    ],
}

RUN = {
    "schema_version": 1,
    "run_id": "run-1",
    "scenario_id": "scenario-1",
    "source_traces": ["azure_functions"],
    "scheduler_policy": "balanced",
    "topology_id": "eu-small-v1",
    "seed": 7,
    "scale_factor": 0.4,
    "time_acceleration": 1,
    "started_at": datetime(2026, 9, 25, 0, 0, tzinfo=UTC),
}


def test_canonical_workload_rejects_negative_resources() -> None:
    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        CanonicalWorkload.model_validate({**WORKLOAD, "requested_memory_mb": -1})


def test_topology_rejects_dangling_link() -> None:
    with pytest.raises(ValidationError, match="unknown destination node missing"):
        TopologyDocument.model_validate(
            {
                **TOPOLOGY,
                "links": [
                    {
                        **TOPOLOGY["links"][0],
                        "destination": "missing",
                    }
                ],
            }
        )


def test_run_configuration_rejects_mixed_trace_sources() -> None:
    with pytest.raises(ValidationError, match="exactly one source trace"):
        RunConfiguration.model_validate(
            {
                **RUN,
                "source_traces": ["azure_functions", "google_borg"],
            }
        )
