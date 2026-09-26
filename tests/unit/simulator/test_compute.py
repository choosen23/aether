from __future__ import annotations

import pytest
from aether_contracts import CanonicalWorkload, TopologyNode
from aether_simulator.compute import CapacityUnavailableError, ComputeState

NODE_WITH_ONE_GPU = TopologyNode.model_validate(
    {
        "node_id": "node-a",
        "site_id": "site-a",
        "tier": "edge",
        "latitude": 40.0,
        "longitude": 20.0,
        "capacity_cpu_millicores": 10_000,
        "capacity_memory_mb": 16_384,
        "capacity_gpu_units": 1,
        "capacity_slots": 8,
        "operating_cost_per_hour": 1.0,
        "operating_energy_watts": 100.0,
        "provenance": "simulated",
    }
)

SMALL_NODE = TopologyNode.model_validate(
    {
        "node_id": "small",
        "site_id": "small",
        "tier": "far_edge",
        "latitude": 41.0,
        "longitude": 21.0,
        "capacity_cpu_millicores": 100,
        "capacity_memory_mb": 128,
        "capacity_gpu_units": 0,
        "capacity_slots": 1,
        "operating_cost_per_hour": 1.0,
        "operating_energy_watts": 100.0,
        "provenance": "simulated",
    }
)

GPU_WORKLOAD = CanonicalWorkload.model_validate(
    {
        "schema_version": 1,
        "workload_id": "gpu-workload-1",
        "scenario_id": "scenario-1",
        "source_trace": "google_borg",
        "source_record_id": "row-1",
        "arrival_time_ms": 0,
        "expected_duration_ms": 100,
        "deadline_ms": 300,
        "priority": 1,
        "requested_cpu_millicores": 500,
        "requested_memory_mb": 1024,
        "requested_gpu_units": 1,
        "input_data_bytes": 1,
        "output_data_bytes": 1,
        "movable_state_bytes": 1,
        "preemptible": False,
        "restartable": True,
        "migration_allowed": True,
        "field_provenance": {
            "requested_cpu_millicores": "trace_derived",
            "requested_memory_mb": "trace_derived",
            "requested_gpu_units": "trace_derived",
        },
        "source_trace_version": "fixture-v1",
        "adapter_version": "adapter-v1",
    }
)

SECOND_GPU_WORKLOAD = GPU_WORKLOAD.model_copy(update={"workload_id": "gpu-workload-2"})

OVERSIZED_WORKLOAD = CanonicalWorkload.model_validate(
    {
        "schema_version": 1,
        "workload_id": "oversized-1",
        "scenario_id": "scenario-1",
        "source_trace": "azure_functions",
        "source_record_id": "row-2",
        "arrival_time_ms": 0,
        "expected_duration_ms": 100,
        "deadline_ms": 300,
        "priority": 1,
        "requested_cpu_millicores": 200,
        "requested_memory_mb": 512,
        "requested_gpu_units": 0,
        "input_data_bytes": 1,
        "output_data_bytes": 1,
        "movable_state_bytes": 1,
        "preemptible": True,
        "restartable": True,
        "migration_allowed": False,
        "field_provenance": {
            "requested_cpu_millicores": "modelled",
            "requested_memory_mb": "trace_derived",
        },
        "source_trace_version": "fixture-v1",
        "adapter_version": "adapter-v1",
    }
)


def test_failed_reservation_changes_no_resource() -> None:
    state = ComputeState.from_nodes([NODE_WITH_ONE_GPU])
    state.reserve("node-a", GPU_WORKLOAD)
    before = state.node("node-a")

    with pytest.raises(CapacityUnavailableError):
        state.reserve("node-a", SECOND_GPU_WORKLOAD)

    assert state.node("node-a") == before


def test_oversized_workload_is_unschedulable() -> None:
    state = ComputeState.from_nodes([SMALL_NODE])
    result = state.classify(OVERSIZED_WORKLOAD, eligible_node_ids=("small",))
    assert result == "unschedulable"


def test_complete_releases_reserved_resources() -> None:
    state = ComputeState.from_nodes([NODE_WITH_ONE_GPU])
    state.arrive(GPU_WORKLOAD)
    state.reserve("node-a", GPU_WORKLOAD)
    state.place(GPU_WORKLOAD.workload_id, "node-a")
    state.enqueue(GPU_WORKLOAD)
    state.start(GPU_WORKLOAD.workload_id, "node-a")

    assert state.node("node-a").used.gpu_units == 1
    state.complete(GPU_WORKLOAD.workload_id)
    assert state.node("node-a").used.gpu_units == 0
