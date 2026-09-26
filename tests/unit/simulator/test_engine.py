from __future__ import annotations

import json
from pathlib import Path

from aether_contracts import CanonicalWorkload, TopologyDocument
from aether_simulator.compute import ComputeState
from aether_simulator.engine import SimulationEngine
from aether_simulator.mobility import MobilityTimeline
from aether_simulator.network import NetworkState
from aether_simulator.policies import PlacementDecision, SchedulingView
from aether_simulator.topology import TopologyGraph


class DeterministicScheduler:
    name = "deterministic"
    version = "v1"

    def decide(self, view: SchedulingView) -> PlacementDecision:
        return PlacementDecision(
            node_id=None,
            action="queue",
            explanation="unused in current engine tests",
            score_components={},
        )


def test_same_inputs_produce_equivalent_events() -> None:
    first = engine_for(seed=17).run()
    second = engine_for(seed=17).run()
    assert [event.model_dump(mode="json") for event in first.events] == [
        event.model_dump(mode="json") for event in second.events
    ]


def test_expired_mobility_pauses_at_last_committed_time() -> None:
    result = engine_with_expired_mobility().run()
    assert result.status == "paused"
    assert result.stopping_reason == "mobility_snapshot_expired"
    assert result.simulated_time_ms == result.last_committed_time_ms


def test_disconnected_workload_rejects_at_deadline() -> None:
    result = disconnected_engine().run()
    assert result.workload("w1").status == "rejected"
    assert result.workload("w1").reason == "deadline_expired_while_disconnected"


def test_failure_is_processed_before_completion_at_same_timestamp() -> None:
    schedule = json.loads(
        Path("tests/fixtures/simulation/failure-schedule.json").read_text(encoding="utf-8")
    )
    result = engine_for(seed=17, failure_events=tuple(schedule)).run()
    at_ten = [event.event_type for event in result.events if event.simulated_time_ms == 10]
    assert at_ten[:2] == ["link_failed", "execution_completed"]


def engine_for(seed: int, failure_events: tuple[dict[str, object], ...] = ()) -> SimulationEngine:
    topology = _connected_topology()
    mobility = MobilityTimeline.from_jsonl(Path("tests/fixtures/simulation/mobility.jsonl"))
    workload = _workload("w1", arrival_ms=0, duration_ms=10, deadline_ms=50)
    return SimulationEngine(
        run_id=f"run-{seed}",
        workloads=(workload,),
        mobility=mobility,
        topology=topology,
        network=NetworkState(topology),
        compute=ComputeState.from_nodes(list(topology.document.nodes)),
        scheduler=DeterministicScheduler(),
        failure_events=failure_events,
        max_mobility_age_ms=30_000,
    )


def engine_with_expired_mobility() -> SimulationEngine:
    topology = _connected_topology()
    mobility = MobilityTimeline.from_jsonl(Path("tests/fixtures/simulation/mobility.jsonl"))
    workload = _workload("w1", arrival_ms=90_001, duration_ms=10, deadline_ms=100_000)
    return SimulationEngine(
        run_id="run-expired",
        workloads=(workload,),
        mobility=mobility,
        topology=topology,
        network=NetworkState(topology),
        compute=ComputeState.from_nodes(list(topology.document.nodes)),
        scheduler=DeterministicScheduler(),
        max_mobility_age_ms=30_000,
    )


def disconnected_engine() -> SimulationEngine:
    topology = _disconnected_topology()
    mobility = MobilityTimeline.from_jsonl(Path("tests/fixtures/simulation/mobility.jsonl"))
    workload = _workload("w1", arrival_ms=0, duration_ms=10, deadline_ms=50)
    return SimulationEngine(
        run_id="run-disconnected",
        workloads=(workload,),
        mobility=mobility,
        topology=topology,
        network=NetworkState(topology),
        compute=ComputeState.from_nodes(list(topology.document.nodes)),
        scheduler=DeterministicScheduler(),
        max_mobility_age_ms=30_000,
    )


def _connected_topology() -> TopologyGraph:
    return TopologyGraph(
        TopologyDocument.model_validate(
            {
                "schema_version": 1,
                "topology_id": "connected",
                "generated_at": "2026-09-25T00:00:00Z",
                "nodes": [_node("a"), _node("b")],
                "links": [_link("direct", "a", "b")],
            }
        )
    )


def _disconnected_topology() -> TopologyGraph:
    return TopologyGraph(
        TopologyDocument.model_validate(
            {
                "schema_version": 1,
                "topology_id": "disconnected",
                "generated_at": "2026-09-25T00:00:00Z",
                "nodes": [_node("a"), _node("b")],
                "links": [],
            }
        )
    )


def _node(node_id: str) -> dict[str, object]:
    return {
        "node_id": node_id,
        "site_id": node_id,
        "tier": "edge",
        "latitude": 40.0,
        "longitude": 10.0,
        "capacity_cpu_millicores": 10_000,
        "capacity_memory_mb": 10_000,
        "capacity_gpu_units": 0,
        "capacity_slots": 10,
        "operating_cost_per_hour": 1.0,
        "operating_energy_watts": 1.0,
        "provenance": "simulated",
    }


def _link(link_id: str, source: str, destination: str) -> dict[str, object]:
    return {
        "link_id": link_id,
        "source": source,
        "destination": destination,
        "bandwidth_bps": 10_000_000.0,
        "propagation_latency_ms": 1.0,
        "transfer_cost_per_gb": 0.0,
        "energy_joules_per_gb": 0.0,
        "provenance": "simulated",
    }


def _workload(
    workload_id: str,
    arrival_ms: int,
    duration_ms: int,
    deadline_ms: int,
) -> CanonicalWorkload:
    return CanonicalWorkload.model_validate(
        {
            "schema_version": 1,
            "workload_id": workload_id,
            "scenario_id": "scenario-1",
            "source_trace": "azure_functions",
            "source_record_id": f"rec-{workload_id}",
            "arrival_time_ms": arrival_ms,
            "expected_duration_ms": duration_ms,
            "deadline_ms": deadline_ms,
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
