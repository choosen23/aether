from __future__ import annotations

from aether_contracts import TopologyDocument
from aether_simulator.network import FlowCompletion, NetworkState
from aether_simulator.topology import TopologyGraph


def test_two_flows_share_the_bottleneck_equally() -> None:
    network = NetworkState(_ten_megabit_graph())
    network.start_flow("f1", "a", "b", byte_count=625_000, at_ms=0)
    network.start_flow("f2", "a", "b", byte_count=625_000, at_ms=0)
    assert network.advance(1_000) == (
        FlowCompletion("f1", 1_000),
        FlowCompletion("f2", 1_000),
    )


def test_failure_at_completion_timestamp_is_applied_first() -> None:
    network = NetworkState(_redundant_graph())
    network.start_flow("f1", "a", "c", byte_count=125_000, at_ms=0)
    network.fail_link("direct", at_ms=100)
    network.advance(100)
    assert network.flow("f1").path.link_ids == ("ab", "bc")


def _ten_megabit_graph() -> TopologyGraph:
    return TopologyGraph(
        TopologyDocument.model_validate(
            {
                "schema_version": 1,
                "topology_id": "ten-megabit",
                "generated_at": "2026-09-25T00:00:00Z",
                "nodes": [
                    _node("a"),
                    _node("b"),
                ],
                "links": [
                    _link("ab", "a", "b", 10_000_000),
                ],
            }
        )
    )


def _redundant_graph() -> TopologyGraph:
    return TopologyGraph(
        TopologyDocument.model_validate(
            {
                "schema_version": 1,
                "topology_id": "redundant",
                "generated_at": "2026-09-25T00:00:00Z",
                "nodes": [
                    _node("a"),
                    _node("b"),
                    _node("c"),
                ],
                "links": [
                    _link("direct", "a", "c", 10_000_000),
                    _link("ab", "a", "b", 5_000_000),
                    _link("bc", "b", "c", 5_000_000),
                ],
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


def _link(link_id: str, source: str, destination: str, bandwidth_bps: float) -> dict[str, object]:
    return {
        "link_id": link_id,
        "source": source,
        "destination": destination,
        "bandwidth_bps": bandwidth_bps,
        "propagation_latency_ms": 1.0,
        "transfer_cost_per_gb": 0.0,
        "energy_joules_per_gb": 0.0,
        "provenance": "simulated",
    }
