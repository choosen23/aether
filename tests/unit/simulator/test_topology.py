from __future__ import annotations

from collections import Counter
from pathlib import Path

from aether_contracts import TopologyDocument
from aether_simulator.topology import TopologyGraph

EU_SMALL = Path("topologies/eu-small-v1.json")

ASYMMETRIC_TOPOLOGY = TopologyDocument.model_validate(
    {
        "schema_version": 1,
        "topology_id": "asymmetric-v1",
        "generated_at": "2026-09-25T00:00:00Z",
        "nodes": [
            {
                "node_id": "a",
                "site_id": "a",
                "tier": "far_edge",
                "latitude": 40,
                "longitude": 10,
                "capacity_cpu_millicores": 100,
                "capacity_memory_mb": 100,
                "capacity_gpu_units": 0,
                "capacity_slots": 2,
                "operating_cost_per_hour": 1,
                "operating_energy_watts": 1,
                "provenance": "simulated",
            },
            {
                "node_id": "b",
                "site_id": "b",
                "tier": "edge",
                "latitude": 41,
                "longitude": 11,
                "capacity_cpu_millicores": 100,
                "capacity_memory_mb": 100,
                "capacity_gpu_units": 0,
                "capacity_slots": 2,
                "operating_cost_per_hour": 1,
                "operating_energy_watts": 1,
                "provenance": "simulated",
            },
            {
                "node_id": "c",
                "site_id": "c",
                "tier": "cloud",
                "latitude": 42,
                "longitude": 12,
                "capacity_cpu_millicores": 100,
                "capacity_memory_mb": 100,
                "capacity_gpu_units": 0,
                "capacity_slots": 2,
                "operating_cost_per_hour": 1,
                "operating_energy_watts": 1,
                "provenance": "simulated",
            },
        ],
        "links": [
            {
                "link_id": "ab",
                "source": "a",
                "destination": "b",
                "bandwidth_bps": 1000,
                "propagation_latency_ms": 1,
                "transfer_cost_per_gb": 1,
                "energy_joules_per_gb": 1,
                "provenance": "simulated",
            },
            {
                "link_id": "ac",
                "source": "a",
                "destination": "c",
                "bandwidth_bps": 1000,
                "propagation_latency_ms": 1,
                "transfer_cost_per_gb": 1,
                "energy_joules_per_gb": 1,
                "provenance": "simulated",
            },
            {
                "link_id": "bc",
                "source": "b",
                "destination": "c",
                "bandwidth_bps": 1000,
                "propagation_latency_ms": 2,
                "transfer_cost_per_gb": 1,
                "energy_joules_per_gb": 1,
                "provenance": "simulated",
            },
        ],
    }
)


def test_eu_small_has_required_tier_counts() -> None:
    topology = TopologyDocument.model_validate_json(EU_SMALL.read_text(encoding="utf-8"))
    counts = Counter(node.tier for node in topology.nodes)
    assert counts == {"far_edge": 20, "edge": 8, "cloud": 4}


def test_paths_are_directional() -> None:
    graph = TopologyGraph(ASYMMETRIC_TOPOLOGY)
    assert graph.shortest_path("a", "b", "latency").node_ids == ("a", "b")
    assert graph.shortest_path("b", "a", "latency") is None


def test_link_failure_invalidates_cached_paths() -> None:
    graph = TopologyGraph(ASYMMETRIC_TOPOLOGY)
    before = graph.shortest_path("a", "c", "latency")
    graph.fail_link("ac")
    after = graph.shortest_path("a", "c", "latency")
    assert before.link_ids != after.link_ids
