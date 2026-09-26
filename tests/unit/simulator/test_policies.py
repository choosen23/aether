from __future__ import annotations

from copy import deepcopy

import pytest
from aether_simulator.policies import (
    Balanced,
    CloudOnly,
    NearestFeasible,
    NodeView,
    ObjectiveWeights,
    ResourceFirst,
    Scheduler,
    SchedulingView,
)

VIEW = SchedulingView(
    workload_id="w1",
    now_ms=0,
    weights=ObjectiveWeights(
        compute_pressure=1.0,
        path_latency=1.0,
        transfer_time=1.0,
        queue_delay=1.0,
        deadline_risk=1.0,
        migration_cost=1.0,
        transfer_cost=1.0,
        energy=1.0,
    ),
    nodes=(
        NodeView(
            node_id="cloud-near",
            tier="cloud",
            feasible=True,
            estimated_latency_ms=8,
            estimated_transfer_time_ms=4,
            estimated_queue_delay_ms=3,
            estimated_execution_ms=6,
            normalized_headroom=0.2,
            queue_capacity_score=0.3,
            compute_pressure=0.8,
            deadline_risk=0.3,
            migration_cost=0.5,
            transfer_cost=0.6,
            compute_energy=0.7,
            network_energy=0.3,
        ),
        NodeView(
            node_id="far-edge",
            tier="far_edge",
            feasible=True,
            estimated_latency_ms=2,
            estimated_transfer_time_ms=2,
            estimated_queue_delay_ms=1,
            estimated_execution_ms=4,
            normalized_headroom=0.4,
            queue_capacity_score=0.4,
            compute_pressure=0.5,
            deadline_risk=0.2,
            migration_cost=0.4,
            transfer_cost=0.4,
            compute_energy=0.4,
            network_energy=0.2,
        ),
        NodeView(
            node_id="edge-empty",
            tier="edge",
            feasible=True,
            estimated_latency_ms=6,
            estimated_transfer_time_ms=5,
            estimated_queue_delay_ms=1,
            estimated_execution_ms=5,
            normalized_headroom=0.95,
            queue_capacity_score=0.9,
            compute_pressure=0.7,
            deadline_risk=0.4,
            migration_cost=0.3,
            transfer_cost=0.3,
            compute_energy=0.3,
            network_energy=0.2,
        ),
        NodeView(
            node_id="edge-balanced",
            tier="edge",
            feasible=True,
            estimated_latency_ms=3,
            estimated_transfer_time_ms=3,
            estimated_queue_delay_ms=2,
            estimated_execution_ms=4,
            normalized_headroom=0.7,
            queue_capacity_score=0.7,
            compute_pressure=0.2,
            deadline_risk=0.1,
            migration_cost=0.1,
            transfer_cost=0.1,
            compute_energy=0.1,
            network_energy=0.1,
        ),
    ),
)

TIED_VIEW = SchedulingView(
    workload_id="w2",
    now_ms=0,
    weights=VIEW.weights,
    nodes=(
        NodeView(
            node_id="edge-b",
            tier="edge",
            feasible=True,
            estimated_latency_ms=4,
            estimated_transfer_time_ms=1,
            estimated_queue_delay_ms=1,
            estimated_execution_ms=2,
            normalized_headroom=0.5,
            queue_capacity_score=0.5,
            compute_pressure=0.5,
            deadline_risk=0.1,
            migration_cost=0.1,
            transfer_cost=0.1,
            compute_energy=0.1,
            network_energy=0.1,
        ),
        NodeView(
            node_id="edge-a",
            tier="edge",
            feasible=True,
            estimated_latency_ms=4,
            estimated_transfer_time_ms=1,
            estimated_queue_delay_ms=1,
            estimated_execution_ms=2,
            normalized_headroom=0.5,
            queue_capacity_score=0.5,
            compute_pressure=0.5,
            deadline_risk=0.1,
            migration_cost=0.1,
            transfer_cost=0.1,
            compute_energy=0.1,
            network_energy=0.1,
        ),
    ),
)


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (CloudOnly(), "cloud-near"),
        (NearestFeasible(), "far-edge"),
        (ResourceFirst(), "edge-empty"),
        (Balanced(), "edge-balanced"),
    ],
)
def test_baseline_policy_choice(policy: Scheduler, expected: str) -> None:
    original = deepcopy(VIEW)
    assert policy.decide(VIEW).node_id == expected
    assert VIEW == original


def test_ties_are_broken_by_node_id() -> None:
    decision = NearestFeasible().decide(TIED_VIEW)
    assert decision.node_id == "edge-a"
