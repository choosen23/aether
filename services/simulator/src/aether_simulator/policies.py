from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class NodeView:
    node_id: str
    tier: str
    feasible: bool
    estimated_latency_ms: float
    estimated_transfer_time_ms: float
    estimated_queue_delay_ms: float
    estimated_execution_ms: float
    normalized_headroom: float
    queue_capacity_score: float
    compute_pressure: float
    deadline_risk: float
    migration_cost: float
    transfer_cost: float
    compute_energy: float
    network_energy: float


@dataclass(frozen=True)
class ObjectiveWeights:
    compute_pressure: float
    path_latency: float
    transfer_time: float
    queue_delay: float
    deadline_risk: float
    migration_cost: float
    transfer_cost: float
    energy: float


@dataclass(frozen=True)
class SchedulingView:
    workload_id: str
    nodes: tuple[NodeView, ...]
    now_ms: int
    weights: ObjectiveWeights


@dataclass(frozen=True)
class PlacementDecision:
    node_id: str | None
    action: str
    explanation: str
    score_components: dict[str, float]


class Scheduler(Protocol):
    name: str
    version: str

    def decide(self, view: SchedulingView) -> PlacementDecision: ...


class CloudOnly:
    name = "cloud_only"
    version = "v1"

    def decide(self, view: SchedulingView) -> PlacementDecision:
        candidates = [node for node in view.nodes if node.feasible and node.tier == "cloud"]
        if not candidates:
            return PlacementDecision(
                node_id=None,
                action="queue",
                explanation="no feasible cloud node",
                score_components={},
            )

        best = min(candidates, key=lambda node: (node.estimated_latency_ms, node.node_id))
        return PlacementDecision(
            node_id=best.node_id,
            action="place",
            explanation="selected feasible cloud node with lowest latency",
            score_components={"latency_ms": best.estimated_latency_ms},
        )


class NearestFeasible:
    name = "nearest_feasible"
    version = "v1"

    def decide(self, view: SchedulingView) -> PlacementDecision:
        candidates = [node for node in view.nodes if node.feasible]
        if not candidates:
            return PlacementDecision(
                node_id=None,
                action="queue",
                explanation="no feasible node",
                score_components={},
            )

        def score(node: NodeView) -> float:
            return (
                node.estimated_latency_ms
                + node.estimated_transfer_time_ms
                + node.estimated_queue_delay_ms
                + node.estimated_execution_ms
            )

        best = min(candidates, key=lambda node: (score(node), node.node_id))
        return PlacementDecision(
            node_id=best.node_id,
            action="place",
            explanation="selected feasible node with lowest end-to-end estimate",
            score_components={
                "latency_ms": best.estimated_latency_ms,
                "transfer_time_ms": best.estimated_transfer_time_ms,
                "queue_delay_ms": best.estimated_queue_delay_ms,
                "execution_ms": best.estimated_execution_ms,
            },
        )


class ResourceFirst:
    name = "resource_first"
    version = "v1"

    def decide(self, view: SchedulingView) -> PlacementDecision:
        candidates = [node for node in view.nodes if node.feasible]
        if not candidates:
            return PlacementDecision(
                node_id=None,
                action="queue",
                explanation="no feasible node",
                score_components={},
            )

        best = min(
            candidates,
            key=lambda node: (
                -node.normalized_headroom,
                -node.queue_capacity_score,
                node.estimated_latency_ms,
                node.node_id,
            ),
        )

        return PlacementDecision(
            node_id=best.node_id,
            action="place",
            explanation="selected feasible node with maximum post-placement headroom",
            score_components={
                "normalized_headroom": best.normalized_headroom,
                "queue_capacity_score": best.queue_capacity_score,
                "latency_ms": best.estimated_latency_ms,
            },
        )


class Balanced:
    name = "balanced"
    version = "v1"

    def decide(self, view: SchedulingView) -> PlacementDecision:
        candidates = [node for node in view.nodes if node.feasible]
        if not candidates:
            return PlacementDecision(
                node_id=None,
                action="queue",
                explanation="no feasible node",
                score_components={},
            )

        def normalize(values: list[float]) -> list[float]:
            low = min(values)
            high = max(values)
            if high == low:
                return [0.0 for _ in values]
            return [(value - low) / (high - low) for value in values]

        pressure_norm = normalize([node.compute_pressure for node in candidates])
        latency_norm = normalize([node.estimated_latency_ms for node in candidates])
        transfer_norm = normalize([node.estimated_transfer_time_ms for node in candidates])
        queue_norm = normalize([node.estimated_queue_delay_ms for node in candidates])
        deadline_norm = normalize([node.deadline_risk for node in candidates])
        migration_norm = normalize([node.migration_cost for node in candidates])
        transfer_cost_norm = normalize([node.transfer_cost for node in candidates])
        energy_norm = normalize([node.compute_energy + node.network_energy for node in candidates])

        def weighted_score(index: int) -> float:
            weights = view.weights
            return (
                weights.compute_pressure * pressure_norm[index]
                + weights.path_latency * latency_norm[index]
                + weights.transfer_time * transfer_norm[index]
                + weights.queue_delay * queue_norm[index]
                + weights.deadline_risk * deadline_norm[index]
                + weights.migration_cost * migration_norm[index]
                + weights.transfer_cost * transfer_cost_norm[index]
                + weights.energy * energy_norm[index]
            )

        indexed = list(enumerate(candidates))
        best_index, best = min(
            indexed,
            key=lambda item: (weighted_score(item[0]), item[1].node_id),
        )
        return PlacementDecision(
            node_id=best.node_id,
            action="place",
            explanation="selected node with lowest weighted normalized objective",
            score_components={
                "weighted_score": weighted_score(best_index),
                "compute_pressure": best.compute_pressure,
                "latency_ms": best.estimated_latency_ms,
                "transfer_time_ms": best.estimated_transfer_time_ms,
                "queue_delay_ms": best.estimated_queue_delay_ms,
                "deadline_risk": best.deadline_risk,
                "migration_cost": best.migration_cost,
                "transfer_cost": best.transfer_cost,
                "compute_energy": best.compute_energy,
                "network_energy": best.network_energy,
            },
        )
