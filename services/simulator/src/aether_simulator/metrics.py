from __future__ import annotations

from dataclasses import dataclass

from aether_contracts import ObjectiveWeights

from aether_simulator.engine import EngineResult


@dataclass(frozen=True)
class PercentileSeries:
    p50: int
    p95: int


@dataclass(frozen=True)
class MetricsSummary:
    latency_ms: PercentileSeries
    completion_rate: float
    rejection_rate: float
    failure_rate: float
    combined_score: float | None
    objective_weights: ObjectiveWeights


def summarize(result: EngineResult, weights: ObjectiveWeights) -> MetricsSummary:
    latencies = sorted(
        event.simulated_time_ms
        for event in result.events
        if event.event_type == "execution_completed"
    )
    p50 = _nearest_rank(latencies, 50)
    p95 = _nearest_rank(latencies, 95)

    total = len(result.workloads)
    completed = sum(1 for workload in result.workloads if workload.status == "completed")
    rejected = sum(1 for workload in result.workloads if workload.status == "rejected")
    failed = sum(1 for workload in result.workloads if workload.status == "failed")

    completion_rate = completed / total if total else 0.0
    rejection_rate = rejected / total if total else 0.0
    failure_rate = failed / total if total else 0.0

    score = _combined_score(p50, p95, completion_rate, rejection_rate, failure_rate, weights)

    return MetricsSummary(
        latency_ms=PercentileSeries(p50=p50, p95=p95),
        completion_rate=completion_rate,
        rejection_rate=rejection_rate,
        failure_rate=failure_rate,
        combined_score=score,
        objective_weights=weights,
    )


def _nearest_rank(values: list[int], percentile: int) -> int:
    if not values:
        return 0
    rank = max(1, (len(values) * percentile + 99) // 100)
    return values[min(rank - 1, len(values) - 1)]


def _combined_score(
    p50: int,
    p95: int,
    completion_rate: float,
    rejection_rate: float,
    failure_rate: float,
    weights: ObjectiveWeights,
) -> float:
    return (
        weights.latency * float(p50 + p95)
        + weights.transfer_time * 0.0
        + weights.queue_delay * 0.0
        + weights.deadline_risk * rejection_rate
        + weights.migration_cost * 0.0
        + weights.transfer_cost * 0.0
        + weights.compute_energy * 0.0
        + weights.network_energy * failure_rate
        - completion_rate
    )
