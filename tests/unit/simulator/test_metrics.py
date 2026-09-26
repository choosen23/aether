from __future__ import annotations

from aether_contracts import ObjectiveWeights, SimulationEvent
from aether_simulator.engine import EngineResult, WorkloadOutcome
from aether_simulator.metrics import summarize


def test_summary_reports_components_with_combined_score() -> None:
    result = EngineResult(
        run_id="run-1",
        status="completed",
        stopping_reason=None,
        simulated_time_ms=100,
        last_committed_time_ms=100,
        events=(
            SimulationEvent(
                run_id="run-1",
                event_id="evt-1",
                sequence=1,
                simulated_time_ms=10,
                event_type="execution_completed",
                payload={},
            ),
            SimulationEvent(
                run_id="run-1",
                event_id="evt-2",
                sequence=2,
                simulated_time_ms=95,
                event_type="execution_completed",
                payload={},
            ),
        ),
        workloads=(
            WorkloadOutcome(workload_id="w1", status="completed"),
            WorkloadOutcome(workload_id="w2", status="completed"),
        ),
    )
    weights = ObjectiveWeights.model_validate(
        {
            "latency": 1.0,
            "transfer_time": 1.0,
            "queue_delay": 1.0,
            "deadline_risk": 1.0,
            "migration_cost": 1.0,
            "transfer_cost": 1.0,
            "compute_energy": 1.0,
            "network_energy": 1.0,
        }
    )

    summary = summarize(result, weights)
    assert summary.latency_ms.p50 == 10
    assert summary.latency_ms.p95 == 95
    assert summary.combined_score is not None
    assert summary.objective_weights == weights
