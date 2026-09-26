from __future__ import annotations

import time

from aether_contracts import ObjectiveWeights, SimulationEvent
from aether_simulator.engine import EngineResult, WorkloadOutcome
from aether_simulator.metrics import summarize


def test_metrics_summary_handles_ten_thousand_events_under_one_second() -> None:
    events = tuple(
        SimulationEvent(
            run_id="run-load",
            event_id=f"evt-{index}",
            sequence=index + 1,
            simulated_time_ms=index,
            event_type="execution_completed",
            payload={},
        )
        for index in range(10_000)
    )
    workloads = tuple(
        WorkloadOutcome(workload_id=f"w-{index}", status="completed")
        for index in range(10_000)
    )
    result = EngineResult(
        run_id="run-load",
        status="completed",
        stopping_reason=None,
        simulated_time_ms=10_000,
        last_committed_time_ms=10_000,
        events=events,
        workloads=workloads,
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

    started = time.perf_counter()
    summary = summarize(result, weights)

    assert summary.completion_rate == 1.0
    assert summary.latency_ms.p95 > 0
    assert time.perf_counter() - started < 1.0
