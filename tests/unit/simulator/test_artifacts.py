from __future__ import annotations

from pathlib import Path

import pytest
from aether_simulator.artifacts import (
    CompletedRunImmutableError,
    RunBundle,
    write_run_bundle,
)


def test_completed_bundle_cannot_be_overwritten(tmp_path: Path) -> None:
    bundle = _bundle()
    write_run_bundle(tmp_path / "run-1", bundle)

    with pytest.raises(CompletedRunImmutableError, match="run-1"):
        write_run_bundle(tmp_path / "run-1", bundle)


def test_bundle_contains_required_files(tmp_path: Path) -> None:
    output = write_run_bundle(tmp_path / "run-1", _bundle())
    names = {path.name for path in output.iterdir()}
    assert {
        "run-config.json",
        "trace-manifest.json",
        "topology-manifest.json",
        "events.jsonl",
        "metrics.parquet",
        "summary.json",
        "provenance.json",
        "completion.json",
        "checksums.json",
        "COMPLETED",
    }.issubset(names)


def _bundle() -> RunBundle:
    return RunBundle(
        run_config={"run_id": "run-1"},
        trace_manifest={"source": "fixture"},
        topology_manifest={"topology_id": "eu-small-v1"},
        events=({"event_id": "evt-1", "event_type": "workload_arrived"},),
        metrics_rows=({"run_id": "run-1", "combined_score": 1.0},),
        summary={"run_id": "run-1", "combined_score": 1.0},
        provenance={"simulator_version": "v1"},
        completion={"status": "completed", "last_committed_time_ms": 10},
    )
