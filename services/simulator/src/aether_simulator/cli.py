from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from aether_contracts import CanonicalWorkload, ObjectiveWeights, TopologyDocument

from aether_simulator.artifacts import RunBundle, write_run_bundle
from aether_simulator.compute import ComputeState
from aether_simulator.engine import SimulationEngine
from aether_simulator.metrics import summarize
from aether_simulator.mobility import MobilityTimeline
from aether_simulator.network import NetworkState
from aether_simulator.policies import (
    Balanced,
    CloudOnly,
    NearestFeasible,
    ResourceFirst,
    Scheduler,
)
from aether_simulator.topology import TopologyGraph


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "compare":
        _compare(
            config_path=Path(args.config),
            azure_artifact=Path(args.azure_artifact),
            borg_artifact=Path(args.borg_artifact),
            output_dir=Path(args.output),
        )
        return 0

    # Command placeholders for full lifecycle API in subsequent tasks.
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aether-sim")
    sub = parser.add_subparsers(dest="command", required=True)

    compare = sub.add_parser("compare")
    compare.add_argument("--config", required=True)
    compare.add_argument("--azure-artifact", required=True)
    compare.add_argument("--borg-artifact", required=True)
    compare.add_argument("--output", required=True)

    for name in ("prepare", "run", "pause", "resume", "cancel"):
        sub.add_parser(name)

    return parser


def _compare(
    *,
    config_path: Path,
    azure_artifact: Path,
    borg_artifact: Path,
    output_dir: Path,
) -> None:
    config = _load_config(config_path)
    _validate_compare_config(config)

    topology = TopologyDocument.model_validate_json(
        Path(config["topology"]).read_text(encoding="utf-8")
    )
    mobility = MobilityTimeline.from_jsonl(Path(config["mobility"]))
    failure_events = tuple(_load_failure_schedule(config))
    weights = ObjectiveWeights.model_validate(config["objective_weights"])

    manifests = {
        "azure_functions": json.loads(
            (azure_artifact / "manifest.json").read_text(encoding="utf-8")
        ),
        "google_borg": json.loads((borg_artifact / "manifest.json").read_text(encoding="utf-8")),
    }

    policies: dict[str, Scheduler] = {
        "cloud_only": CloudOnly(),
        "nearest_feasible": NearestFeasible(),
        "resource_first": ResourceFirst(),
        "balanced": Balanced(),
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    run_summaries: list[dict[str, Any]] = []

    for trace in ("azure_functions", "google_borg"):
        for policy_name in config["policies"]:
            scheduler = policies[policy_name]
            run_id = f"{trace}-{policy_name}"
            topology_graph = TopologyGraph(topology)
            workload = _fixture_workload(run_id=run_id, source_trace=trace)
            engine = SimulationEngine(
                run_id=run_id,
                workloads=(workload,),
                mobility=mobility,
                topology=topology_graph,
                network=NetworkState(topology_graph),
                compute=ComputeState.from_nodes(list(topology.nodes)),
                scheduler=scheduler,
                failure_events=failure_events,
                max_mobility_age_ms=int(config["max_mobility_age_ms"]),
            )
            result = engine.run()
            summary = summarize(result, weights)

            run_dir = output_dir / run_id
            bundle = RunBundle(
                run_config={
                    "run_id": run_id,
                    "source_trace": trace,
                    "scheduler_policy": policy_name,
                    "seed": config["seed"],
                    "scale_factor": config["scale_factor"],
                    "time_acceleration": config["time_acceleration"],
                },
                trace_manifest=manifests[trace],
                topology_manifest={
                    "topology_id": topology.topology_id,
                    "node_count": len(topology.nodes),
                    "link_count": len(topology.links),
                },
                events=tuple(event.model_dump(mode="json") for event in result.events),
                metrics_rows=(
                    {
                        "run_id": run_id,
                        "latency_p50_ms": summary.latency_ms.p50,
                        "latency_p95_ms": summary.latency_ms.p95,
                        "completion_rate": summary.completion_rate,
                        "rejection_rate": summary.rejection_rate,
                        "failure_rate": summary.failure_rate,
                        "combined_score": summary.combined_score,
                    },
                ),
                summary={
                    "run_id": run_id,
                    "component_metrics": {
                        "latency_p50_ms": summary.latency_ms.p50,
                        "latency_p95_ms": summary.latency_ms.p95,
                        "completion_rate": summary.completion_rate,
                        "rejection_rate": summary.rejection_rate,
                        "failure_rate": summary.failure_rate,
                    },
                    "combined_score": summary.combined_score,
                    "objective_weights": summary.objective_weights.model_dump(mode="json"),
                },
                provenance={
                    "source": trace,
                    "policy": policy_name,
                    "simulator_version": "v1",
                },
                completion={
                    "status": result.status,
                    "stopping_reason": result.stopping_reason,
                    "simulated_time_ms": result.simulated_time_ms,
                    "last_committed_time_ms": result.last_committed_time_ms,
                },
            )
            write_run_bundle(run_dir, bundle)

            run_summaries.append(
                {
                    "run_id": run_id,
                    "source_trace": trace,
                    "policy": policy_name,
                    "status": result.status,
                    "combined_score": summary.combined_score,
                }
            )

    comparison_json = {
        "scenario_count": 2,
        "policy_count": 4,
        "run_count": len(run_summaries),
        "runs": run_summaries,
    }
    (output_dir / "comparison.json").write_text(
        json.dumps(comparison_json, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "comparison.md").write_text(_comparison_markdown(run_summaries), encoding="utf-8")


def _load_config(path: Path) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        msg = "compare config must be a mapping"
        raise ValueError(msg)
    return loaded


def _validate_compare_config(config: dict[str, Any]) -> None:
    required = {
        "topology",
        "mobility",
        "failure_schedule",
        "seed",
        "scale_factor",
        "time_acceleration",
        "objective_weights",
        "policies",
        "max_mobility_age_ms",
    }
    missing = required - set(config)
    if missing:
        msg = f"missing compare config fields: {sorted(missing)}"
        raise ValueError(msg)

    policies = config["policies"]
    if policies != ["cloud_only", "nearest_feasible", "resource_first", "balanced"]:
        msg = "compare requires exactly four baseline policies in canonical order"
        raise ValueError(msg)

    overrides = config.get("policy_overrides", {})
    blocked_keys = {
        "topology",
        "mobility",
        "failure_schedule",
        "capacity_profile",
        "scale_factor",
        "time_acceleration",
        "seed",
    }
    if isinstance(overrides, dict):
        for policy_name, override in overrides.items():
            if not isinstance(override, dict):
                continue
            illegal = blocked_keys & set(override)
            if illegal:
                msg = (
                    "compare config cannot vary topology, mobility, failure schedule, "
                    "capacity profile, "
                    "scale, acceleration, or seed between policies"
                )
                raise ValueError(f"{msg} (policy={policy_name}, keys={sorted(illegal)})")


def _load_failure_schedule(config: dict[str, Any]) -> list[dict[str, object]]:
    content = json.loads(Path(config["failure_schedule"]).read_text(encoding="utf-8"))
    if not isinstance(content, list):
        msg = "failure_schedule must contain a JSON array"
        raise ValueError(msg)
    rows: list[dict[str, object]] = []
    for item in content:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _fixture_workload(run_id: str, source_trace: str) -> CanonicalWorkload:
    return CanonicalWorkload.model_validate(
        {
            "schema_version": 1,
            "workload_id": f"workload-{run_id}",
            "scenario_id": run_id,
            "source_trace": source_trace,
            "source_record_id": f"record-{run_id}",
            "arrival_time_ms": 0,
            "expected_duration_ms": 10,
            "deadline_ms": 60,
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


def _comparison_markdown(rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Simulation Comparison",
        "",
        "| Run ID | Trace | Policy | Status | Combined Score |",
        "|---|---|---|---|---:|",
    ]
    for row in rows:
        score = row["combined_score"]
        score_cell = "" if score is None else f"{float(score):.6f}"
        lines.append(
            f"| {row['run_id']} | {row['source_trace']} | {row['policy']} | "
            f"{row['status']} | {score_cell} |"
        )
    lines.append("")
    return "\n".join(lines)
