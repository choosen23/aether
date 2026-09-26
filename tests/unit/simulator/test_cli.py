from __future__ import annotations

import json
from pathlib import Path

import pytest
from aether_simulator.cli import main


def test_compare_writes_eight_runs_and_comparison_bundle(tmp_path: Path) -> None:
    output = tmp_path / "comparison"
    exit_code = main(
        [
            "compare",
            "--config",
            "configs/experiments/eu-small-acceptance-v1.yaml",
            "--azure-artifact",
            "tests/fixtures/prepared/azure",
            "--borg-artifact",
            "tests/fixtures/prepared/borg",
            "--output",
            str(output),
        ]
    )
    assert exit_code == 0

    payload = json.loads((output / "comparison.json").read_text(encoding="utf-8"))
    assert payload["run_count"] == 8
    assert (output / "comparison.md").exists()


def test_compare_rejects_policy_overrides_for_frozen_parameters(tmp_path: Path) -> None:
    config = tmp_path / "bad-config.yaml"
    config.write_text(
        "\n".join(
            [
                "topology: topologies/eu-small-v1.json",
                "mobility: tests/fixtures/simulation/mobility.jsonl",
                "failure_schedule: tests/fixtures/simulation/failure-schedule.json",
                "seed: 7",
                "scale_factor: 1.0",
                "time_acceleration: 1.0",
                "max_mobility_age_ms: 30000",
                "objective_weights:",
                "  latency: 1.0",
                "  transfer_time: 1.0",
                "  queue_delay: 1.0",
                "  deadline_risk: 1.0",
                "  migration_cost: 1.0",
                "  transfer_cost: 1.0",
                "  compute_energy: 1.0",
                "  network_energy: 1.0",
                "policies: [cloud_only, nearest_feasible, resource_first, balanced]",
                "policy_overrides:",
                "  balanced:",
                "    seed: 999",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="cannot vary topology"):
        main(
            [
                "compare",
                "--config",
                str(config),
                "--azure-artifact",
                "tests/fixtures/prepared/azure",
                "--borg-artifact",
                "tests/fixtures/prepared/borg",
                "--output",
                str(tmp_path / "out"),
            ]
        )
