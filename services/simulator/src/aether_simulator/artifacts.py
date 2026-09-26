from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from aether_simulator.checksums import sha256_file


class CompletedRunImmutableError(RuntimeError):
    pass


@dataclass(frozen=True)
class RunBundle:
    run_config: dict[str, object]
    trace_manifest: dict[str, object]
    topology_manifest: dict[str, object]
    events: tuple[dict[str, object], ...]
    metrics_rows: tuple[dict[str, object], ...]
    summary: dict[str, object]
    provenance: dict[str, object]
    completion: dict[str, object]


def write_run_bundle(run_dir: Path, bundle: RunBundle) -> Path:
    if (run_dir / "COMPLETED").exists():
        msg = f"completed run bundle is immutable: {run_dir.name}"
        raise CompletedRunImmutableError(msg)

    tmp_dir = run_dir.parent / f".{run_dir.name}.tmp"
    if tmp_dir.exists():
        _remove_tree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=False)

    _write_json(tmp_dir / "run-config.json", bundle.run_config)
    _write_json(tmp_dir / "trace-manifest.json", bundle.trace_manifest)
    _write_json(tmp_dir / "topology-manifest.json", bundle.topology_manifest)
    _write_jsonl(tmp_dir / "events.jsonl", bundle.events)
    _write_metrics(tmp_dir / "metrics.parquet", bundle.metrics_rows)
    _write_json(tmp_dir / "summary.json", bundle.summary)
    _write_json(tmp_dir / "provenance.json", bundle.provenance)
    _write_json(tmp_dir / "completion.json", bundle.completion)

    checksums = {
        file.name: sha256_file(file)
        for file in sorted(tmp_dir.iterdir())
        if file.is_file() and file.name != "checksums.json"
    }
    _write_json(tmp_dir / "checksums.json", checksums)

    if run_dir.exists():
        _remove_tree(run_dir)
    tmp_dir.replace(run_dir)

    marker = run_dir / "COMPLETED"
    marker.write_text("immutable\n", encoding="utf-8")
    os.chmod(marker, 0o444)
    return run_dir


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")), encoding="utf-8")


def _write_jsonl(path: Path, rows: tuple[dict[str, object], ...]) -> None:
    lines = [json.dumps(row, sort_keys=True, separators=(",", ":")) for row in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _write_metrics(path: Path, rows: tuple[dict[str, object], ...]) -> None:
    table = pa.Table.from_pylist(list(rows))
    pq.write_table(table, path, compression="zstd")


def _remove_tree(path: Path) -> None:
    for child in sorted(path.glob("**/*"), reverse=True):
        if child.is_file() or child.is_symlink():
            child.unlink()
        elif child.is_dir():
            child.rmdir()
    path.rmdir()
