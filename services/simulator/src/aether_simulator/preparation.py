from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Protocol

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from aether_contracts import CanonicalWorkload
from pydantic import BaseModel, ConfigDict, Field

from aether_simulator.checksums import sha256_file
from aether_simulator.manifest import PreparationManifest


class DuplicateWorkloadIdError(ValueError):
    pass


class InvalidRowError(ValueError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class AdapterContext:
    scenario_id: str
    source_trace: str
    source_trace_version: str
    adapter_version: str
    seed: int


class TraceAdapter(Protocol):
    name: str
    version: str

    def validate_schema(self, columns: frozenset[str]) -> None: ...

    def records(self, source: Path, partition: str) -> list[Mapping[str, object]]: ...

    def normalize(
        self, record: Mapping[str, object], context: AdapterContext
    ) -> CanonicalWorkload: ...


class PreparationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: Path
    output_dir: Path
    expected_source_sha256: str | None = None
    scenario_id: str
    source_trace: str
    source_trace_version: str
    partition: str
    window_start: datetime
    window_end: datetime
    scale_factor: float = Field(gt=0, le=1)
    seed: int

    def with_output(self, path: Path) -> PreparationConfig:
        return self.model_copy(update={"output_dir": path})


@dataclass(frozen=True)
class PreparationResult:
    artifact_sha256: str
    manifest: PreparationManifest
    artifact_path: Path
    manifest_path: Path


def selected(source_record_id: str, scale_factor: float, seed: int) -> bool:
    digest = sha256(f"{seed}:{source_record_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64 < scale_factor


def prepare_trace(config: PreparationConfig, adapter: TraceAdapter) -> PreparationResult:
    config.output_dir.mkdir(parents=True, exist_ok=True)

    source_sha = sha256_file(config.source)
    if config.expected_source_sha256 is not None and source_sha != config.expected_source_sha256:
        msg = f"source checksum mismatch for {config.source}"
        raise ValueError(msg)

    all_records = adapter.records(config.source, config.partition)
    if not all_records:
        msg = "source trace has no rows"
        raise ValueError(msg)

    adapter.validate_schema(frozenset(all_records[0].keys()))

    context = AdapterContext(
        scenario_id=config.scenario_id,
        source_trace=config.source_trace,
        source_trace_version=config.source_trace_version,
        adapter_version=adapter.version,
        seed=config.seed,
    )

    accepted: list[CanonicalWorkload] = []
    rejected_rows: dict[str, int] = {}
    seen_ids: set[str] = set()

    for row in all_records:
        source_record_id = str(row.get("source_record_id", ""))
        if not source_record_id or not selected(source_record_id, config.scale_factor, config.seed):
            continue
        try:
            workload = adapter.normalize(row, context)
        except InvalidRowError as exc:
            rejected_rows[exc.reason] = rejected_rows.get(exc.reason, 0) + 1
            continue

        if workload.workload_id in seen_ids:
            msg = f"duplicate workload_id detected: {workload.workload_id}"
            raise DuplicateWorkloadIdError(msg)
        seen_ids.add(workload.workload_id)
        accepted.append(workload)

    accepted.sort(key=lambda item: (item.arrival_time_ms, item.workload_id))

    workload_schema = pa.schema(
        [
            pa.field("schema_version", pa.int64()),
            pa.field("workload_id", pa.string()),
            pa.field("scenario_id", pa.string()),
            pa.field("source_trace", pa.string()),
            pa.field("source_record_id", pa.string()),
            pa.field("arrival_time_ms", pa.int64()),
            pa.field("expected_duration_ms", pa.int64()),
            pa.field("deadline_ms", pa.int64()),
            pa.field("priority", pa.int64()),
            pa.field("requested_cpu_millicores", pa.int64()),
            pa.field("requested_memory_mb", pa.int64()),
            pa.field("requested_gpu_units", pa.int64()),
            pa.field("input_data_bytes", pa.int64()),
            pa.field("output_data_bytes", pa.int64()),
            pa.field("movable_state_bytes", pa.int64()),
            pa.field("preemptible", pa.bool_()),
            pa.field("restartable", pa.bool_()),
            pa.field("migration_allowed", pa.bool_()),
            pa.field("origin_h3_cell", pa.string()),
            pa.field("origin_latitude", pa.float64()),
            pa.field("origin_longitude", pa.float64()),
            pa.field("field_provenance", pa.string()),
            pa.field("source_trace_version", pa.string()),
            pa.field("adapter_version", pa.string()),
        ]
    )

    rows: list[dict[str, object]] = []
    for workload in accepted:
        item = workload.model_dump(mode="json")
        item["field_provenance"] = json.dumps(
            item["field_provenance"],
            sort_keys=True,
            separators=(",", ":"),
        )
        rows.append(item)

    table = pa.Table.from_pylist(rows, schema=workload_schema)

    parquet_target = config.output_dir / "workloads.parquet"
    parquet_tmp = config.output_dir / "workloads.parquet.tmp"
    pq.write_table(table, parquet_tmp, compression="zstd", version="2.6", row_group_size=10_000)
    _fsync_file(parquet_tmp)
    parquet_tmp.replace(parquet_target)

    artifact_sha = sha256_file(parquet_target)
    manifest = PreparationManifest(
        adapter_name=adapter.name,
        adapter_version=adapter.version,
        source_path=str(config.source),
        source_sha256=source_sha,
        output_sha256=artifact_sha,
        partition=config.partition,
        start_iso=_as_iso(config.window_start),
        end_iso=_as_iso(config.window_end),
        scale_factor=config.scale_factor,
        seed=config.seed,
        accepted_count=len(accepted),
        rejected_count=sum(rejected_rows.values()),
        rejected_rows=rejected_rows,
    )

    manifest_target = config.output_dir / "manifest.json"
    manifest_tmp = config.output_dir / "manifest.json.tmp"
    manifest_bytes = json.dumps(
        manifest.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    manifest_tmp.write_bytes(manifest_bytes)
    _fsync_file(manifest_tmp)
    manifest_tmp.replace(manifest_target)

    return PreparationResult(
        artifact_sha256=artifact_sha,
        manifest=manifest,
        artifact_path=parquet_target,
        manifest_path=manifest_target,
    )


def _as_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())
