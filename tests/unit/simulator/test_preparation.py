from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest
from aether_contracts import CanonicalWorkload
from aether_simulator.preparation import (
    AdapterContext,
    DuplicateWorkloadIdError,
    InvalidRowError,
    PreparationConfig,
    TraceAdapter,
    prepare_trace,
)


class FakeAdapter(TraceAdapter):
    name = "fake"
    version = "fake-v1"

    def validate_schema(self, columns: frozenset[str]) -> None:
        required = {"source_record_id", "arrival_time_ms", "expected_duration_ms", "memory_mb"}
        missing = required - columns
        if missing:
            msg = f"missing columns: {sorted(missing)}"
            raise ValueError(msg)

    def records(self, source: Path, partition: str) -> list[Mapping[str, object]]:
        return [
            {
                "source_record_id": "row-2",
                "arrival_time_ms": 2,
                "expected_duration_ms": 100,
                "memory_mb": 512,
            },
            {
                "source_record_id": "row-1",
                "arrival_time_ms": 1,
                "expected_duration_ms": 200,
                "memory_mb": 256,
            },
        ]

    def normalize(self, record: Mapping[str, object], context: AdapterContext) -> CanonicalWorkload:
        source_record_id = str(record["source_record_id"])
        return _workload(
            workload_id=f"workload-{source_record_id}",
            source_record_id=source_record_id,
            arrival_time_ms=int(record["arrival_time_ms"]),
            expected_duration_ms=int(record["expected_duration_ms"]),
            memory_mb=int(record["memory_mb"]),
            context=context,
        )


class DuplicateAdapter(FakeAdapter):
    def normalize(self, record: Mapping[str, object], context: AdapterContext) -> CanonicalWorkload:
        base = super().normalize(record, context)
        return base.model_copy(update={"workload_id": "workload-001"})


class OneMalformedAdapter(FakeAdapter):
    def records(self, source: Path, partition: str) -> list[Mapping[str, object]]:
        rows = super().records(source, partition)
        rows.append(
            {
                "source_record_id": "bad-1",
                "arrival_time_ms": 3,
                "memory_mb": 1024,
            }
        )
        return rows

    def normalize(self, record: Mapping[str, object], context: AdapterContext) -> CanonicalWorkload:
        if "expected_duration_ms" not in record:
            raise InvalidRowError("missing_duration")
        return super().normalize(record, context)


def test_preparation_is_byte_identical(tmp_path: Path) -> None:
    config = _config(tmp_path)
    first = prepare_trace(config.with_output(tmp_path / "a"), FakeAdapter())
    second = prepare_trace(config.with_output(tmp_path / "b"), FakeAdapter())
    assert first.artifact_sha256 == second.artifact_sha256
    assert (tmp_path / "a/workloads.parquet").read_bytes() == (
        tmp_path / "b/workloads.parquet"
    ).read_bytes()


def test_preparation_rejects_duplicate_workload_ids(tmp_path: Path) -> None:
    with pytest.raises(DuplicateWorkloadIdError, match="workload-001"):
        prepare_trace(_config(tmp_path).with_output(tmp_path / "dup"), DuplicateAdapter())


def test_malformed_rows_are_reported_by_reason(tmp_path: Path) -> None:
    result = prepare_trace(_config(tmp_path).with_output(tmp_path / "rej"), OneMalformedAdapter())
    assert result.manifest.rejected_rows == {"missing_duration": 1}


def _config(tmp_path: Path) -> PreparationConfig:
    source = tmp_path / "source.csv"
    source.write_text("source_record_id,arrival_time_ms,expected_duration_ms,memory_mb\n")
    return PreparationConfig(
        source=source,
        output_dir=tmp_path / "out",
        expected_source_sha256=None,
        scenario_id="scenario-1",
        source_trace="azure_functions",
        source_trace_version="fixture-v1",
        partition="fixture",
        window_start=datetime(2026, 9, 25, 0, 0, tzinfo=UTC),
        window_end=datetime(2026, 9, 25, 1, 0, tzinfo=UTC),
        scale_factor=1.0,
        seed=42,
    )


def _workload(
    *,
    workload_id: str,
    source_record_id: str,
    arrival_time_ms: int,
    expected_duration_ms: int,
    memory_mb: int,
    context: AdapterContext,
) -> CanonicalWorkload:
    return CanonicalWorkload.model_validate(
        {
            "schema_version": 1,
            "workload_id": workload_id,
            "scenario_id": context.scenario_id,
            "source_trace": context.source_trace,
            "source_record_id": source_record_id,
            "arrival_time_ms": arrival_time_ms,
            "expected_duration_ms": expected_duration_ms,
            "deadline_ms": expected_duration_ms + 100,
            "priority": 1,
            "requested_cpu_millicores": 100,
            "requested_memory_mb": memory_mb,
            "requested_gpu_units": 0,
            "input_data_bytes": 1,
            "output_data_bytes": 1,
            "movable_state_bytes": 1,
            "preemptible": True,
            "restartable": True,
            "migration_allowed": True,
            "field_provenance": {
                "requested_memory_mb": "trace_derived",
                "requested_cpu_millicores": "modelled",
            },
            "source_trace_version": context.source_trace_version,
            "adapter_version": context.adapter_version,
        }
    )
