from __future__ import annotations

import csv
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from typing import ClassVar

from aether_contracts import CanonicalWorkload

from aether_simulator.adapters.azure_functions import stable_workload_id
from aether_simulator.preparation import AdapterContext, InvalidRowError

BORG_MODEL_VERSION = "borg-missing-fields-v1"


class BorgAdapter:
    name = "google_borg"
    version = "borg-v1"

    _required_columns: ClassVar[set[str]] = {
        "source_record_id",
        "submission_time_ms",
        "duration_ms",
        "requested_cpu_millicores",
        "requested_memory_mb",
        "priority",
    }

    def validate_schema(self, columns: frozenset[str]) -> None:
        missing = self._required_columns - columns
        if missing:
            msg = f"missing required columns: {sorted(missing)}"
            raise ValueError(msg)

    def records(self, source: Path, partition: str) -> list[Mapping[str, object]]:
        with source.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def normalize(self, record: Mapping[str, object], context: AdapterContext) -> CanonicalWorkload:
        source_record_id = _require_string(record, "source_record_id")
        arrival = _require_int(record, "submission_time_ms", reason="invalid_submission_time")
        duration = _require_int(record, "duration_ms", reason="missing_duration")
        cpu = _require_int(record, "requested_cpu_millicores", reason="invalid_cpu")
        memory = _require_int(record, "requested_memory_mb", reason="invalid_memory")
        priority = _require_int(record, "priority", reason="invalid_priority")

        stable_hash = _stable_hash(context.seed, source_record_id)
        deadline = duration + 1_000 + (stable_hash % 2_000)
        input_bytes = 1_000 + (stable_hash % 4_000)
        output_bytes = 1_000 + (stable_hash % 3_000)
        state_bytes = 1_000 + (stable_hash % 5_000)

        return CanonicalWorkload(
            workload_id=stable_workload_id(context, source_record_id),
            scenario_id=context.scenario_id,
            source_trace="google_borg",
            source_record_id=source_record_id,
            arrival_time_ms=arrival,
            expected_duration_ms=duration,
            deadline_ms=deadline,
            priority=priority,
            requested_cpu_millicores=cpu,
            requested_memory_mb=memory,
            requested_gpu_units=0,
            input_data_bytes=input_bytes,
            output_data_bytes=output_bytes,
            movable_state_bytes=state_bytes,
            preemptible=False,
            restartable=True,
            migration_allowed=True,
            field_provenance={
                "expected_duration_ms": "trace_derived",
                "requested_cpu_millicores": "trace_derived",
                "requested_memory_mb": "trace_derived",
                "priority": "trace_derived",
                "deadline_ms": "modelled",
                "input_data_bytes": "modelled",
                "output_data_bytes": "modelled",
                "movable_state_bytes": "modelled",
            },
            source_trace_version=context.source_trace_version,
            adapter_version=context.adapter_version,
        )


def _stable_hash(seed: int, source_record_id: str) -> int:
    digest = sha256(f"{seed}:{source_record_id}:{BORG_MODEL_VERSION}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _require_string(record: Mapping[str, object], key: str) -> str:
    value = str(record.get(key, "")).strip()
    if not value:
        msg = f"missing required value {key}"
        raise InvalidRowError(msg)
    return value


def _require_int(record: Mapping[str, object], key: str, *, reason: str) -> int:
    raw = record.get(key)
    if raw is None or str(raw).strip() == "":
        raise InvalidRowError(reason)
    try:
        return int(float(str(raw)))
    except ValueError as exc:
        raise InvalidRowError(reason) from exc
