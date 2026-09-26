from __future__ import annotations

from hashlib import sha256
from pathlib import Path

import pytest
from aether_simulator.adapters.borg import BorgAdapter
from aether_simulator.preparation import AdapterContext, InvalidRowError

FIXTURE = Path("tests/fixtures/traces/borg_official_schema.csv")
CONTEXT = AdapterContext(
    scenario_id="scenario-borg",
    source_trace="google_borg",
    source_trace_version="borg-fixture-v1",
    adapter_version="borg-v1",
    seed=9,
)


def test_borg_uses_trace_resources_and_priority() -> None:
    adapter = BorgAdapter()
    record = adapter.records(FIXTURE, partition="fixture")[0]
    item = adapter.normalize(record, CONTEXT)
    assert item.source_trace == "google_borg"
    assert item.requested_cpu_millicores == 500
    assert item.priority == 7
    assert item.field_provenance["requested_cpu_millicores"] == "trace_derived"


def test_borg_stable_id_matches_expected_formula() -> None:
    adapter = BorgAdapter()
    record = adapter.records(FIXTURE, partition="fixture")[0]
    item = adapter.normalize(record, CONTEXT)
    expected = sha256(b"scenario-borg:bg-001:borg-v1:9").hexdigest()[:26]
    assert item.workload_id == expected


def test_borg_invalid_numeric_row_gets_explicit_reason() -> None:
    adapter = BorgAdapter()
    with pytest.raises(InvalidRowError, match="invalid_cpu"):
        adapter.normalize(
            {
                "source_record_id": "bad-1",
                "submission_time_ms": "1",
                "duration_ms": "10",
                "requested_cpu_millicores": "abc",
                "requested_memory_mb": "256",
                "priority": "7",
            },
            CONTEXT,
        )
