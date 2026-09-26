from __future__ import annotations

from hashlib import sha256
from pathlib import Path

import pytest
from aether_simulator.adapters.azure_functions import AzureFunctionsAdapter
from aether_simulator.preparation import AdapterContext, InvalidRowError

FIXTURE = Path("tests/fixtures/traces/azure_functions_official_schema.csv")
CONTEXT = AdapterContext(
    scenario_id="scenario-azure",
    source_trace="azure_functions",
    source_trace_version="azure-fixture-v1",
    adapter_version="azure-v1",
    seed=42,
)


def test_azure_preserves_trace_fields_and_models_missing_fields() -> None:
    adapter = AzureFunctionsAdapter()
    record = adapter.records(FIXTURE, partition="fixture")[0]
    item = adapter.normalize(record, CONTEXT)
    assert item.source_trace == "azure_functions"
    assert item.field_provenance["expected_duration_ms"] == "trace_derived"
    assert item.field_provenance["requested_cpu_millicores"] == "modelled"
    assert item.restartable is True


def test_azure_stable_id_matches_expected_formula() -> None:
    adapter = AzureFunctionsAdapter()
    record = adapter.records(FIXTURE, partition="fixture")[0]
    item = adapter.normalize(record, CONTEXT)
    expected = sha256(b"scenario-azure:af-001:azure-v1:42").hexdigest()[:26]
    assert item.workload_id == expected


def test_azure_invalid_numeric_row_gets_explicit_reason() -> None:
    adapter = AzureFunctionsAdapter()
    with pytest.raises(InvalidRowError, match="missing_duration"):
        adapter.normalize(
            {
                "source_record_id": "bad-1",
                "arrival_time_ms": "10",
                "duration_ms": "",
                "memory_mb": "256",
                "app_name": "x",
            },
            CONTEXT,
        )
