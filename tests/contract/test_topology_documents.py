from __future__ import annotations

import json
from pathlib import Path

from aether_contracts import TopologyDocument


def test_topology_schema_document_exists_and_is_v1() -> None:
    schema_path = Path("topologies/schema-v1.json")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert schema["title"] == "Aether Topology Schema v1"
    assert schema["properties"]["schema_version"]["const"] == 1


def test_eu_profiles_load_with_shared_contract() -> None:
    small = TopologyDocument.model_validate_json(
        Path("topologies/eu-small-v1.json").read_text(encoding="utf-8")
    )
    medium = TopologyDocument.model_validate_json(
        Path("topologies/eu-medium-v1.json").read_text(encoding="utf-8")
    )
    assert small.schema_version == 1
    assert medium.schema_version == 1
    assert small.topology_id == "eu-small-v1"
    assert medium.topology_id == "eu-medium-v1"
