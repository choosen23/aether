from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class PreparationManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    adapter_name: str
    adapter_version: str
    source_path: str
    source_sha256: str
    output_sha256: str
    partition: str
    start_iso: str
    end_iso: str
    scale_factor: float
    seed: int
    accepted_count: int
    rejected_count: int
    rejected_rows: dict[str, int]
