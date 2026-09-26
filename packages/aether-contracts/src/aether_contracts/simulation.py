from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)


def _serialize_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _normalize_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


FieldProvenance = Literal["observed", "trace_derived", "modelled", "simulated"]


class CanonicalWorkload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    workload_id: str
    scenario_id: str
    source_trace: Literal["azure_functions", "google_borg"]
    source_record_id: str
    arrival_time_ms: Annotated[int, Field(ge=0)]
    expected_duration_ms: Annotated[int, Field(gt=0)]
    deadline_ms: Annotated[int, Field(gt=0)]
    priority: int
    requested_cpu_millicores: Annotated[int, Field(ge=0)]
    requested_memory_mb: Annotated[int, Field(ge=0)]
    requested_gpu_units: Annotated[int, Field(ge=0)] = 0
    input_data_bytes: Annotated[int, Field(ge=0)]
    output_data_bytes: Annotated[int, Field(ge=0)]
    movable_state_bytes: Annotated[int, Field(ge=0)]
    preemptible: bool
    restartable: bool
    migration_allowed: bool
    origin_h3_cell: str | None = None
    origin_latitude: Annotated[float | None, Field(ge=-90, le=90)] = None
    origin_longitude: Annotated[float | None, Field(ge=-180, le=180)] = None
    field_provenance: dict[str, FieldProvenance]
    source_trace_version: str
    adapter_version: str

    @model_validator(mode="after")
    def validate_origin_triplet(self) -> Self:
        values = (self.origin_h3_cell, self.origin_latitude, self.origin_longitude)
        if any(value is None for value in values) and any(value is not None for value in values):
            msg = "origin_h3_cell, origin_latitude, and origin_longitude must be set together"
            raise ValueError(msg)
        return self


class TopologyNode(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    node_id: str
    site_id: str
    tier: str
    latitude: Annotated[float, Field(ge=-90, le=90)]
    longitude: Annotated[float, Field(ge=-180, le=180)]
    capacity_cpu_millicores: Annotated[int, Field(ge=0)]
    capacity_memory_mb: Annotated[int, Field(ge=0)]
    capacity_gpu_units: Annotated[int, Field(ge=0)] = 0
    capacity_slots: Annotated[int, Field(ge=0)]
    operating_cost_per_hour: Annotated[float, Field(ge=0)] = 0
    operating_energy_watts: Annotated[float, Field(ge=0)] = 0
    provenance: Literal["simulated"]


class TopologyLink(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    link_id: str
    source: str
    destination: str
    bandwidth_bps: Annotated[float, Field(gt=0)]
    propagation_latency_ms: Annotated[float, Field(ge=0)]
    transfer_cost_per_gb: Annotated[float, Field(ge=0)] = 0
    energy_joules_per_gb: Annotated[float, Field(ge=0)] = 0
    provenance: Literal["simulated"]


class TopologyDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    topology_id: str
    generated_at: AwareDatetime
    nodes: tuple[TopologyNode, ...]
    links: tuple[TopologyLink, ...]

    @field_validator("generated_at")
    @classmethod
    def normalize_datetimes(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("generated_at")
    def serialize_datetimes(self, value: datetime) -> str:
        return _serialize_utc(value)

    @model_validator(mode="after")
    def validate_graph_integrity(self) -> Self:
        node_ids = {node.node_id for node in self.nodes}
        if len(node_ids) != len(self.nodes):
            msg = "duplicate node_id found in topology nodes"
            raise ValueError(msg)

        link_ids = {link.link_id for link in self.links}
        if len(link_ids) != len(self.links):
            msg = "duplicate link_id found in topology links"
            raise ValueError(msg)

        for link in self.links:
            if link.source not in node_ids:
                msg = f"unknown source node {link.source}"
                raise ValueError(msg)
            if link.destination not in node_ids:
                msg = f"unknown destination node {link.destination}"
                raise ValueError(msg)

        return self


class ObjectiveWeights(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    latency: Annotated[float, Field(ge=0)]
    transfer_time: Annotated[float, Field(ge=0)]
    queue_delay: Annotated[float, Field(ge=0)]
    deadline_risk: Annotated[float, Field(ge=0)]
    migration_cost: Annotated[float, Field(ge=0)]
    transfer_cost: Annotated[float, Field(ge=0)]
    compute_energy: Annotated[float, Field(ge=0)]
    network_energy: Annotated[float, Field(ge=0)]

    @model_validator(mode="after")
    def validate_positive_sum(self) -> Self:
        total = (
            self.latency
            + self.transfer_time
            + self.queue_delay
            + self.deadline_risk
            + self.migration_cost
            + self.transfer_cost
            + self.compute_energy
            + self.network_energy
        )
        if total <= 0:
            msg = "objective weight sum must be greater than 0"
            raise ValueError(msg)
        return self


class RunConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    run_id: str
    scenario_id: str
    source_traces: tuple[Literal["azure_functions", "google_borg"], ...]
    scheduler_policy: str
    topology_id: str
    seed: int
    scale_factor: Annotated[float, Field(gt=0, le=1)]
    time_acceleration: Annotated[float, Field(gt=0)] = 1
    started_at: AwareDatetime

    @field_validator("started_at")
    @classmethod
    def normalize_datetimes(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("started_at")
    def serialize_datetimes(self, value: datetime) -> str:
        return _serialize_utc(value)

    @model_validator(mode="after")
    def validate_single_trace(self) -> Self:
        if len(self.source_traces) != 1:
            msg = "run configuration must include exactly one source trace"
            raise ValueError(msg)
        return self


class SimulationEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    run_id: str
    event_id: str
    sequence: Annotated[int, Field(ge=0)]
    simulated_time_ms: Annotated[int, Field(ge=0)]
    event_type: str
    payload: dict[str, object] = Field(default_factory=dict)


class RunSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    run_id: str
    status: Literal["running", "paused", "completed", "failed", "cancelled"]
    sequence: Annotated[int, Field(ge=0)]
    simulated_time_ms: Annotated[int, Field(ge=0)]
    last_committed_time_ms: Annotated[int, Field(ge=0)]
    stopping_reason: str | None = None
    updated_at: AwareDatetime

    @field_validator("updated_at")
    @classmethod
    def normalize_datetimes(cls, value: datetime) -> datetime:
        return _normalize_utc(value)

    @field_serializer("updated_at")
    def serialize_datetimes(self, value: datetime) -> str:
        return _serialize_utc(value)


class RunSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    run_id: str
    completed: bool
    completion_rate: Annotated[float, Field(ge=0, le=1)]
    rejection_rate: Annotated[float, Field(ge=0, le=1)]
    failure_rate: Annotated[float, Field(ge=0, le=1)]
    combined_score: float | None = None
    objective_weights: ObjectiveWeights
