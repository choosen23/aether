from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from aether_contracts import CanonicalWorkload, TopologyNode

WorkloadStatus = Literal[
    "arrived",
    "placed",
    "transferring",
    "queued",
    "running",
    "completed",
    "rejected",
    "unschedulable",
    "failed",
]


class CapacityUnavailableError(RuntimeError):
    pass


class InvalidLifecycleTransitionError(RuntimeError):
    pass


@dataclass(frozen=True)
class ResourceVector:
    cpu_millicores: int
    memory_mb: int
    gpu_units: int
    slots: int = 1

    def fits_within(self, capacity: ResourceVector) -> bool:
        return (
            self.cpu_millicores <= capacity.cpu_millicores
            and self.memory_mb <= capacity.memory_mb
            and self.gpu_units <= capacity.gpu_units
            and self.slots <= capacity.slots
        )


@dataclass(frozen=True)
class NodeLedger:
    node_id: str
    capacity: ResourceVector
    used: ResourceVector


@dataclass(frozen=True)
class WorkloadRuntime:
    workload_id: str
    node_id: str | None
    status: WorkloadStatus
    reason: str | None = None


ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "arrived": {"placed", "rejected", "unschedulable"},
    "placed": {"transferring", "queued", "failed"},
    "transferring": {"queued", "failed", "rejected"},
    "queued": {"running", "rejected", "failed"},
    "running": {"completed", "transferring", "failed"},
    "completed": set(),
    "rejected": set(),
    "unschedulable": set(),
    "failed": set(),
}


@dataclass
class _MutableNode:
    node_id: str
    capacity: ResourceVector
    used: ResourceVector


class ComputeState:
    def __init__(self, nodes: dict[str, _MutableNode]) -> None:
        self._nodes = nodes
        self._reserved_by_workload: dict[str, tuple[str, ResourceVector]] = {}
        self._runtimes: dict[str, WorkloadRuntime] = {}

    @classmethod
    def from_nodes(cls, nodes: list[TopologyNode]) -> ComputeState:
        mapped: dict[str, _MutableNode] = {}
        for node in nodes:
            mapped[node.node_id] = _MutableNode(
                node_id=node.node_id,
                capacity=ResourceVector(
                    cpu_millicores=node.capacity_cpu_millicores,
                    memory_mb=node.capacity_memory_mb,
                    gpu_units=node.capacity_gpu_units,
                    slots=node.capacity_slots,
                ),
                used=ResourceVector(cpu_millicores=0, memory_mb=0, gpu_units=0, slots=0),
            )
        return cls(mapped)

    def node(self, node_id: str) -> NodeLedger:
        node = self._nodes[node_id]
        return NodeLedger(node_id=node.node_id, capacity=node.capacity, used=node.used)

    def can_fit(self, node_id: str, workload: CanonicalWorkload) -> bool:
        node = self._nodes[node_id]
        request = _resource_request(workload)
        proposed = _add(node.used, request)
        return proposed.fits_within(node.capacity)

    def classify(
        self,
        workload: CanonicalWorkload,
        eligible_node_ids: tuple[str, ...],
    ) -> Literal["feasible", "queued", "unschedulable"]:
        request = _resource_request(workload)
        capacities = [self._nodes[node_id].capacity for node_id in eligible_node_ids]
        if not capacities:
            return "unschedulable"

        if not any(request.fits_within(capacity) for capacity in capacities):
            return "unschedulable"

        if any(self.can_fit(node_id, workload) for node_id in eligible_node_ids):
            return "feasible"
        return "queued"

    def reserve(self, node_id: str, workload: CanonicalWorkload) -> None:
        request = _resource_request(workload)
        node = self._nodes[node_id]
        proposed = _add(node.used, request)
        if not proposed.fits_within(node.capacity):
            msg = f"insufficient capacity on node {node_id}"
            raise CapacityUnavailableError(msg)

        node.used = proposed
        self._reserved_by_workload[workload.workload_id] = (node_id, request)

    def release(self, workload_id: str) -> None:
        reservation = self._reserved_by_workload.pop(workload_id, None)
        if reservation is None:
            return

        node_id, request = reservation
        node = self._nodes[node_id]
        node.used = _subtract(node.used, request)

    def enqueue(self, workload: CanonicalWorkload) -> None:
        self._transition(workload.workload_id, to_status="queued", node_id=None)

    def start(self, workload_id: str, node_id: str) -> None:
        self._transition(workload_id, to_status="running", node_id=node_id)

    def complete(self, workload_id: str) -> None:
        self.release(workload_id)
        self._transition(workload_id, to_status="completed", node_id=None)

    def reject(self, workload_id: str, reason: str) -> None:
        self.release(workload_id)
        self._transition(workload_id, to_status="rejected", node_id=None, reason=reason)

    def fail(self, workload_id: str, reason: str) -> None:
        self.release(workload_id)
        self._transition(workload_id, to_status="failed", node_id=None, reason=reason)

    def begin_migration(self, workload_id: str, source_node_id: str) -> None:
        self._transition(workload_id, to_status="transferring", node_id=source_node_id)

    def arrive(self, workload: CanonicalWorkload) -> None:
        self._runtimes[workload.workload_id] = WorkloadRuntime(
            workload_id=workload.workload_id,
            node_id=None,
            status="arrived",
            reason=None,
        )

    def place(self, workload_id: str, node_id: str) -> None:
        self._transition(workload_id, to_status="placed", node_id=node_id)

    def runtime(self, workload_id: str) -> WorkloadRuntime:
        return self._runtimes[workload_id]

    def _transition(
        self,
        workload_id: str,
        *,
        to_status: WorkloadStatus,
        node_id: str | None,
        reason: str | None = None,
    ) -> None:
        current = self._runtimes.get(workload_id)
        if current is None:
            current = WorkloadRuntime(
                workload_id=workload_id,
                node_id=None,
                status="arrived",
                reason=None,
            )

        allowed = ALLOWED_TRANSITIONS[current.status]
        if to_status not in allowed:
            msg = f"invalid lifecycle transition from {current.status} to {to_status}"
            raise InvalidLifecycleTransitionError(msg)

        self._runtimes[workload_id] = WorkloadRuntime(
            workload_id=workload_id,
            node_id=node_id,
            status=to_status,
            reason=reason,
        )


def _resource_request(workload: CanonicalWorkload) -> ResourceVector:
    return ResourceVector(
        cpu_millicores=workload.requested_cpu_millicores,
        memory_mb=workload.requested_memory_mb,
        gpu_units=workload.requested_gpu_units,
        slots=1,
    )


def _add(a: ResourceVector, b: ResourceVector) -> ResourceVector:
    return ResourceVector(
        cpu_millicores=a.cpu_millicores + b.cpu_millicores,
        memory_mb=a.memory_mb + b.memory_mb,
        gpu_units=a.gpu_units + b.gpu_units,
        slots=a.slots + b.slots,
    )


def _subtract(a: ResourceVector, b: ResourceVector) -> ResourceVector:
    return ResourceVector(
        cpu_millicores=a.cpu_millicores - b.cpu_millicores,
        memory_mb=a.memory_mb - b.memory_mb,
        gpu_units=a.gpu_units - b.gpu_units,
        slots=a.slots - b.slots,
    )
