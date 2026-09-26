from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from heapq import heappop, heappush
from typing import Literal

from aether_contracts import CanonicalWorkload, RunSnapshot, SimulationEvent

from aether_simulator.compute import ComputeState
from aether_simulator.events import EVENT_PRIORITY, QueuedEvent
from aether_simulator.mobility import MobilityExpiredError, MobilityTimeline, assign_origin
from aether_simulator.network import NetworkState
from aether_simulator.policies import Scheduler
from aether_simulator.topology import TopologyGraph

EngineStatus = Literal["running", "paused", "completed", "failed", "cancelled"]


@dataclass(frozen=True)
class WorkloadOutcome:
    workload_id: str
    status: str
    reason: str | None = None


@dataclass(frozen=True)
class EngineResult:
    run_id: str
    status: str
    stopping_reason: str | None
    simulated_time_ms: int
    last_committed_time_ms: int
    events: tuple[SimulationEvent, ...]
    workloads: tuple[WorkloadOutcome, ...]

    def workload(self, workload_id: str) -> WorkloadOutcome:
        for outcome in self.workloads:
            if outcome.workload_id == workload_id:
                return outcome
        msg = f"unknown workload_id: {workload_id}"
        raise KeyError(msg)


class SimulationEngine:
    def __init__(
        self,
        *,
        run_id: str,
        workloads: tuple[CanonicalWorkload, ...],
        mobility: MobilityTimeline,
        topology: TopologyGraph,
        network: NetworkState,
        compute: ComputeState,
        scheduler: Scheduler,
        failure_events: tuple[dict[str, object], ...] = (),
        max_mobility_age_ms: int = 30_000,
    ) -> None:
        self._run_id = run_id
        self._workloads = {item.workload_id: item for item in workloads}
        self._mobility = mobility
        self._topology = topology
        self._network = network
        self._compute = compute
        self._scheduler = scheduler
        self._max_mobility_age_ms = max_mobility_age_ms

        self._queue: list[QueuedEvent] = []
        self._events: list[SimulationEvent] = []
        self._sequence = 0
        self._event_counter = 0
        self._simulated_time_ms = 0
        self._last_committed_time_ms = 0
        self._status: EngineStatus = "running"
        self._stopping_reason: str | None = None
        self._cancelled = False
        self._paused = False
        self._outcomes: dict[str, WorkloadOutcome] = {
            workload_id: WorkloadOutcome(workload_id=workload_id, status="arrived")
            for workload_id in self._workloads
        }

        for workload in workloads:
            self._schedule(
                at_ms=workload.arrival_time_ms,
                kind="workload_arrived",
                payload={"workload_id": workload.workload_id},
            )
            self._schedule(
                at_ms=workload.deadline_ms,
                kind="deadline_reached",
                payload={"workload_id": workload.workload_id},
            )

        for failure_event in failure_events:
            self._schedule(
                at_ms=_coerce_int(failure_event["at_ms"]),
                kind=str(failure_event["kind"]),
                payload={
                    key: value
                    for key, value in failure_event.items()
                    if key not in {"at_ms", "kind"}
                },
            )

    def run(self, until_ms: int | None = None) -> EngineResult:
        while self._queue and not self._cancelled and not self._paused:
            queued = heappop(self._queue)
            if until_ms is not None and queued.at_ms > until_ms:
                heappush(self._queue, queued)
                break

            self._simulated_time_ms = queued.at_ms
            self._process(queued)

            completions = self._network.advance(self._simulated_time_ms)
            for completion in completions:
                self._schedule(
                    at_ms=completion.completed_at_ms,
                    kind="flow_completed",
                    payload={"flow_id": completion.flow_id},
                )

        if self._cancelled:
            self._status = "cancelled"
        elif self._paused:
            self._status = "paused"
            self._simulated_time_ms = self._last_committed_time_ms
        elif self._queue:
            self._status = "running"
        else:
            self._status = "completed"

        return EngineResult(
            run_id=self._run_id,
            status=self._status,
            stopping_reason=self._stopping_reason,
            simulated_time_ms=self._simulated_time_ms,
            last_committed_time_ms=self._last_committed_time_ms,
            events=tuple(self._events),
            workloads=tuple(sorted(self._outcomes.values(), key=lambda item: item.workload_id)),
        )

    def pause(self, reason: str) -> None:
        self._paused = True
        self._stopping_reason = reason

    def resume(self) -> None:
        self._paused = False
        self._stopping_reason = None

    def cancel(self, reason: str) -> None:
        self._cancelled = True
        self._stopping_reason = reason

    def snapshot(self) -> RunSnapshot:
        return RunSnapshot(
            run_id=self._run_id,
            status=self._status,
            sequence=self._sequence,
            simulated_time_ms=self._simulated_time_ms,
            last_committed_time_ms=self._last_committed_time_ms,
            stopping_reason=self._stopping_reason,
            updated_at=datetime(2026, 9, 25, 0, 0, tzinfo=UTC),
        )

    def _process(self, queued: QueuedEvent) -> None:
        if queued.kind in {"node_failed", "link_failed", "node_recovered", "link_recovered"}:
            self._record_event(queued)
            return

        if queued.kind == "workload_arrived":
            self._process_arrival(queued)
            return

        if queued.kind == "execution_completed":
            workload_id = str(queued.payload["workload_id"])
            self._compute.complete(workload_id)
            self._outcomes[workload_id] = WorkloadOutcome(
                workload_id=workload_id,
                status="completed",
            )
            self._record_event(queued)
            return

        if queued.kind == "deadline_reached":
            workload_id = str(queued.payload["workload_id"])
            outcome = self._outcomes[workload_id]
            if outcome.status in {"arrived", "queued", "placed", "transferring"}:
                self._compute.reject(workload_id, "deadline_expired_while_disconnected")
                self._outcomes[workload_id] = WorkloadOutcome(
                    workload_id=workload_id,
                    status="rejected",
                    reason="deadline_expired_while_disconnected",
                )
            self._record_event(queued)
            return

        if queued.kind == "flow_completed":
            self._record_event(queued)
            return

        msg = f"unsupported event kind: {queued.kind}"
        raise ValueError(msg)

    def _process_arrival(self, queued: QueuedEvent) -> None:
        workload_id = str(queued.payload["workload_id"])
        workload = self._workloads[workload_id]
        self._compute.arrive(workload)

        try:
            distribution = self._mobility.distribution_at(
                simulated_ms=queued.at_ms,
                max_age_ms=self._max_mobility_age_ms,
            )
        except MobilityExpiredError:
            self.pause("mobility_snapshot_expired")
            return

        with_origin = assign_origin(workload, distribution, seed=17)

        node_ids = sorted(node.node_id for node in self._topology.document.nodes)
        if len(node_ids) < 2:
            self._compute.reject(workload_id, "deadline_expired_while_disconnected")
            self._outcomes[workload_id] = WorkloadOutcome(
                workload_id=workload_id,
                status="rejected",
                reason="deadline_expired_while_disconnected",
            )
            self._record_event(queued)
            return

        source_node = node_ids[0]
        destination_node = node_ids[-1]
        path = self._topology.shortest_path(source_node, destination_node, "latency")
        if path is None:
            self._outcomes[workload_id] = WorkloadOutcome(workload_id=workload_id, status="arrived")
            self._record_event(queued)
            return

        self._compute.reserve(destination_node, with_origin)
        self._compute.place(workload_id, destination_node)
        self._compute.enqueue(with_origin)
        self._compute.start(workload_id, destination_node)
        self._outcomes[workload_id] = WorkloadOutcome(workload_id=workload_id, status="running")

        self._schedule(
            at_ms=queued.at_ms + with_origin.expected_duration_ms,
            kind="execution_completed",
            payload={"workload_id": workload_id},
        )
        self._record_event(queued)

    def _schedule(self, *, at_ms: int, kind: str, payload: dict[str, object]) -> None:
        self._event_counter += 1
        event_id = f"q-{self._event_counter:08d}"
        priority = EVENT_PRIORITY[kind]
        heappush(
            self._queue,
            QueuedEvent(
                at_ms=at_ms,
                priority=priority,
                event_id=event_id,
                kind=kind,
                payload=payload,
            ),
        )

    def _record_event(self, queued: QueuedEvent) -> None:
        self._sequence += 1
        event = SimulationEvent(
            run_id=self._run_id,
            event_id=f"evt-{self._sequence:08d}",
            sequence=self._sequence,
            simulated_time_ms=queued.at_ms,
            event_type=queued.kind,
            payload=dict(queued.payload),
        )
        self._events.append(event)
        self._last_committed_time_ms = queued.at_ms


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        msg = "boolean value is not a valid integer timestamp"
        raise TypeError(msg)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value)
    msg = f"cannot convert value {value!r} to integer"
    raise TypeError(msg)
