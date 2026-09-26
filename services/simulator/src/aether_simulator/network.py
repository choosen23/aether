from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Literal

from aether_simulator.topology import PathResult, TopologyGraph


@dataclass(frozen=True)
class Flow:
    flow_id: str
    source: str
    destination: str
    path: PathResult
    started_at_ms: int
    remaining_bytes: int
    purpose: Literal["input", "output", "migration"]


@dataclass(frozen=True)
class FlowCompletion:
    flow_id: str
    completed_at_ms: int


@dataclass
class _FlowRuntime:
    flow_id: str
    source: str
    destination: str
    path: PathResult
    started_at_ms: int
    remaining_bytes: float
    purpose: Literal["input", "output", "migration"]
    blocked: bool = False


class NetworkState:
    def __init__(self, graph: TopologyGraph) -> None:
        self._graph = graph
        self._link_bandwidth_bps = {
            link.link_id: link.bandwidth_bps for link in self._graph.document.links
        }
        self._now_ms = 0
        self._flows: dict[str, _FlowRuntime] = {}
        self._failures: dict[int, list[str]] = {}
        self._recoveries: dict[int, list[str]] = {}

    def start_flow(
        self,
        flow_id: str,
        source: str,
        destination: str,
        *,
        byte_count: int,
        at_ms: int,
        purpose: Literal["input", "output", "migration"] = "input",
    ) -> Flow:
        self.advance(at_ms)
        path = self._graph.shortest_path(source, destination, "transfer_time")
        if path is None:
            msg = f"no directed path from {source} to {destination}"
            raise ValueError(msg)

        runtime = _FlowRuntime(
            flow_id=flow_id,
            source=source,
            destination=destination,
            path=path,
            started_at_ms=at_ms,
            remaining_bytes=float(byte_count),
            purpose=purpose,
        )
        self._flows[flow_id] = runtime
        return self.flow(flow_id)

    def advance(self, to_ms: int) -> tuple[FlowCompletion, ...]:
        if to_ms < self._now_ms:
            msg = "cannot move network time backwards"
            raise ValueError(msg)

        completions: list[FlowCompletion] = []
        while self._now_ms < to_ms:
            rates = self._flow_rates_bps()
            next_completion_ms = self._next_completion_timestamp(rates)
            next_external_ms = self._next_external_timestamp()

            event_time = to_ms
            external_first = False
            if next_external_ms is not None and next_external_ms <= event_time:
                event_time = next_external_ms
                external_first = True
            if next_completion_ms is not None and next_completion_ms < event_time:
                event_time = next_completion_ms
                external_first = False

            self._advance_interval(event_time - self._now_ms, rates)
            self._now_ms = event_time

            if external_first:
                self._apply_external_events(self._now_ms)
                for flow in self._flows.values():
                    if flow.remaining_bytes <= 0:
                        flow.remaining_bytes = 1e-9
                continue

            completed_ids = [
                flow.flow_id for flow in self._flows.values() if flow.remaining_bytes <= 0
            ]
            for flow_id in sorted(completed_ids):
                del self._flows[flow_id]
                completions.append(FlowCompletion(flow_id=flow_id, completed_at_ms=self._now_ms))

        return tuple(completions)

    def fail_link(self, link_id: str, at_ms: int) -> None:
        self._failures.setdefault(at_ms, []).append(link_id)

    def recover_link(self, link_id: str, at_ms: int) -> None:
        self._recoveries.setdefault(at_ms, []).append(link_id)

    def flow(self, flow_id: str) -> Flow:
        flow = self._flows[flow_id]
        return Flow(
            flow_id=flow.flow_id,
            source=flow.source,
            destination=flow.destination,
            path=flow.path,
            started_at_ms=flow.started_at_ms,
            remaining_bytes=max(0, ceil(flow.remaining_bytes)),
            purpose=flow.purpose,
        )

    def _advance_interval(self, dt_ms: int, rates_bps: dict[str, float]) -> None:
        if dt_ms <= 0:
            return
        dt_seconds = dt_ms / 1000.0
        for flow in self._flows.values():
            rate = rates_bps.get(flow.flow_id, 0.0)
            transferred = (rate * dt_seconds) / 8.0
            flow.remaining_bytes -= transferred

    def _flow_rates_bps(self) -> dict[str, float]:
        link_users: dict[str, list[_FlowRuntime]] = {}
        for flow in self._flows.values():
            if flow.blocked:
                continue
            for link_id in flow.path.link_ids:
                link_users.setdefault(link_id, []).append(flow)

        rates: dict[str, float] = {}
        for flow in self._flows.values():
            if flow.blocked:
                rates[flow.flow_id] = 0.0
                continue
            shares: list[float] = []
            for link_id in flow.path.link_ids:
                users = link_users.get(link_id, [])
                if not users:
                    continue
                link_bandwidth = self._link_bandwidth_bps[link_id]
                shares.append(link_bandwidth / len(users))
            rates[flow.flow_id] = min(shares) if shares else 0.0
        return rates

    def _next_completion_timestamp(self, rates_bps: dict[str, float]) -> int | None:
        completion_times: list[int] = []
        for flow in self._flows.values():
            rate = rates_bps.get(flow.flow_id, 0.0)
            if rate <= 0:
                continue
            seconds = (flow.remaining_bytes * 8.0) / rate
            completion_times.append(self._now_ms + ceil(seconds * 1000))
        if not completion_times:
            return None
        return min(completion_times)

    def _next_external_timestamp(self) -> int | None:
        candidates = [
            time
            for time in (*self._failures.keys(), *self._recoveries.keys())
            if time >= self._now_ms
        ]
        if not candidates:
            return None
        return min(candidates)

    def _apply_external_events(self, at_ms: int) -> None:
        for link_id in sorted(self._failures.pop(at_ms, [])):
            self._graph.fail_link(link_id)
            for flow in self._flows.values():
                if link_id in flow.path.link_ids:
                    reroute = self._graph.shortest_path(
                        flow.source,
                        flow.destination,
                        "transfer_time",
                    )
                    if reroute is None:
                        flow.blocked = True
                        continue
                    flow.path = reroute
                    flow.blocked = False

        for link_id in sorted(self._recoveries.pop(at_ms, [])):
            self._graph.recover_link(link_id)
            for flow in self._flows.values():
                if not flow.blocked:
                    continue
                reroute = self._graph.shortest_path(flow.source, flow.destination, "transfer_time")
                if reroute is not None:
                    flow.path = reroute
                    flow.blocked = False
