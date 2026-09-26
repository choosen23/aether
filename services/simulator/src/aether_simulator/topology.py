from __future__ import annotations

from dataclasses import dataclass
from heapq import heappop, heappush
from typing import Literal

from aether_contracts import TopologyDocument, TopologyLink

Criterion = Literal["latency", "transfer_time", "cost", "energy"]


@dataclass(frozen=True)
class PathResult:
    node_ids: tuple[str, ...]
    link_ids: tuple[str, ...]
    propagation_latency_ms: float
    bottleneck_bandwidth_bps: float
    transfer_cost: float
    network_energy_joules: float


class TopologyGraph:
    def __init__(self, document: TopologyDocument) -> None:
        self.document = document
        self._nodes = {node.node_id: node for node in document.nodes}
        self._links = {link.link_id: link for link in document.links}
        self._adj: dict[str, tuple[TopologyLink, ...]] = {}
        for node_id in self._nodes:
            outgoing = [link for link in document.links if link.source == node_id]
            self._adj[node_id] = tuple(sorted(outgoing, key=lambda item: item.link_id))

        self._failed_nodes: set[str] = set()
        self._failed_links: set[str] = set()
        self._failure_revision = 0
        self._congestion_revision = 0
        self._cache: dict[tuple[str, str, str, int, int], PathResult | None] = {}

    def shortest_path(
        self,
        source: str,
        destination: str,
        criterion: Criterion,
        failed_nodes: frozenset[str] = frozenset(),
        failed_links: frozenset[str] = frozenset(),
    ) -> PathResult | None:
        key = (
            source,
            destination,
            criterion,
            self._failure_revision,
            self._congestion_revision,
        )
        if not failed_nodes and not failed_links and key in self._cache:
            return self._cache[key]

        path = self._compute_shortest_path(
            source,
            destination,
            criterion,
            failed_nodes,
            failed_links,
        )
        if not failed_nodes and not failed_links:
            self._cache[key] = path
        return path

    def fail_link(self, link_id: str) -> None:
        self._failed_links.add(link_id)
        self._failure_revision += 1
        self._cache.clear()

    def recover_link(self, link_id: str) -> None:
        self._failed_links.discard(link_id)
        self._failure_revision += 1
        self._cache.clear()

    def invalidate(self, failure_domain: str) -> None:
        self._failure_revision += 1
        if failure_domain.startswith("node:"):
            self._failed_nodes.add(failure_domain.removeprefix("node:"))
        if failure_domain.startswith("link:"):
            self._failed_links.add(failure_domain.removeprefix("link:"))
        self._cache.clear()

    def _compute_shortest_path(
        self,
        source: str,
        destination: str,
        criterion: Criterion,
        failed_nodes: frozenset[str],
        failed_links: frozenset[str],
    ) -> PathResult | None:
        if source not in self._nodes or destination not in self._nodes:
            return None

        blocked_nodes = set(failed_nodes) | self._failed_nodes
        blocked_links = set(failed_links) | self._failed_links
        if source in blocked_nodes or destination in blocked_nodes:
            return None

        queue: list[
            tuple[
                float,
                str,
                tuple[str, ...],
                tuple[str, ...],
                float,
                float,
                float,
                float,
            ]
        ] = []
        heappush(queue, (0.0, source, (source,), (), float("inf"), 0.0, 0.0, 0.0))
        best_score: dict[str, float] = {source: 0.0}

        while queue:
            (
                score,
                node_id,
                path_nodes,
                path_links,
                bottleneck,
                latency,
                cost,
                energy,
            ) = heappop(queue)
            if node_id == destination:
                return PathResult(
                    node_ids=path_nodes,
                    link_ids=path_links,
                    propagation_latency_ms=latency,
                    bottleneck_bandwidth_bps=bottleneck if bottleneck != float("inf") else 0.0,
                    transfer_cost=cost,
                    network_energy_joules=energy,
                )

            if score > best_score.get(node_id, float("inf")):
                continue

            for link in self._adj.get(node_id, ()):
                if link.link_id in blocked_links:
                    continue
                if link.destination in blocked_nodes:
                    continue

                link_score = _link_weight(link, criterion)
                next_score = score + link_score
                best = best_score.get(link.destination)
                next_links = (*path_links, link.link_id)
                should_replace = best is None or next_score < best
                if not should_replace and best is not None and next_score == best:
                    # Stable tie-break by destination node id then path link IDs.
                    should_replace = next_links < self._best_links_for_destination(
                        queue,
                        link.destination,
                    )

                if not should_replace:
                    continue

                best_score[link.destination] = next_score
                heappush(
                    queue,
                    (
                        next_score,
                        link.destination,
                        (*path_nodes, link.destination),
                        next_links,
                        min(bottleneck, link.bandwidth_bps),
                        latency + link.propagation_latency_ms,
                        cost + link.transfer_cost_per_gb,
                        energy + link.energy_joules_per_gb,
                    ),
                )

        return None

    @staticmethod
    def _best_links_for_destination(
        queue: list[
            tuple[
                float,
                str,
                tuple[str, ...],
                tuple[str, ...],
                float,
                float,
                float,
                float,
            ]
        ],
        destination: str,
    ) -> tuple[str, ...]:
        candidates = [entry[3] for entry in queue if entry[1] == destination]
        if not candidates:
            return ("~",)
        return min(candidates)


def _link_weight(link: TopologyLink, criterion: Criterion) -> float:
    if criterion == "latency":
        return link.propagation_latency_ms
    if criterion == "transfer_time":
        return 1.0 / link.bandwidth_bps
    if criterion == "cost":
        return link.transfer_cost_per_gb
    if criterion == "energy":
        return link.energy_joules_per_gb
    msg = f"unsupported path criterion: {criterion}"
    raise ValueError(msg)
