from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

EVENT_PRIORITY = {
    "node_failed": 0,
    "link_failed": 0,
    "node_recovered": 1,
    "link_recovered": 1,
    "flow_completed": 2,
    "workload_arrived": 3,
    "execution_completed": 4,
    "deadline_reached": 5,
}


@dataclass(order=True, frozen=True)
class QueuedEvent:
    at_ms: int
    priority: int
    event_id: str
    kind: str = field(compare=False)
    payload: Mapping[str, object] = field(compare=False)
