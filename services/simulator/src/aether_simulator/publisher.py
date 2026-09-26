from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

from aether_contracts import SimulationEvent


class ProducerProtocol(Protocol):
    async def send_and_wait(self, topic: str, value: bytes, key: bytes | None = None) -> object: ...


@dataclass(frozen=True)
class PublishOutcome:
    delivered: bool
    topic: str
    event_id: str


class SimulationPublisher:
    def __init__(self, producer: ProducerProtocol, topic: str = "simulation.event.v1") -> None:
        self._producer = producer
        self._topic = topic

    async def publish(self, event: SimulationEvent) -> PublishOutcome:
        payload = json.dumps(
            event.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        key = event.run_id.encode("utf-8")
        try:
            await self._producer.send_and_wait(self._topic, payload, key=key)
            return PublishOutcome(delivered=True, topic=self._topic, event_id=event.event_id)
        except Exception:
            return PublishOutcome(delivered=False, topic=self._topic, event_id=event.event_id)
