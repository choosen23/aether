from __future__ import annotations

import asyncio
from collections.abc import Iterable
from uuid import UUID, uuid4

from aether_contracts import MobilityStreamMessage

from aether_api.metrics import mobility_dropped_clients


class MobilityBroadcaster:
    def __init__(self, queue_size: int = 100) -> None:
        self._clients: dict[UUID, asyncio.Queue[MobilityStreamMessage]] = {}
        self._queue_size = queue_size
        self.evictions = 0

    @property
    def client_ids(self) -> set[UUID]:
        return set(self._clients.keys())

    def subscribe(self) -> tuple[UUID, asyncio.Queue[MobilityStreamMessage]]:
        client_id = uuid4()
        self._clients[client_id] = asyncio.Queue(maxsize=self._queue_size)
        return client_id, self._clients[client_id]

    def unsubscribe(self, client_id: UUID) -> None:
        self._clients.pop(client_id, None)

    async def publish(self, message: MobilityStreamMessage) -> None:
        for client_id, queue in list(self._clients.items()):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                self.unsubscribe(client_id)
                self.evictions += 1
                mobility_dropped_clients.inc()

    def publish_nowait(self, message: MobilityStreamMessage) -> None:
        for client_id, queue in list(self._clients.items()):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                self.unsubscribe(client_id)
                self.evictions += 1
                mobility_dropped_clients.inc()

    async def publish_many(self, messages: Iterable[MobilityStreamMessage]) -> None:
        for message in messages:
            await self.publish(message)
