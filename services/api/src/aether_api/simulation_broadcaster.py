from __future__ import annotations

import asyncio
from uuid import UUID, uuid4


class SimulationBroadcaster:
    def __init__(self, queue_size: int = 100) -> None:
        self._queue_size = queue_size
        self._clients_by_run: dict[str, dict[UUID, asyncio.Queue[dict[str, object]]]] = {}

    def subscribe(self, run_id: str) -> tuple[UUID, asyncio.Queue[dict[str, object]]]:
        client_id = uuid4()
        clients = self._clients_by_run.setdefault(run_id, {})
        clients[client_id] = asyncio.Queue(maxsize=self._queue_size)
        return client_id, clients[client_id]

    def unsubscribe(self, run_id: str, client_id: UUID) -> None:
        clients = self._clients_by_run.get(run_id)
        if clients is None:
            return
        clients.pop(client_id, None)
        if not clients:
            self._clients_by_run.pop(run_id, None)

    async def publish(self, run_id: str, message: dict[str, object]) -> None:
        self.publish_nowait(run_id, message)

    def publish_nowait(self, run_id: str, message: dict[str, object]) -> None:
        clients = list(self._clients_by_run.get(run_id, {}).values())
        for queue in clients:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(message)
