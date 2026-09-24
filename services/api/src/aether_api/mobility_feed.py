from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from typing import Protocol

from aether_contracts import MobilityCell, MobilityCellRemove, MobilityCellUpsert


class MobilityBroadcasterProtocol(Protocol):
    async def publish(self, message: object) -> None: ...


class MobilityRepositoryProtocol(Protocol):
    async def remove_expired(self, now: datetime) -> list[str]: ...


class MobilityFeed:
    def __init__(
        self,
        *,
        redis: object,
        broadcaster: MobilityBroadcasterProtocol,
        repository: MobilityRepositoryProtocol,
        namespace: str,
        removal_interval_seconds: float,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._redis = redis
        self._broadcaster = broadcaster
        self._repository = repository
        self._channel = f"{namespace}:mobility:updates"
        self._removal_interval = removal_interval_seconds
        self._clock = clock or (lambda: datetime.now(tz=UTC))

    async def handle_payload(self, payload: str | bytes) -> None:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")

        try:
            remove = MobilityCellRemove.model_validate_json(payload)
        except Exception:
            remove = None

        if remove is not None:
            await self._broadcaster.publish(remove)
            return

        cell = MobilityCell.model_validate_json(payload)
        await self._broadcaster.publish(MobilityCellUpsert(sent_at=self._clock(), cells=[cell]))

    async def publish_expired(self, now: datetime) -> None:
        removed = await self._repository.remove_expired(now)
        if removed:
            await self._broadcaster.publish(MobilityCellRemove(sent_at=now, cell_ids=removed))

    async def run_updates(self) -> None:
        pubsub = self._redis.pubsub()  # type: ignore[attr-defined]
        await pubsub.subscribe(self._channel)
        try:
            async for message in pubsub.listen():
                if message.get("type") == "message":
                    await self.handle_payload(message["data"])
        finally:
            await pubsub.aclose()

    async def run_expiry(self) -> None:
        while True:
            await self.publish_expired(self._clock())
            await asyncio.sleep(self._removal_interval)

    async def run(self) -> None:
        with suppress(asyncio.CancelledError):
            async with asyncio.TaskGroup() as group:
                group.create_task(self.run_updates())
                group.create_task(self.run_expiry())
