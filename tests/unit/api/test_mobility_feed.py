from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_api.mobility_feed import MobilityFeed
from aether_contracts import MobilityCellRemove

NOW = datetime(2026, 9, 24, 9, 10, tzinfo=UTC)
CELL = "841f1d7ffffffff"


class RecordingBroadcaster:
    def __init__(self) -> None:
        self.queue: list[object] = []

    async def publish(self, message: object) -> None:
        self.queue.append(message)


class ExpiringRepository:
    async def remove_expired(self, _now: datetime) -> list[str]:
        return [CELL]


@pytest.mark.asyncio
async def test_feed_forwards_typed_removal() -> None:
    broadcaster = RecordingBroadcaster()
    feed = MobilityFeed(
        redis=object(),
        broadcaster=broadcaster,
        repository=ExpiringRepository(),
        namespace="aether",
        removal_interval_seconds=1,
    )
    await feed.handle_payload(MobilityCellRemove(sent_at=NOW, cell_ids=[CELL]).model_dump_json())

    assert broadcaster.queue
    assert isinstance(broadcaster.queue[0], MobilityCellRemove)
    assert broadcaster.queue[0].cell_ids == [CELL]
