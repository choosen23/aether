from __future__ import annotations

import pytest
from aether_contracts import SimulationEvent
from aether_state_builder.simulation_projection import SimulationApplyResult, SimulationProjection


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.streams: dict[str, list[dict[str, object]]] = {}
        self.xadd_count = 0

    async def hget(self, name: str, key: str) -> object | None:
        return self.hashes.get(name, {}).get(key)

    async def hset(self, name: str, mapping: dict[str, object]) -> object:
        self.hashes.setdefault(name, {}).update(mapping)
        return 1

    async def zadd(self, name: str, mapping: dict[str, float]) -> object:
        self.zsets.setdefault(name, {}).update(mapping)
        return 1

    async def xadd(self, name: str, fields: dict[str, object]) -> object:
        self.streams.setdefault(name, []).append(fields)
        self.xadd_count += 1
        return "1-0"


def _event(sequence: int) -> SimulationEvent:
    return SimulationEvent(
        run_id="run-1",
        event_id=f"evt-{sequence}",
        sequence=sequence,
        simulated_time_ms=sequence * 10,
        event_type="workload_arrived",
        payload={"sequence": sequence},
    )


@pytest.mark.asyncio
async def test_projection_ignores_duplicate_sequence() -> None:
    redis = FakeRedis()
    projection = SimulationProjection(redis, namespace="aether")

    assert await projection.apply(_event(4)) is SimulationApplyResult.ACCEPTED
    assert await projection.apply(_event(4)) is SimulationApplyResult.DUPLICATE

    run_key = "aether:simulation:run:run-1"
    assert await redis.hget(run_key, "last_sequence") == "4"
    assert redis.xadd_count == 1


@pytest.mark.asyncio
async def test_projection_rejects_out_of_order_sequence() -> None:
    redis = FakeRedis()
    projection = SimulationProjection(redis, namespace="aether")

    assert await projection.apply(_event(2)) is SimulationApplyResult.ACCEPTED
    assert await projection.apply(_event(4)) is SimulationApplyResult.OUT_OF_ORDER
    assert await projection.apply(_event(3)) is SimulationApplyResult.ACCEPTED


@pytest.mark.asyncio
async def test_projection_can_rebuild_from_event_history() -> None:
    redis = FakeRedis()
    projection = SimulationProjection(redis, namespace="aether")

    events = (_event(1), _event(2), _event(3))
    for event in events:
        assert await projection.apply(event) is SimulationApplyResult.ACCEPTED

    run_key = "aether:simulation:run:run-1"
    assert await redis.hget(run_key, "last_sequence") == "3"
    assert redis.zsets["aether:simulation:runs"]["run-1"] == 30.0
    assert len(redis.streams["aether:simulation:changes"]) == 3
