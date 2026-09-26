from __future__ import annotations

import pytest
from aether_contracts import SimulationEvent
from aether_simulator.publisher import SimulationPublisher


class FailingProducer:
    async def send_and_wait(self, topic: str, value: bytes, key: bytes | None = None) -> object:
        msg = f"failed to publish to {topic}"
        raise RuntimeError(msg)


class CapturingProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes | None]] = []

    async def send_and_wait(self, topic: str, value: bytes, key: bytes | None = None) -> object:
        self.sent.append((topic, value, key))
        return object()


COMMITTED_EVENT = SimulationEvent(
    run_id="run-1",
    event_id="evt-17",
    sequence=17,
    simulated_time_ms=100,
    event_type="execution_completed",
    payload={"workload_id": "w1"},
)


@pytest.mark.asyncio
async def test_publish_failure_does_not_change_engine_result() -> None:
    publisher = SimulationPublisher(FailingProducer())
    outcome = await publisher.publish(COMMITTED_EVENT)
    assert outcome.delivered is False
    assert COMMITTED_EVENT.sequence == 17


@pytest.mark.asyncio
async def test_publish_success_is_keyed_by_run_id() -> None:
    producer = CapturingProducer()
    publisher = SimulationPublisher(producer)
    outcome = await publisher.publish(COMMITTED_EVENT)
    assert outcome.delivered is True
    assert producer.sent[0][2] == b"run-1"
