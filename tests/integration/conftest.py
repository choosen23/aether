from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio
from aether_api.app import create_app
from aether_api.config import ApiSettings
from aether_api.repository import AircraftRepository
from aether_contracts import AircraftSnapshot, AircraftUpsert, ProjectedAircraft
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
from redis import asyncio as redis_async


def _epoch_ms(value: datetime | str) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


@dataclass(slots=True)
class _WebSocketAdapter:
    socket: object

    async def receive_json(self) -> dict[str, object]:
        return await asyncio.to_thread(self.socket.receive_json)

    async def receive_until(self, message_type: str) -> dict[str, object]:
        while True:
            payload = await self.receive_json()
            if payload.get("message_type") == message_type:
                return payload


class PipelineHarness:
    def __init__(self, redis_url: str, namespace: str) -> None:
        self._namespace = namespace
        self._redis = redis_async.from_url(redis_url)
        settings = ApiSettings(
            redis_url=redis_url,
            redis_namespace=namespace,
            allowed_origins=["*"],
        )
        repository = AircraftRepository(self._redis, settings)
        self._app = create_app(settings, repository)
        self._http = AsyncClient(transport=ASGITransport(app=self._app), base_url="http://testserver")
        self._client = TestClient(self._app)
        self._latest_by_icao24: dict[str, ProjectedAircraft] = {}

    @property
    def _index_key(self) -> str:
        return f"{self._namespace}:aircraft:index"

    def _aircraft_key(self, icao24: str) -> str:
        return f"{self._namespace}:aircraft:{icao24}"

    @property
    def _source_key(self) -> str:
        return f"{self._namespace}:source:opensky"

    async def wait_ready(self, timeout: float = 30) -> None:
        await asyncio.wait_for(self._redis.ping(), timeout=timeout)

    async def publish(self, aircraft: ProjectedAircraft) -> None:
        current = self._latest_by_icao24.get(aircraft.icao24)
        if current is not None and _epoch_ms(aircraft.observed_at) <= _epoch_ms(
            current.observed_at
        ):
            return

        self._latest_by_icao24[aircraft.icao24] = aircraft
        observed_ms = _epoch_ms(aircraft.observed_at)
        await self._redis.hset(
            self._aircraft_key(aircraft.icao24),
            mapping={
                "observed_at_epoch_ms": observed_ms,
                "event_id": aircraft.event_id,
                "payload": aircraft.model_dump_json(),
            },
        )
        await self._redis.zadd(self._index_key, {aircraft.icao24: observed_ms})
        await self._redis.hset(
            self._source_key,
            mapping={
                "last_observation_epoch_ms": observed_ms,
                "event_id": aircraft.event_id,
            },
        )

        upsert = AircraftUpsert(
            sent_at=datetime.now(tz=UTC),
            aircraft=[aircraft],
        )
        await self._app.state.broadcaster.publish(upsert)

    async def wait_until_projected(self, icao24: str, timeout: float = 5.0) -> None:
        started = datetime.now(tz=UTC)
        while True:
            payload = await self._redis.hget(self._aircraft_key(icao24), "payload")
            if payload is not None:
                return
            if (datetime.now(tz=UTC) - started).total_seconds() > timeout:
                msg = f"timed out waiting for projection {icao24}"
                raise TimeoutError(msg)
            await asyncio.sleep(0.05)

    async def snapshot(self) -> AircraftSnapshot:
        response = await self._http.get("/api/v1/aircraft")
        if response.status_code != 200:
            msg = f"snapshot request failed: {response.status_code} {response.text}"
            raise RuntimeError(msg)
        return AircraftSnapshot.model_validate(response.json())

    @asynccontextmanager
    async def websocket(self):
        socket_cm = self._client.websocket_connect(
            "/api/v1/stream/aircraft",
            headers={"origin": "http://testserver"},
        )
        socket = await asyncio.to_thread(socket_cm.__enter__)
        try:
            yield _WebSocketAdapter(socket)
        finally:
            await asyncio.to_thread(socket_cm.__exit__, None, None, None)

    async def cleanup(self) -> None:
        keys = await self._redis.keys(f"{self._namespace}:*")
        if keys:
            await self._redis.delete(*keys)
        await self._http.aclose()
        await self._redis.aclose()
        await asyncio.to_thread(self._client.close)


@pytest_asyncio.fixture
async def pipeline() -> PipelineHarness:
    namespace = f"test:{uuid4().hex}"
    harness = PipelineHarness(redis_url="redis://localhost:6379/0", namespace=namespace)
    try:
        await harness.wait_ready(timeout=2)
    except Exception:
        pytest.skip("redis is not available for integration tests")
    yield harness
    await harness.cleanup()
