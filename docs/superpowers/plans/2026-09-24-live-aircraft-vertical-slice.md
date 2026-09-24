# Aether Live Aircraft Vertical Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a locally runnable path from anonymous OpenSky observations through Redpanda and Redis to a polished, smoothly animated Europe aircraft map.

**Architecture:** Three independently restartable Python processes share versioned Pydantic contracts: the ingestor publishes normalized events to Redpanda, the state builder creates an idempotent Redis projection, and FastAPI serves snapshots and projection notifications. A Next.js client loads a snapshot, consumes WebSocket deltas, and renders bounded interpolation through one MapLibre GeoJSON layer.

**Tech Stack:** Python 3.13, FastAPI 0.141.1, Pydantic 2, httpx, confluent-kafka, redis-py asyncio, prometheus-client, structlog, pytest; Node.js 22, Next.js App Router, React, TypeScript, MapLibre GL JS 6.11.x, Vitest, Testing Library, Playwright; Redpanda, Redis 7, Docker Compose, Prometheus.

**Spec:** `docs/superpowers/specs/2026-09-24-live-aircraft-vertical-slice-design.md`

## Global Constraints

- Anonymous OpenSky is the zero-credential default and is polled no faster than every 10 seconds.
- Real source values, client-derived interpolation, fixture test data, and future simulated data must remain explicitly distinguishable.
- Automated tests must never call OpenSky; use `tests/fixtures/opensky/states-europe.json` or an injected fake transport.
- Redpanda topic name is `aircraft.position.v1`; ICAO24 is the message key.
- Redis is a rebuildable latest-state projection, not the authoritative event history.
- JSON payloads use UTC RFC 3339 timestamps, `schema_version: 1`, and strict boundary validation.
- The browser must stop interpolation at its configured horizon; it must never silently start a synthetic feed.
- Later Aether phases consume stable contracts but are not implemented in this plan.
- Python formatting/linting uses Ruff; Python typing uses mypy; frontend linting uses ESLint and TypeScript `--noEmit`.
- Runtime images and lockfiles pin resolved dependency versions; dependency updates are separate reviewed changes.

## Review Focus

1. A malformed row inside an otherwise valid OpenSky response must be rejected and counted without discarding valid sibling rows; Task 2 adds this test.
2. Equal or older observation timestamps from retries or multiple partitions must never overwrite newer Redis state; Task 4 tests the atomic Lua result codes.
3. A slow WebSocket client must not create an unbounded per-client buffer or block healthy clients; Task 6 tests bounded queue eviction.
4. Interpolation across longitude `179` to `-179` and heading `359` to `1` must follow the shortest path; Task 8 tests both wraparounds.
5. Failure to load the external map style must expose a usable error state while source and stream status remain legible; Task 9 adds a component test and Playwright assertion.

---

## File Map

```text
pyproject.toml                         Python workspace, tooling, scripts
uv.lock                               exact Python dependency lock
package.json / package-lock.json      root frontend command delegation and locks
.env.example                          documented non-secret runtime configuration
Makefile                              stable developer commands
packages/aether-contracts/            canonical Pydantic event/API models
services/ingestor/                    OpenSky adapter and Redpanda producer loop
services/state-builder/               Kafka consumer and atomic Redis projection
services/api/                         FastAPI snapshot, health, metrics, WebSocket
contracts/asyncapi.yaml               durable and transient message contract
web/app/                              page, layout, global visual tokens
web/components/                       map and operational overlays
web/lib/                              TypeScript contracts, stream store, interpolation
tests/unit/                            Python behavior tests
tests/contract/                        schema examples and AsyncAPI checks
tests/integration/                     real Redpanda/Redis/API path
tests/fixtures/opensky/                recorded, attributed provider fixtures
tests/e2e/                             browser smoke journey
infra/prometheus/                      local scrape configuration
docs/architecture.md                  implemented system and provenance
docs/adr/                              decisions for transport, projection, contracts, map
```

## Task 1: Establish the Workspace and Canonical Contracts

**Files:**
- Create: `pyproject.toml`
- Create: `.python-version`
- Create: `.gitignore`
- Create: `packages/aether-contracts/src/aether_contracts/__init__.py`
- Create: `packages/aether-contracts/src/aether_contracts/aircraft.py`
- Create: `packages/aether-contracts/src/aether_contracts/api.py`
- Create: `tests/unit/contracts/test_aircraft.py`
- Create: `tests/contract/examples/aircraft-position.v1.json`

**Interfaces:**
- Produces: `AircraftPositionEvent`, `AircraftState`, `ProjectedAircraft`, `Position`, `Motion`, `SourceStatus`, `AircraftSnapshot`, and WebSocket message models.
- Produces: `deterministic_event_id(source: str, icao24: str, observed_at: datetime) -> str`.
- Consumes: no project interfaces.

- [ ] **Step 1: Write failing contract tests**

```python
def test_event_normalizes_identity_and_serializes_utc() -> None:
    event = AircraftPositionEvent.model_validate(EXAMPLE)
    assert event.aircraft.icao24 == "3c6444"
    assert event.aircraft.callsign == "DLH4YA"
    assert event.schema_version == 1
    assert event.model_dump(mode="json")["observed_at"].endswith("Z")

def test_event_id_is_stable() -> None:
    observed = datetime(2026, 9, 24, 9, 10, tzinfo=UTC)
    assert deterministic_event_id("opensky", "3c6444", observed) == deterministic_event_id(
        "opensky", "3C6444", observed
    )

@pytest.mark.parametrize("icao24", ["", "XYZ123", "abc12", "abcdef0"])
def test_invalid_icao24_is_rejected(icao24: str) -> None:
    with pytest.raises(ValidationError):
        AircraftState(icao24=icao24, origin_country="Unknown")
```

- [ ] **Step 2: Run the contract tests and verify RED**

Run: `python -m pytest tests/unit/contracts/test_aircraft.py -q`  
Expected: FAIL because `aether_contracts` does not exist.

- [ ] **Step 3: Add workspace tooling and minimal typed models**

```python
class Position(BaseModel):
    latitude: Annotated[float, Field(ge=-90, le=90)]
    longitude: Annotated[float, Field(ge=-180, le=180)]
    barometric_altitude_m: float | None = None
    on_ground: bool

class AircraftPositionEvent(BaseModel):
    event_id: str
    event_type: Literal["aircraft.position"] = "aircraft.position"
    schema_version: Literal[1] = 1
    observed_at: AwareDatetime
    ingested_at: AwareDatetime
    source: Literal["opensky"] = "opensky"
    aircraft: AircraftState

class ProjectedAircraft(AircraftState):
    event_id: str
    observed_at: AwareDatetime
```

Use a SHA-256 digest truncated to 26 lowercase hex characters for deterministic IDs. Configure `pyproject.toml` with Python `>=3.13,<3.14`, editable workspace packages, pytest asyncio strict mode, Ruff, and mypy strict mode.

- [ ] **Step 4: Add API and stream models**

```python
class SourceStatus(BaseModel):
    source: Literal["opensky"]
    status: Literal["live", "stale", "offline"]
    last_observation_at: AwareDatetime | None
    age_seconds: float | None

class AircraftSnapshot(BaseModel):
    generated_at: AwareDatetime
    source_status: SourceStatus
    aircraft: list[ProjectedAircraft]

StreamMessage = Annotated[
    StreamReady | AircraftUpsert | AircraftRemove | Heartbeat,
    Field(discriminator="message_type"),
]
```

- [ ] **Step 5: Run focused quality checks and verify GREEN**

Run: `python -m pytest tests/unit/contracts/test_aircraft.py -q && ruff check packages/aether-contracts tests/unit/contracts && mypy packages/aether-contracts`  
Expected: PASS with no lint or typing errors.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml .python-version .gitignore packages/aether-contracts tests/unit/contracts tests/contract/examples
git commit -m "feat: define aircraft event contracts"
```

## Task 2: Normalize Anonymous OpenSky Responses

**Files:**
- Create: `services/ingestor/src/aether_ingestor/__init__.py`
- Create: `services/ingestor/src/aether_ingestor/config.py`
- Create: `services/ingestor/src/aether_ingestor/opensky.py`
- Create: `services/ingestor/src/aether_ingestor/normalizer.py`
- Create: `tests/fixtures/opensky/states-europe.json`
- Create: `tests/unit/ingestor/test_normalizer.py`
- Create: `tests/unit/ingestor/test_opensky.py`

**Interfaces:**
- Consumes: Task 1 `AircraftPositionEvent` and `deterministic_event_id`.
- Produces: `OpenSkyClient.fetch_states(bbox: BoundingBox) -> OpenSkyResponse`.
- Produces: `normalize_response(response: OpenSkyResponse, ingested_at: datetime) -> NormalizationResult` where the result contains `events` and row-level `rejections`.

- [ ] **Step 1: Write failing normalization tests**

```python
def test_normalizes_fixture_and_preserves_nullable_values(load_fixture) -> None:
    raw = load_fixture("opensky/states-europe.json")
    result = normalize_response(OpenSkyResponse.model_validate(raw), FIXED_NOW)
    assert result.events[0].aircraft.icao24 == "3c6444"
    assert result.events[0].aircraft.callsign == "DLH4YA"
    assert result.events[0].aircraft.position_source == "ads_b"
    assert result.events[1].aircraft.callsign is None

def test_bad_row_does_not_discard_valid_siblings() -> None:
    response = OpenSkyResponse(time=FIXED_EPOCH, states=[VALID_ROW, ["bad"]])
    result = normalize_response(response, FIXED_NOW)
    assert len(result.events) == 1
    assert result.rejections == [RowRejection(index=1, reason="state vector has 1 fields; expected at least 17")]
```

- [ ] **Step 2: Verify normalization tests fail**

Run: `python -m pytest tests/unit/ingestor/test_normalizer.py -q`  
Expected: FAIL because the ingestor package is missing.

- [ ] **Step 3: Implement index-safe normalization**

```python
OPEN_SKY_FIELDS = {
    "icao24": 0, "callsign": 1, "origin_country": 2, "time_position": 3,
    "last_contact": 4, "longitude": 5, "latitude": 6,
    "baro_altitude": 7, "on_ground": 8, "velocity": 9,
    "true_track": 10, "vertical_rate": 11, "position_source": 16,
}

def normalize_response(response: OpenSkyResponse, ingested_at: datetime) -> NormalizationResult:
    events: list[AircraftPositionEvent] = []
    rejections: list[RowRejection] = []
    for index, row in enumerate(response.states or []):
        try:
            events.append(normalize_row(row, ingested_at))
        except (ValueError, ValidationError) as exc:
            rejections.append(RowRejection(index=index, reason=str(exc)))
    return NormalizationResult(events=events, rejections=rejections)
```

- [ ] **Step 4: Write failing HTTP adapter tests**

```python
async def test_fetch_uses_bbox_and_no_auth_by_default() -> None:
    transport = httpx.MockTransport(assert_anonymous_request)
    response = await OpenSkyClient(settings(), transport=transport).fetch_states(EUROPE)
    assert response.time == 1_798_000_000

async def test_rate_limit_exposes_retry_after() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(429, headers={"X-Rate-Limit-Retry-After-Seconds": "37"}))
    with pytest.raises(OpenSkyRateLimited) as error:
        await OpenSkyClient(settings(), transport=transport).fetch_states(EUROPE)
    assert error.value.retry_after_seconds == 37
```

- [ ] **Step 5: Implement the anonymous client and typed settings**

```python
class OpenSkyClient:
    async def fetch_states(self, bbox: BoundingBox) -> OpenSkyResponse:
        response = await self._client.get(
            "/states/all",
            params={"lamin": bbox.min_lat, "lomin": bbox.min_lon,
                    "lamax": bbox.max_lat, "lomax": bbox.max_lon},
        )
        if response.status_code == 429:
            raise OpenSkyRateLimited(parse_retry_after(response.headers))
        response.raise_for_status()
        return OpenSkyResponse.model_validate(response.json())
```

Reject configured poll intervals below 10 seconds in `IngestorSettings`. Reserve optional client ID/secret fields without performing OAuth in this task.

- [ ] **Step 6: Run and commit**

Run: `python -m pytest tests/unit/ingestor -q && ruff check services/ingestor tests/unit/ingestor && mypy services/ingestor`  
Expected: PASS.

```bash
git add services/ingestor tests/fixtures/opensky tests/unit/ingestor
git commit -m "feat: normalize anonymous OpenSky observations"
```

## Task 3: Publish the Ingestion Stream Reliably

**Files:**
- Create: `services/ingestor/src/aether_ingestor/producer.py`
- Create: `services/ingestor/src/aether_ingestor/metrics.py`
- Create: `services/ingestor/src/aether_ingestor/logging.py`
- Create: `services/ingestor/src/aether_ingestor/main.py`
- Create: `tests/unit/ingestor/test_producer.py`
- Create: `tests/unit/ingestor/test_poll_loop.py`

**Interfaces:**
- Consumes: Task 2 client and normalizer.
- Produces: `EventProducer.publish(event: AircraftPositionEvent) -> Awaitable[None]`.
- Produces: `run_poll_cycle(client, producer, clock) -> PollResult` and `run_forever(...) -> None`.

- [ ] **Step 1: Write failing producer and poll-loop tests**

```python
async def test_publish_keys_event_by_icao24() -> None:
    broker = FakeBroker()
    await EventProducer(broker, topic="aircraft.position.v1").publish(EVENT)
    assert broker.records[0].key == b"3c6444"
    assert json.loads(broker.records[0].value)["schema_version"] == 1

async def test_cycle_publishes_each_valid_event_once() -> None:
    result = await run_poll_cycle(FakeOpenSky(RESPONSE), FakeProducer(), FixedClock())
    assert result.fetched == 3
    assert result.published == 2
    assert result.rejected == 1
```

- [ ] **Step 2: Verify RED**

Run: `python -m pytest tests/unit/ingestor/test_producer.py tests/unit/ingestor/test_poll_loop.py -q`  
Expected: FAIL for missing producer and loop.

- [ ] **Step 3: Implement acknowledged publishing**

```python
async def publish(self, event: AircraftPositionEvent) -> None:
    loop = asyncio.get_running_loop()
    delivery = loop.create_future()
    self._producer.produce(
        self._topic,
        key=event.aircraft.icao24.encode(),
        value=event.model_dump_json().encode(),
        on_delivery=delivery_callback(loop, delivery),
    )
    self._producer.poll(0)
    await asyncio.wait_for(delivery, timeout=self._delivery_timeout)
```

Configure `acks=all`, idempotence, bounded message retries, JSON structlog, and Prometheus counters/histograms. Log row rejection metadata without logging the entire provider response.

- [ ] **Step 4: Implement backoff and quota-aware polling**

```python
except OpenSkyRateLimited as exc:
    delay = max(exc.retry_after_seconds, settings.poll_interval_seconds)
except (httpx.TransportError, KafkaException):
    delay = backoff.next_delay_with_jitter()
else:
    backoff.reset()
    delay = settings.poll_interval_seconds
await sleeper(delay)
```

Add tests with injected `sleeper` and deterministic jitter proving `429` guidance is honored and transient failures never activate fixture mode.

- [ ] **Step 5: Run and commit**

Run: `python -m pytest tests/unit/ingestor -q && ruff check services/ingestor && mypy services/ingestor`  
Expected: PASS.

```bash
git add services/ingestor tests/unit/ingestor
git commit -m "feat: publish aircraft events to Redpanda"
```

## Task 4: Build an Atomic Redis Latest-State Projection

**Files:**
- Create: `services/state-builder/src/aether_state_builder/__init__.py`
- Create: `services/state-builder/src/aether_state_builder/config.py`
- Create: `services/state-builder/src/aether_state_builder/projection.py`
- Create: `services/state-builder/src/aether_state_builder/consumer.py`
- Create: `services/state-builder/src/aether_state_builder/main.py`
- Create: `tests/unit/state_builder/test_projection.py`
- Create: `tests/unit/state_builder/test_consumer.py`
- Create: `tests/integration/compose.dependencies.yaml`

**Interfaces:**
- Consumes: `AircraftPositionEvent` from Task 1 and topic records from Task 3.
- Produces: `AircraftProjection.apply(event) -> ApplyResult` with `accepted`, `duplicate`, `stale`, or `coordinate_missing`.
- Produces Redis keys `aether:aircraft:{icao24}`, `aether:aircraft:index`, `aether:source:opensky`, and Pub/Sub channel `aether:aircraft:updates`.

- [ ] **Step 1: Write failing projection tests**

```python
async def test_newer_event_replaces_projection(redis) -> None:
    assert await projection.apply(EVENT_100) is ApplyResult.ACCEPTED
    assert await projection.apply(EVENT_110) is ApplyResult.ACCEPTED
    assert (await projection.get("3c6444")).observed_at == EVENT_110.observed_at

async def test_equal_and_older_events_do_not_regress_state(redis) -> None:
    assert await projection.apply(EVENT_110) is ApplyResult.ACCEPTED
    assert await projection.apply(EVENT_110) is ApplyResult.DUPLICATE
    assert await projection.apply(EVENT_100) is ApplyResult.STALE
    assert (await projection.get("3c6444")).event_id == EVENT_110.event_id
```

- [ ] **Step 2: Verify RED against a test Redis**

Run: `docker compose -f tests/integration/compose.dependencies.yaml up -d redis && python -m pytest tests/unit/state_builder/test_projection.py -q`  
Expected: FAIL because the projection is missing.

- [ ] **Step 3: Implement the atomic Lua projection**

```lua
local current = redis.call('HGET', KEYS[1], 'observed_at_epoch_ms')
if current and tonumber(current) > tonumber(ARGV[1]) then return 2 end
if current and tonumber(current) == tonumber(ARGV[1]) then return 1 end
redis.call('HSET', KEYS[1],
  'observed_at_epoch_ms', ARGV[1], 'event_id', ARGV[2], 'payload', ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[4])
redis.call('PUBLISH', KEYS[3], ARGV[3])
return 0
```

Map Lua results to `ApplyResult`; skip the aircraft map key for missing coordinates while still updating source freshness and metrics.

- [ ] **Step 4: Write and implement consumer acknowledgement tests**

```python
async def test_offset_is_committed_only_after_projection_success() -> None:
    consumer = FakeConsumer([RECORD])
    await consume_one(consumer, SuccessfulProjection())
    assert consumer.committed == [RECORD]

async def test_invalid_payload_is_counted_and_committed() -> None:
    consumer = FakeConsumer([MALFORMED_RECORD])
    result = await consume_one(consumer, projection)
    assert result is ConsumeResult.REJECTED
    assert consumer.committed == [MALFORMED_RECORD]
```

Retry Redis connection failures without committing; commit schema-invalid messages after logging an error and incrementing `projection_rejected_total` so poison messages do not halt the partition.

- [ ] **Step 5: Run and commit**

Run: `python -m pytest tests/unit/state_builder -q && ruff check services/state-builder && mypy services/state-builder`  
Expected: PASS.

```bash
git add services/state-builder tests/unit/state_builder tests/integration/compose.dependencies.yaml
git commit -m "feat: project latest aircraft state into Redis"
```

## Task 5: Serve Snapshot, Health, and Metrics APIs

**Files:**
- Create: `services/api/src/aether_api/__init__.py`
- Create: `services/api/src/aether_api/config.py`
- Create: `services/api/src/aether_api/repository.py`
- Create: `services/api/src/aether_api/routes.py`
- Create: `services/api/src/aether_api/metrics.py`
- Create: `services/api/src/aether_api/app.py`
- Create: `tests/unit/api/test_routes.py`
- Create: `tests/unit/api/test_repository.py`

**Interfaces:**
- Consumes: Redis keys from Task 4 and snapshot models from Task 1.
- Produces: `create_app(settings, repository) -> FastAPI`.
- Produces: `GET /api/v1/aircraft`, `/health/live`, `/health/ready`, and `/metrics`.

- [ ] **Step 1: Write failing snapshot and freshness tests**

```python
def test_snapshot_excludes_removed_but_keeps_stale_aircraft(client, clock) -> None:
    response = client.get("/api/v1/aircraft")
    assert response.status_code == 200
    assert [item["icao24"] for item in response.json()["aircraft"]] == ["3c6444"]
    assert response.json()["source_status"]["status"] == "stale"

def test_ready_is_503_when_redis_is_unavailable(client_with_failed_repository) -> None:
    response = client_with_failed_repository.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
```

- [ ] **Step 2: Verify RED**

Run: `python -m pytest tests/unit/api/test_routes.py tests/unit/api/test_repository.py -q`  
Expected: FAIL because `aether_api` is absent.

- [ ] **Step 3: Implement repository and status thresholds**

```python
async def snapshot(self, now: datetime) -> AircraftSnapshot:
    minimum_ms = epoch_ms(now - timedelta(seconds=self._removal_seconds))
    ids = await self._redis.zrangebyscore(self._index_key, minimum_ms, "+inf")
    aircraft = await self._load_states(ids)
    source = await self._load_source_status(now)
    return AircraftSnapshot(generated_at=now, source_status=source, aircraft=aircraft)
```

Derive `live`, `stale`, and `offline` from explicit settings. Bound snapshot results with `MAX_SNAPSHOT_AIRCRAFT` and return aircraft in ICAO24 order for deterministic contracts.

- [ ] **Step 4: Implement FastAPI routes and metrics**

```python
@router.get("/api/v1/aircraft", response_model=AircraftSnapshot)
async def aircraft_snapshot(repository: AircraftRepository = Depends(get_repository)):
    return await repository.snapshot(utc_now())

@router.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "alive"}
```

Expose Prometheus ASGI output at `/metrics`; record snapshot duration, output size, Redis errors, and source age.

- [ ] **Step 5: Run and commit**

Run: `python -m pytest tests/unit/api -q && ruff check services/api && mypy services/api`  
Expected: PASS.

```bash
git add services/api tests/unit/api
git commit -m "feat: expose aircraft snapshot and health APIs"
```

## Task 6: Stream Bounded WebSocket Updates

**Files:**
- Create: `services/api/src/aether_api/broadcaster.py`
- Create: `services/api/src/aether_api/websocket.py`
- Modify: `services/api/src/aether_api/app.py`
- Create: `tests/unit/api/test_broadcaster.py`
- Create: `tests/unit/api/test_websocket.py`

**Interfaces:**
- Consumes: Redis Pub/Sub notifications from Task 4 and stream models from Task 1.
- Produces: `WS /api/v1/stream/aircraft` and `AircraftBroadcaster` lifecycle methods.

- [ ] **Step 1: Write failing bounded-buffer tests**

```python
async def test_slow_client_is_evicted_when_queue_is_full() -> None:
    broadcaster = AircraftBroadcaster(queue_size=2)
    client_id, queue = broadcaster.subscribe()
    await broadcaster.publish(UPSERT_1)
    await broadcaster.publish(UPSERT_2)
    await broadcaster.publish(UPSERT_3)
    assert client_id not in broadcaster.client_ids
    assert broadcaster.evictions == 1

def test_websocket_starts_with_ready_message(test_client) -> None:
    with test_client.websocket_connect("/api/v1/stream/aircraft") as socket:
        assert socket.receive_json()["message_type"] == "stream.ready"
```

- [ ] **Step 2: Verify RED**

Run: `python -m pytest tests/unit/api/test_broadcaster.py tests/unit/api/test_websocket.py -q`  
Expected: FAIL for missing broadcaster.

- [ ] **Step 3: Implement one Redis subscription and bounded fan-out**

```python
def subscribe(self) -> tuple[UUID, asyncio.Queue[StreamMessage]]:
    client_id = uuid4()
    self._clients[client_id] = asyncio.Queue(maxsize=self._queue_size)
    return client_id, self._clients[client_id]

async def publish(self, message: StreamMessage) -> None:
    for client_id, queue in list(self._clients.items()):
        try:
            queue.put_nowait(message)
        except asyncio.QueueFull:
            self.unsubscribe(client_id)
            self.evictions += 1
```

One application task listens to Redis; do not create one Redis subscription per browser. Batch upserts for at most 100 milliseconds or 250 aircraft, whichever occurs first.

- [ ] **Step 4: Implement ready, heartbeat, cleanup, and origin validation**

```python
@router.websocket("/api/v1/stream/aircraft")
async def aircraft_stream(socket: WebSocket) -> None:
    validate_origin(socket.headers.get("origin"), settings.allowed_origins)
    await socket.accept()
    await socket.send_json(StreamReady(sent_at=utc_now()).model_dump(mode="json"))
    client_id, queue = broadcaster.subscribe()
    try:
        await send_messages_and_heartbeats(socket, queue, settings.heartbeat_seconds)
    finally:
        broadcaster.unsubscribe(client_id)
```

- [ ] **Step 5: Run and commit**

Run: `python -m pytest tests/unit/api -q && ruff check services/api && mypy services/api`  
Expected: PASS.

```bash
git add services/api tests/unit/api
git commit -m "feat: stream live aircraft projection updates"
```

## Task 7: Publish AsyncAPI and Cross-Language Contract Fixtures

**Files:**
- Create: `contracts/asyncapi.yaml`
- Create: `tests/contract/test_asyncapi.py`
- Create: `web/lib/contracts.ts`
- Create: `web/lib/contracts.test.ts`
- Create: `web/package.json`
- Create: `web/tsconfig.json`
- Create: `web/vitest.config.ts`

**Interfaces:**
- Consumes: Task 1 models and canonical JSON examples.
- Produces: checked-in AsyncAPI 3 document and discriminated TypeScript stream types.

- [ ] **Step 1: Write failing schema and TypeScript fixture tests**

```python
def test_asyncapi_defines_topic_and_all_stream_messages(asyncapi_document) -> None:
    assert "aircraft.position.v1" in asyncapi_document["channels"]
    schemas = asyncapi_document["components"]["schemas"]
    assert {"AircraftPositionEvent", "StreamReady", "AircraftUpsert", "AircraftRemove", "Heartbeat"} <= schemas.keys()
```

```typescript
it("parses the canonical aircraft event", () => {
  const event = parseAircraftPositionEvent(fixture);
  expect(event.aircraft.icao24).toBe("3c6444");
  expect(event.schema_version).toBe(1);
});
```

- [ ] **Step 2: Verify RED in both runtimes**

Run: `python -m pytest tests/contract/test_asyncapi.py -q && npm --prefix web test -- contracts.test.ts`  
Expected: FAIL because the document and frontend package do not exist.

- [ ] **Step 3: Add explicit schemas and discriminated TypeScript types**

```typescript
export type StreamMessage =
  | StreamReady
  | AircraftUpsert
  | AircraftRemove
  | Heartbeat;

export function assertStreamMessage(value: unknown): StreamMessage {
  if (!isRecord(value) || typeof value.message_type !== "string") {
    throw new ContractError("stream message has no message_type");
  }
  return parseByMessageType(value);
}
```

AsyncAPI must describe the Kafka topic key, retained event message, WebSocket channel, and all required/nullable fields. Do not generate TypeScript during ordinary builds; keep the small public interface reviewed and test it against shared fixtures.

- [ ] **Step 4: Run and commit**

Run: `python -m pytest tests/contract -q && npm --prefix web test && npm --prefix web run typecheck`  
Expected: PASS.

```bash
git add contracts tests/contract web/package.json web/tsconfig.json web/vitest.config.ts web/lib/contracts.ts web/lib/contracts.test.ts
git commit -m "docs: publish aircraft streaming contracts"
```

## Task 8: Implement Client State, Reconnects, and Interpolation

**Files:**
- Create: `web/lib/aircraft-store.ts`
- Create: `web/lib/aircraft-store.test.ts`
- Create: `web/lib/interpolation.ts`
- Create: `web/lib/interpolation.test.ts`
- Create: `web/lib/aircraft-client.ts`
- Create: `web/lib/aircraft-client.test.ts`

**Interfaces:**
- Consumes: HTTP snapshot and `StreamMessage` from Tasks 5–7.
- Produces: `AircraftStore`, `interpolateAircraft(track, now, horizonMs)`, and `AircraftClient.start()/stop()`.

- [ ] **Step 1: Write failing reducer and removal tests**

```typescript
it("replaces state from a reconnect snapshot before applying deltas", () => {
  const store = new AircraftStore();
  store.replaceSnapshot(snapshot([A, B]));
  store.replaceSnapshot(snapshot([B]));
  store.apply(upsert([C]));
  expect(store.ids()).toEqual(["b00002", "c00003"]);
});

it("removes explicitly expired aircraft", () => {
  store.replaceSnapshot(snapshot([A]));
  store.apply(remove([A.icao24]));
  expect(store.ids()).toEqual([]);
});
```

- [ ] **Step 2: Write failing wraparound and bounded-horizon tests**

```typescript
it("uses shortest paths across longitude and heading wraparound", () => {
  const point = interpolateAircraft(track(at(179, 359), at(-179, 1)), MIDPOINT, 10_000);
  expect(Math.abs(Math.abs(point.longitude) - 180)).toBeLessThan(0.01);
  expect(point.track_degrees).toBeCloseTo(0);
});

it("freezes at the newest observation after the horizon", () => {
  expect(interpolateAircraft(TRACK, AFTER_HORIZON, 10_000)).toEqual(TRACK.current);
});
```

- [ ] **Step 3: Verify RED**

Run: `npm --prefix web test -- aircraft-store.test.ts interpolation.test.ts`  
Expected: FAIL for missing modules.

- [ ] **Step 4: Implement store and pure interpolation**

```typescript
const wrapDelta = (delta: number, period: number) =>
  ((((delta + period / 2) % period) + period) % period) - period / 2;

export function interpolateAngle(from: number, to: number, ratio: number): number {
  return (from + wrapDelta(to - from, 360) * ratio + 360) % 360;
}
```

Keep previous/current observations per ICAO24, derive stale state from server time, and expose a subscription API compatible with `useSyncExternalStore`.

- [ ] **Step 5: Implement snapshot-first reconnect behavior**

```typescript
async connect(): Promise<void> {
  const snapshot = await this.fetchSnapshot(this.abort.signal);
  this.store.replaceSnapshot(snapshot);
  const socket = this.openSocket(this.wsUrl);
  socket.onmessage = (event) => this.store.apply(assertStreamMessage(JSON.parse(event.data)));
  socket.onclose = () => this.scheduleReconnect(jitter(this.retryAttempt++));
}
```

Every reconnect fetches a fresh snapshot before accepting deltas. Abort fetches, close sockets, and clear retry timers in `stop()`.

- [ ] **Step 6: Run and commit**

Run: `npm --prefix web test && npm --prefix web run typecheck`  
Expected: PASS.

```bash
git add web/lib
git commit -m "feat: manage and interpolate live aircraft state"
```

## Task 9: Build the Operational Europe Map Interface

**Files:**
- Create: `web/app/layout.tsx`
- Create: `web/app/page.tsx`
- Create: `web/app/globals.css`
- Create: `web/components/airspace-map.tsx`
- Create: `web/components/aircraft-detail.tsx`
- Create: `web/components/source-health.tsx`
- Create: `web/components/operations-shell.tsx`
- Create: `web/components/operations-shell.test.tsx`
- Create: `web/public/aircraft.svg`
- Create: `web/next.config.ts`
- Create: `web/eslint.config.mjs`

**Interfaces:**
- Consumes: Task 8 store/client and MapLibre style URL configuration.
- Produces: full-viewport responsive interface and one MapLibre aircraft source/layer.

- [ ] **Step 1: Write failing component tests**

```tsx
it("keeps telemetry status usable when the map style fails", async () => {
  render(<OperationsShell mapFactory={failingMapFactory} client={fakeClient} />);
  expect(await screen.findByText("Map unavailable")).toBeVisible();
  expect(screen.getByText("OpenSky")).toBeVisible();
  expect(screen.getByText("Stream connected")).toBeVisible();
});

it("opens aircraft details from a map selection", async () => {
  render(<OperationsShell mapFactory={fakeMapFactory} client={fakeClient} />);
  fakeMapFactory.select("3c6444");
  expect(await screen.findByText("DLH4YA")).toBeVisible();
});
```

- [ ] **Step 2: Verify RED**

Run: `npm --prefix web test -- operations-shell.test.tsx`  
Expected: FAIL because the UI is absent.

- [ ] **Step 3: Implement visual tokens and accessible shell**

```css
:root {
  --night-flight: #07131d;
  --airspace-blue: #10283a;
  --track-cyan: #64d8e8;
  --amber-vector: #f2b84b;
  --fault-coral: #f26b5e;
  --cloud-white: #dce8ec;
}

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { scroll-behavior: auto !important; transition: none !important; }
}
```

Use IBM Plex Sans through a self-hosted package or checked-in font assets with license; do not make page rendering depend on Google Fonts. Add skip navigation, visible focus, tabular telemetry numerals, responsive overlays, and semantic live-status announcements that do not spam screen readers.

- [ ] **Step 4: Implement the shared GeoJSON layer and animation loop**

```typescript
map.addSource("aircraft", { type: "geojson", data: emptyFeatureCollection });
map.addLayer({
  id: "aircraft-live",
  type: "symbol",
  source: "aircraft",
  layout: {
    "icon-image": "aircraft-glyph",
    "icon-rotate": ["get", "track_degrees"],
    "icon-rotation-alignment": "map",
    "icon-allow-overlap": true,
  },
  paint: { "icon-color": ["case", ["get", "stale"], "#F2B84B", "#64D8E8"] },
});
```

Update source data from one `requestAnimationFrame` loop, pause when the tab is hidden, disable interpolation under reduced motion, and clean up map/listeners on unmount. Surface map errors separately from data-source errors.

- [ ] **Step 5: Run visual quality checks**

Run: `npm --prefix web test && npm --prefix web run lint && npm --prefix web run typecheck && npm --prefix web run build`  
Expected: all commands PASS.

- [ ] **Step 6: Commit**

```bash
git add web/app web/components web/public web/next.config.ts web/eslint.config.mjs
git commit -m "feat: render the live European airspace map"
```

## Task 10: Prove the End-to-End Data Path

**Files:**
- Modify: `tests/integration/compose.dependencies.yaml`
- Create: `tests/integration/conftest.py`
- Create: `tests/integration/test_aircraft_pipeline.py`
- Create: `tests/e2e/aether-live-map.spec.ts`
- Create: `web/playwright.config.ts`
- Create: `services/ingestor/src/aether_ingestor/fixture_source.py`

**Interfaces:**
- Consumes: all backend and frontend interfaces.
- Produces: explicit `AETHER_SOURCE_MODE=fixture` test-only source selected by configuration; production default remains `opensky`.

- [ ] **Step 1: Write the failing integration journey**

```python
async def test_publish_project_snapshot_and_stream(pipeline) -> None:
    await pipeline.publish(EVENT_100)
    await pipeline.wait_until_projected("3c6444")
    assert (await pipeline.snapshot()).aircraft[0].event_id == EVENT_100.event_id

    async with pipeline.websocket() as socket:
        assert (await socket.receive_json())["message_type"] == "stream.ready"
        await pipeline.publish(EVENT_110)
        update = await socket.receive_until("aircraft.upsert")
        assert update["aircraft"][0]["event_id"] == EVENT_110.event_id

    await pipeline.publish(EVENT_100)
    assert (await pipeline.snapshot()).aircraft[0].event_id == EVENT_110.event_id
```

- [ ] **Step 2: Verify RED**

Run: `python -m pytest tests/integration/test_aircraft_pipeline.py -q`  
Expected: FAIL because dependency orchestration and the pipeline fixture are incomplete.

- [ ] **Step 3: Implement isolated integration orchestration**

Use random host ports and a per-run Redis namespace/consumer group. Wait on explicit health endpoints rather than sleeping. Ensure cleanup removes only containers and test keys labeled with the run ID.

```python
@pytest.fixture
async def pipeline(compose_stack, run_id):
    harness = PipelineHarness(compose_stack, namespace=f"test:{run_id}")
    await harness.wait_ready(timeout=30)
    yield harness
    await harness.cleanup()
```

- [ ] **Step 4: Write the failing browser smoke test**

```typescript
test("shows and updates a fixture aircraft", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Stream connected")).toBeVisible();
  await expect(page.getByTestId("aircraft-count")).toHaveText("1");
  await page.getByTestId("aircraft-3c6444").click();
  await expect(page.getByText("DLH4YA")).toBeVisible();
  await expect(page.getByTestId("observation-age")).not.toHaveText("No data");
});
```

- [ ] **Step 5: Add explicit fixture mode and complete both journeys**

Fixture mode reads the checked-in response and emits two timestamp-adjusted observations only when `AETHER_SOURCE_MODE=fixture`. Reject that value when `AETHER_ENVIRONMENT=production`. Add a browser assertion for the map-style failure banner using request interception.

- [ ] **Step 6: Run and commit**

Run: `python -m pytest tests/integration -q && npm --prefix web run test:e2e`  
Expected: PASS without any request to `opensky-network.org`.

```bash
git add tests/integration tests/e2e web/playwright.config.ts services/ingestor/src/aether_ingestor/fixture_source.py
git commit -m "test: verify the live aircraft vertical slice"
```

## Task 11: Package the Runnable Local Stack and Observability

**Files:**
- Create: `compose.yaml`
- Create: `.env.example`
- Create: `Makefile`
- Create: `docker/python.Dockerfile`
- Create: `web/Dockerfile`
- Create: `infra/prometheus/prometheus.yaml`
- Create: `infra/redpanda/create-topics.sh`
- Create: `tests/unit/test_compose_config.py`

**Interfaces:**
- Consumes: service entry points and configuration from Tasks 2–10.
- Produces: `docker compose up --build`, optional `observability` profile, `make test`, `make lint`, and `make smoke`.

- [ ] **Step 1: Write failing configuration assertions**

```python
def test_compose_has_health_checked_dependencies(compose_config) -> None:
    services = compose_config["services"]
    assert {"redpanda", "redis", "ingestor", "state-builder", "api", "web"} <= services.keys()
    assert services["state-builder"]["depends_on"]["redpanda"]["condition"] == "service_healthy"
    assert "healthcheck" in services["api"]

def test_env_example_contains_no_secret_values(env_example) -> None:
    assert "OPEN_SKY_CLIENT_SECRET=" in env_example
    assert not re.search(r"OPEN_SKY_CLIENT_SECRET=.+", env_example)
```

- [ ] **Step 2: Verify RED**

Run: `python -m pytest tests/unit/test_compose_config.py -q`  
Expected: FAIL because Compose does not exist.

- [ ] **Step 3: Add pinned images, health checks, topic setup, and volumes**

```yaml
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:${REDPANDA_VERSION}
    command: [redpanda, start, --mode, dev-container, --smp, "1"]
    healthcheck:
      test: [CMD-SHELL, rpk cluster health | grep -q 'Healthy:.*true']
  redis:
    image: redis:7-alpine
    healthcheck:
      test: [CMD, redis-cli, ping]
```

Use non-root runtime users, multi-stage builds, named local volumes, internal dependency networking, and expose only API, web, Redpanda developer access, and optional Prometheus ports. Topic setup must be idempotent and set explicit partitions and retention.

- [ ] **Step 4: Add stable developer commands and Prometheus profile**

```make
test:
	python -m pytest tests/unit tests/contract
	npm --prefix web test
lint:
	ruff check .
	mypy packages services
	npm --prefix web run lint
	npm --prefix web run typecheck
smoke:
	python -m pytest tests/integration
	npm --prefix web run test:e2e
```

- [ ] **Step 5: Validate and start the stack**

Run: `docker compose config --quiet && docker compose up --build -d && docker compose ps`  
Expected: configuration exit 0; every required service becomes healthy/running.

- [ ] **Step 6: Verify live local endpoints**

Run: `curl --fail http://localhost:8000/health/ready && curl --fail http://localhost:8000/metrics && curl --fail http://localhost:3000`  
Expected: all exit 0; readiness reports ready; Prometheus text and HTML are returned.

- [ ] **Step 7: Commit**

```bash
git add compose.yaml .env.example Makefile docker web/Dockerfile infra tests/unit/test_compose_config.py
git commit -m "build: package the local Aether stack"
```

## Task 12: Document Architecture, Decisions, and Operations

**Files:**
- Create: `README.md`
- Create: `docs/architecture.md`
- Create: `docs/adr/0001-use-redpanda-for-event-transport.md`
- Create: `docs/adr/0002-use-redis-as-rebuildable-projection.md`
- Create: `docs/adr/0003-version-event-contracts.md`
- Create: `docs/adr/0004-render-aircraft-with-maplibre.md`
- Create: `docs/data-provenance.md`
- Create: `tests/contract/test_documentation.py`

**Interfaces:**
- Consumes: actual commands, ports, keys, events, and configuration implemented by Tasks 1–11.
- Produces: newcomer runbook and explicit real/derived/simulated data documentation.

- [ ] **Step 1: Write failing documentation checks**

```python
def test_readme_commands_exist_in_makefile(repo_root) -> None:
    readme = (repo_root / "README.md").read_text()
    makefile = (repo_root / "Makefile").read_text()
    for command in ("make test", "make lint", "make smoke"):
        assert command in readme
        assert f"{command.removeprefix('make ')}:" in makefile

def test_provenance_names_real_and_derived_fields(repo_root) -> None:
    text = (repo_root / "docs/data-provenance.md").read_text()
    assert "Real: OpenSky" in text
    assert "Derived: interpolation" in text
    assert "Simulated: none in this slice" in text
```

- [ ] **Step 2: Verify RED**

Run: `python -m pytest tests/contract/test_documentation.py -q`  
Expected: FAIL because operator documentation is missing.

- [ ] **Step 3: Write the operator README and architecture guide**

README sections must include prerequisites, one-command startup, service URLs,
anonymous quota behavior, configuration, tests, logs/metrics, resetting only
the named Aether volumes, troubleshooting, provenance, and the strict phase
roadmap. `docs/architecture.md` must include the text data-flow diagram, service
ownership, Redis keys, topic contract, reconnect semantics, and future consumer
extension points.

```markdown
## Data provenance

- **Real:** OpenSky observations and their source timestamps.
- **Derived:** bounded browser interpolation and freshness classification.
- **Simulated:** none in this slice; fixture mode is test-only and visibly labeled.
```

- [ ] **Step 4: Record four accepted decisions**

Each ADR uses `Context`, `Decision`, `Consequences`, and `Alternatives` sections. Record why Redpanda was selected over Redis Streams, why Redis is disposable projection state, how versioned contracts permit future Aether consumers, and why MapLibre uses one GeoJSON layer instead of React markers.

- [ ] **Step 5: Run the complete verification suite**

Run: `make test && make lint && make smoke && docker compose config --quiet`  
Expected: every command exits 0; tests do not contact OpenSky.

- [ ] **Step 6: Review the live interface**

Run: `docker compose up --build -d` then open `http://localhost:3000` with the browser smoke tooling. Capture desktop and narrow-viewport screenshots; verify map dominance, selected-aircraft details, status legibility, responsive collapse, reduced motion, map attribution, and no generic dashboard-card treatment. Fix any discrepancy and rerun Task 9 tests plus `make smoke`.

- [ ] **Step 7: Commit**

```bash
git add README.md docs/architecture.md docs/adr docs/data-provenance.md tests/contract/test_documentation.py
git commit -m "docs: explain Aether architecture and local operations"
```

## Final Verification Gate

- [ ] Run `git status --short` and confirm only intended files are present.
- [ ] Run `make test` and record the passing Python/frontend test counts.
- [ ] Run `make lint` and record successful Ruff, mypy, ESLint, and TypeScript results.
- [ ] Run `make smoke` and record integration and Playwright results.
- [ ] Run `docker compose up --build -d`, wait for readiness, and record `docker compose ps`.
- [ ] Confirm `GET /api/v1/aircraft`, `/health/ready`, `/metrics`, and the web root respond successfully.
- [ ] Inspect JSON logs for all three Python processes and verify secrets/raw authorization headers are absent.
- [ ] Re-read the design spec section by section and record any unmet requirement before claiming completion.
