# Geographic Density and Demand Heatmap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert current aircraft positions into sparse H3 density and explicitly modelled demand events, project them through Redis and FastAPI, and render selectable live heatmap layers in the Aether control room.

**Architecture:** A new `density-builder` consumes `aircraft.position.v1`, retains one event-time-ordered position per aircraft for three minutes, and publishes changed H3 cells to `mobility.density.v1` and `demand.region.v1`. The existing state builder atomically joins matching density/demand windows into a Redis projection; a dedicated API snapshot/WebSocket and frontend store feed two mutually exclusive MapLibre polygon layers.

**Tech Stack:** Python 3.13, Pydantic 2, h3-py 4.x, aiokafka, Redis Lua, FastAPI, Prometheus, pytest; TypeScript, React, Next.js, h3-js 4.x, MapLibre GL JS, Vitest, Testing Library, Playwright; Redpanda and Docker Compose.

**Spec:** `docs/superpowers/specs/2026-09-24-geographic-demand-heatmap-design.md`

## Global Constraints

- Preserve `aircraft.position.v1`; Phase 3 is a new consumer, not an ingestion rewrite.
- Default anonymous OpenSky polling to 60 seconds; allow 10 seconds only as explicit short-demo configuration.
- Use event time, accept only strictly newer observations per aircraft, and count each aircraft at most once per emission.
- Use a 180-second freshness window, a 10-second local emission tick, and configurable H3 resolution 4 by default.
- Use 75 seconds for live source freshness and 180 seconds for offline/source expiry so the 60-second poll remains healthy between observations.
- Publish only changed occupied cells plus explicit removals; an emission tick must not contact OpenSky.
- Keep `mobility.density.v1` provenance `derived` and `demand.region.v1` provenance `modelled` in contracts, API payloads, and interface copy.
- Use `demand_units = airborne_count * 1.0 + on_ground_count * 0.35` for coefficient set `default-v1` and model `density-linear-v1`.
- Redpanda is retained history; Redis is a rebuildable latest-state projection.
- Automated tests use recorded fixtures or generated internal events and never call OpenSky.
- Do not add forecasts, workload classes, cloud regions, Kubernetes execution, controllers, replay, chaos, or MARL.
- Before execution, commit the already validated live-aircraft vertical-slice files as a clean baseline so Phase 3 commits do not absorb unrelated untracked work.

## Review Focus

1. A newer aircraft event that crosses a cell boundary must decrement the old cell and increment the new cell exactly once; Task 2 pins movement, duplicates, and equal timestamps.
2. Density and demand records can arrive on different partitions and in either order; Task 4 requires a test that publishes only when both records share the same window end.
3. An unchanged source period must not emit redundant cells, but the three-minute expiry tick must still emit removals; Task 2 tests both behaviors.
4. A density-builder restart must seek each aircraft partition to the window start before catching up, without retaining older ghost aircraft; Task 3 tests timestamp-offset selection and catch-up.
5. Invalid H3 indexes and antimeridian-crossing polygon coordinates must not crash the browser or poison the projection; Tasks 4 and 7 include rejection and geometry tests.

---

## File Map

```text
pyproject.toml                                      add h3-py and workspace package
packages/aether-contracts/src/aether_contracts/
  mobility.py                                      canonical density/demand models
  mobility_api.py                                  mobility snapshot and stream models
services/density-builder/
  pyproject.toml                                    workspace package metadata
  src/aether_density_builder/config.py              H3/window/model/broker settings
  src/aether_density_builder/aggregate.py           event-time working set and sparse diffs
  src/aether_density_builder/demand.py              deterministic demand formula
  src/aether_density_builder/producer.py            acknowledged topic publication
  src/aether_density_builder/consumer.py            validation, commits, replay seek
  src/aether_density_builder/metrics.py              service metrics
  src/aether_density_builder/main.py                 lifecycle and emission loop
services/state-builder/src/aether_state_builder/
  mobility_consumer.py                              parse both mobility topics
  mobility_projection.py                            atomic Redis join/removal projection
  config.py, main.py, metrics.py                     parallel consumer configuration/runtime
services/api/src/aether_api/
  mobility_repository.py                            mobility snapshot and expiry reads
  mobility_feed.py                                  one Redis subscription and removal fan-out
  mobility_broadcaster.py                           bounded client queues
  mobility_routes.py, mobility_websocket.py          HTTP and WebSocket boundaries
  app.py, config.py, metrics.py                      lifecycle wiring and configuration
web/lib/
  mobility-contracts.ts                             browser contract types and guards
  mobility-store.ts                                 snapshot/delta reducer
  mobility-client.ts                                snapshot-first reconnect client
  mobility-geometry.ts                              H3-to-GeoJSON conversion
  runtime-client.ts                                 combined aircraft/mobility runtime facade
web/components/
  layer-control.tsx                                 three-mode selector
  mobility-cell-detail.tsx                          selected-cell provenance/details
  operations-shell.tsx                              combined control-room state
web/lib/maplibre-map.ts                             polygon sources/layers and cell selection
contracts/asyncapi.yaml                             retained and transient channel schemas
infra/redpanda/create-topics.sh                     create both new retained topics
compose.yaml, .env.example                          service and settings wiring
tests/                                              unit, contract, integration, load fixtures
```

## Task 1: Define Canonical Mobility Contracts and Configuration

**Files:**
- Create: `packages/aether-contracts/src/aether_contracts/mobility.py`
- Create: `packages/aether-contracts/src/aether_contracts/mobility_api.py`
- Modify: `packages/aether-contracts/src/aether_contracts/__init__.py`
- Modify: `contracts/asyncapi.yaml`
- Modify: `pyproject.toml`
- Modify: `services/ingestor/src/aether_ingestor/config.py`
- Test: `tests/unit/contracts/test_mobility.py`
- Test: `tests/contract/examples/mobility-density.v1.json`
- Test: `tests/contract/examples/demand-region.v1.json`
- Test: `tests/unit/ingestor/test_config.py`

**Interfaces:**
- Consumes: existing UTC serialization conventions from `aether_contracts.aircraft`.
- Produces: `MobilityDensityEvent`, `MobilityDensityRemoved`, `RegionalDemandEvent`, `MobilityCell`, `MobilitySnapshot`, `MobilityCellUpsert`, `MobilityCellRemove`, `MobilityHeartbeat`, `MobilityStreamMessage`, and `deterministic_mobility_event_id(kind, cell_id, window_ended_at, variant) -> str`.

- [ ] **Step 1: Write failing contract and polling-default tests**

```python
def test_density_counts_are_consistent() -> None:
    with pytest.raises(ValidationError):
        MobilityDensityEvent(**DENSITY, aircraft_count=3, airborne_count=2, on_ground_count=2)

def test_contracts_preserve_provenance() -> None:
    assert MobilityDensityEvent.model_validate(DENSITY).provenance == "derived"
    assert RegionalDemandEvent.model_validate(DEMAND).provenance == "modelled"

def test_anonymous_polling_defaults_to_sixty_seconds() -> None:
    assert IngestorSettings().poll_interval_seconds == 60
    assert IngestorSettings(poll_interval_seconds=10).poll_interval_seconds == 10
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `uv run --group dev python -m pytest tests/unit/contracts/test_mobility.py tests/unit/ingestor/test_config.py -q`  
Expected: FAIL because the mobility models do not exist and the poll default is still 10.

- [ ] **Step 3: Add strict event models and discriminated API messages**

```python
class MobilityDensityEvent(BaseModel):
    event_id: str
    event_type: Literal["mobility.density"] = "mobility.density"
    schema_version: Literal[1] = 1
    generated_at: AwareDatetime
    window_started_at: AwareDatetime
    window_ended_at: AwareDatetime
    source_event_watermark: AwareDatetime
    cell_id: str
    h3_resolution: Annotated[int, Field(ge=0, le=15)]
    aircraft_count: Annotated[int, Field(ge=0)]
    airborne_count: Annotated[int, Field(ge=0)]
    on_ground_count: Annotated[int, Field(ge=0)]
    average_altitude_m: float | None
    average_ground_speed_mps: float | None
    provenance: Literal["derived"] = "derived"

    @model_validator(mode="after")
    def counts_match(self) -> Self:
        if self.airborne_count + self.on_ground_count != self.aircraft_count:
            raise ValueError("airborne_count plus on_ground_count must equal aircraft_count")
        return self

class RegionalDemandEvent(BaseModel):
    event_id: str
    event_type: Literal["demand.region"] = "demand.region"
    schema_version: Literal[1] = 1
    generated_at: AwareDatetime
    window_started_at: AwareDatetime
    window_ended_at: AwareDatetime
    cell_id: str
    h3_resolution: Annotated[int, Field(ge=0, le=15)]
    aircraft_count: Annotated[int, Field(ge=0)]
    demand_units: Annotated[float, Field(ge=0)]
    model_version: Literal["density-linear-v1"] = "density-linear-v1"
    coefficient_set: Literal["default-v1"] = "default-v1"
    provenance: Literal["modelled"] = "modelled"
```

Define `MobilityDensityRemoved(event_type="mobility.density.removed", cell_id, removed_at, last_window_ended_at)`. Define `MobilityCell(density, demand)`, `MobilitySnapshot(generated_at, source_status, cells)`, and WebSocket messages named in the Interfaces block, discriminated by `message_type`.
Implement `deterministic_mobility_event_id` as the first 26 lowercase hex
characters of SHA-256 over normalized kind, H3 cell, UTC window end, and model
variant joined by colons.

- [ ] **Step 4: Extend AsyncAPI and set the quota-conscious default**

Add channels `mobility.density.v1`, `demand.region.v1`, and `/api/v1/stream/mobility`, with examples referencing the two checked-in JSON fixtures. Set `IngestorSettings.poll_interval_seconds = 60`; retain the validator minimum of 10.

- [ ] **Step 5: Add h3-py and run contract quality checks**

Add `"h3>=4,<5"` to root dependencies, then run `uv lock`. Run: `uv run --group dev python -m pytest tests/unit/contracts/test_mobility.py tests/unit/ingestor/test_config.py tests/contract -q && uv run --group dev ruff check packages/aether-contracts tests/unit/contracts && uv run --group dev mypy packages/aether-contracts`  
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock packages/aether-contracts contracts/asyncapi.yaml tests/unit/contracts tests/unit/ingestor/test_config.py tests/contract
git commit -m "feat: define mobility density and demand contracts"
```

## Task 2: Build the Event-Time H3 Aggregator and Demand Model

**Files:**
- Create: `services/density-builder/pyproject.toml`
- Create: `services/density-builder/src/aether_density_builder/__init__.py`
- Create: `services/density-builder/src/aether_density_builder/config.py`
- Create: `services/density-builder/src/aether_density_builder/aggregate.py`
- Create: `services/density-builder/src/aether_density_builder/demand.py`
- Modify: `pyproject.toml`
- Test: `tests/unit/density_builder/test_config.py`
- Test: `tests/unit/density_builder/test_aggregate.py`
- Test: `tests/unit/density_builder/test_demand.py`

**Interfaces:**
- Consumes: `AircraftPositionEvent`.
- Produces: `DensityAggregator.apply(event) -> ApplyOutcome`, `DensityAggregator.prepare(now) -> AggregateBatch`, `DensityAggregator.mark_published(batch) -> None`, and `build_demand(density, settings) -> RegionalDemandEvent`.

- [ ] **Step 1: Write failing tests for ordering, movement, sparse output, and expiry**

```python
def test_crossing_cells_moves_aircraft_exactly_once() -> None:
    aggregator = DensityAggregator(settings())
    aggregator.apply(event("abc123", 50.0, 8.0, at=NOW))
    first_batch = aggregator.prepare(NOW)
    aggregator.mark_published(first_batch)
    first = first_batch.densities
    aggregator.apply(event("abc123", 52.0, 13.0, at=NOW + timedelta(seconds=60)))
    second = aggregator.prepare(NOW + timedelta(seconds=60))
    assert sum(cell.aircraft_count for cell in second.densities) == 1
    assert second.removals == [first[0].cell_id]

def test_duplicate_and_equal_timestamp_do_not_change_output() -> None:
    aggregator = DensityAggregator(settings())
    item = event("abc123", 50.0, 8.0, at=NOW)
    assert aggregator.apply(item) is ApplyOutcome.ACCEPTED
    first = aggregator.prepare(NOW)
    aggregator.mark_published(first)
    assert aggregator.apply(item) is ApplyOutcome.DUPLICATE
    assert aggregator.prepare(NOW + timedelta(seconds=10)).is_empty

def test_expiry_emits_removal_without_new_source_events() -> None:
    aggregator = DensityAggregator(settings(window_seconds=180))
    aggregator.apply(event("abc123", 50.0, 8.0, at=NOW))
    first = aggregator.prepare(NOW)
    aggregator.mark_published(first)
    cell = first.densities[0].cell_id
    assert aggregator.prepare(NOW + timedelta(seconds=181)).removals == [cell]
```

- [ ] **Step 2: Verify RED**

Run: `uv run --group dev python -m pytest tests/unit/density_builder -q`  
Expected: FAIL because `aether_density_builder` does not exist.

- [ ] **Step 3: Implement settings and deterministic demand**

```python
class DensityBuilderSettings(BaseModel):
    h3_resolution: int = Field(default=4, ge=0, le=15)
    aggregation_window_seconds: int = Field(default=180, gt=0)
    emit_interval_seconds: int = Field(default=10, gt=0)
    airborne_weight: float = Field(default=1.0, ge=0)
    on_ground_weight: float = Field(default=0.35, ge=0)
    demand_model_version: Literal["density-linear-v1"] = "density-linear-v1"
    demand_coefficient_set: Literal["default-v1"] = "default-v1"

def build_demand(density: MobilityDensityEvent, settings: DensityBuilderSettings) -> RegionalDemandEvent:
    units = density.airborne_count * settings.airborne_weight + density.on_ground_count * settings.on_ground_weight
    return RegionalDemandEvent(
        event_id=deterministic_mobility_event_id("demand.region", density.cell_id, density.window_ended_at, settings.demand_model_version),
        generated_at=density.generated_at,
        window_started_at=density.window_started_at,
        window_ended_at=density.window_ended_at,
        cell_id=density.cell_id,
        h3_resolution=density.h3_resolution,
        aircraft_count=density.aircraft_count,
        demand_units=units,
    )
```

- [ ] **Step 4: Implement the bounded latest-aircraft working set**

Use `h3.latlng_to_cell(latitude, longitude, resolution)`. Store `dict[icao24, AircraftSample]`; accept only a strictly newer `observed_at`. On `prepare(now)`, identify samples older than `now - window`, group current samples by cell, compute nullable averages, and compare a signature containing member event IDs and aggregate values with the last acknowledged signature. Return only changed densities and sorted removed cell IDs. `mark_published(batch)` is the only operation that removes expired samples and advances acknowledged signatures, so a broker failure causes the same batch to be retried.

- [ ] **Step 5: Run focused tests and static checks**

Run: `uv run --group dev python -m pytest tests/unit/density_builder -q && uv run --group dev ruff check services/density-builder tests/unit/density_builder && uv run --group dev mypy services/density-builder`  
Expected: PASS, including no redundant emission at an idle ten-second tick.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock services/density-builder tests/unit/density_builder
git commit -m "feat: aggregate aircraft into sparse H3 demand cells"
```

## Task 3: Consume, Replay, and Publish Mobility Events Reliably

**Files:**
- Create: `services/density-builder/src/aether_density_builder/consumer.py`
- Create: `services/density-builder/src/aether_density_builder/producer.py`
- Create: `services/density-builder/src/aether_density_builder/metrics.py`
- Create: `services/density-builder/src/aether_density_builder/main.py`
- Test: `tests/unit/density_builder/test_consumer.py`
- Test: `tests/unit/density_builder/test_producer.py`
- Test: `tests/unit/density_builder/test_service.py`

**Interfaces:**
- Consumes: Task 2 `DensityAggregator` and `build_demand`.
- Produces: `seek_to_window(consumer, topic, start_ms)`, `publish_batch(producer, batch, settings)`, and `run_service(settings)`.

- [ ] **Step 1: Write failing replay and acknowledgement tests**

```python
async def test_seek_uses_window_start_for_every_partition() -> None:
    consumer = FakeConsumer(partitions={0, 1}, offsets={0: 21, 1: 34})
    await seek_to_window(consumer, "aircraft.position.v1", start_ms=1_000)
    assert consumer.seeks == [(0, 21), (1, 34)]

async def test_replay_discards_events_older_than_the_working_window() -> None:
    aggregator = await replay([event(at=NOW - timedelta(seconds=181)), event(at=NOW)])
    assert aggregator.sample_count == 1

async def test_publish_batch_sends_density_before_matching_demand() -> None:
    producer = FakeProducer()
    await publish_batch(producer, batch_with_one_cell(), settings())
    assert [record.topic for record in producer.records] == ["mobility.density.v1", "demand.region.v1"]
    assert producer.records[0].key == producer.records[1].key
    assert producer.flush_count == 1
```

- [ ] **Step 2: Verify RED**

Run: `uv run --group dev python -m pytest tests/unit/density_builder/test_consumer.py tests/unit/density_builder/test_producer.py tests/unit/density_builder/test_service.py -q`  
Expected: FAIL because runtime adapters are missing.

- [ ] **Step 3: Implement replay-window seek and poison-record behavior**

After assignment, call `offsets_for_times` for every `TopicPartition` at `utc_now - aggregation_window_seconds`, seek each partition to the returned offset, and seek to the end when no timestamp exists. Parse records strictly; commit malformed records after incrementing a rejection counter. Commit valid aircraft records only after applying them to the in-memory aggregator.

- [ ] **Step 4: Implement acknowledged sparse publication and lifecycle**

Publish changed density records and their demand records keyed by `cell_id`; publish `MobilityDensityRemoved` on `mobility.density.v1` for removals. Await all broker acknowledgements, then call `aggregator.mark_published(batch)`; on any publication error, leave the batch unacknowledged for retry. Run consumption and ten-second emission in one `asyncio.TaskGroup`; shut down consumer, producer, and metrics cleanly.

- [ ] **Step 5: Verify runtime behavior**

Run: `uv run --group dev python -m pytest tests/unit/density_builder -q && uv run --group dev ruff check services/density-builder tests/unit/density_builder && uv run --group dev mypy services/density-builder`  
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add services/density-builder tests/unit/density_builder
git commit -m "feat: publish replayable mobility aggregates"
```

## Task 4: Project and Atomically Join Mobility Topics in Redis

**Files:**
- Create: `services/state-builder/src/aether_state_builder/mobility_consumer.py`
- Create: `services/state-builder/src/aether_state_builder/mobility_projection.py`
- Modify: `services/state-builder/src/aether_state_builder/config.py`
- Modify: `services/state-builder/src/aether_state_builder/main.py`
- Modify: `services/state-builder/src/aether_state_builder/metrics.py`
- Test: `tests/unit/state_builder/test_mobility_projection.py`
- Test: `tests/unit/state_builder/test_mobility_consumer.py`

**Interfaces:**
- Consumes: `MobilityDensityEvent | MobilityDensityRemoved` and `RegionalDemandEvent`.
- Produces: `MobilityProjection.apply_density`, `apply_demand`, and `remove`; Redis keys `aether:mobility:{cell_id}`, `aether:mobility:index`, and channel `aether:mobility:updates`.

- [ ] **Step 1: Write failing cross-topic ordering and invalid-H3 tests**

```python
async def test_projection_publishes_only_matching_windows(redis) -> None:
    projection = MobilityProjection(redis)
    await projection.apply_demand(demand(window=WINDOW_2))
    await projection.apply_density(density(window=WINDOW_1))
    assert redis.published == []
    await projection.apply_density(density(window=WINDOW_2))
    assert MobilityCell.model_validate_json(redis.published[-1]).density.window_ended_at == WINDOW_2

async def test_invalid_h3_cell_is_committed_and_rejected() -> None:
    result = await consume_mobility_one(consumer_with(cell_id="not-h3"), projection)
    assert result is MobilityConsumeResult.REJECTED
    assert consumer.commit_count == 1
```

- [ ] **Step 2: Verify RED**

Run: `uv run --group dev python -m pytest tests/unit/state_builder/test_mobility_projection.py tests/unit/state_builder/test_mobility_consumer.py -q`  
Expected: FAIL because mobility projection modules do not exist.

- [ ] **Step 3: Implement the atomic join Lua script**

The script stores density and demand payloads with separate window epoch fields. It rejects an older window. It publishes a combined `MobilityCell` only when both window fields equal the incoming window, then adds the cell to the sorted index. Removal deletes the hash, removes the index member, and publishes a typed cell-removal payload. Validate `h3.is_valid_cell(cell_id)` before calling Redis.

- [ ] **Step 4: Run the aircraft and mobility consumers concurrently**

Add `REDPANDA_MOBILITY_DENSITY_TOPIC`, `REDPANDA_DEMAND_REGION_TOPIC`, and `MOBILITY_STATE_BUILDER_GROUP`. Keep the existing aircraft consumer unchanged and start an independent mobility consumer in `asyncio.TaskGroup`; a failure in either task must stop the process so the container restarts visibly.

- [ ] **Step 5: Verify projection quality**

Run: `uv run --group dev python -m pytest tests/unit/state_builder -q && uv run --group dev ruff check services/state-builder tests/unit/state_builder && uv run --group dev mypy services/state-builder`  
Expected: PASS for demand-first, density-first, duplicate, stale, removal, malformed, and Redis retry cases.

- [ ] **Step 6: Commit**

```bash
git add services/state-builder tests/unit/state_builder
git commit -m "feat: project joined mobility cells into Redis"
```

## Task 5: Expose Mobility Snapshot and WebSocket APIs

**Files:**
- Create: `services/api/src/aether_api/mobility_repository.py`
- Create: `services/api/src/aether_api/mobility_broadcaster.py`
- Create: `services/api/src/aether_api/mobility_feed.py`
- Create: `services/api/src/aether_api/mobility_routes.py`
- Create: `services/api/src/aether_api/mobility_websocket.py`
- Modify: `services/api/src/aether_api/app.py`
- Modify: `services/api/src/aether_api/config.py`
- Modify: `services/api/src/aether_api/metrics.py`
- Test: `tests/unit/api/test_mobility_repository.py`
- Test: `tests/unit/api/test_mobility_feed.py`
- Test: `tests/unit/api/test_mobility_routes.py`
- Test: `tests/unit/api/test_mobility_websocket.py`

**Interfaces:**
- Produces: `MobilityRepository.snapshot(now) -> MobilitySnapshot`, `/api/v1/mobility/cells`, and `/api/v1/stream/mobility`.

- [ ] **Step 1: Write failing snapshot, removal, heartbeat, and slow-client tests**

```python
async def test_snapshot_excludes_expired_cells() -> None:
    snapshot = await repository.snapshot(NOW)
    assert [cell.density.cell_id for cell in snapshot.cells] == ["841f1d7ffffffff"]

async def test_feed_forwards_typed_removal() -> None:
    await feed.handle_payload(MobilityCellRemove(sent_at=NOW, cell_ids=[CELL]).model_dump_json())
    assert (await broadcaster.queue.get()).cell_ids == [CELL]

def test_slow_mobility_client_is_evicted() -> None:
    broadcaster = MobilityBroadcaster(queue_size=1)
    client_id, _ = broadcaster.subscribe()
    broadcaster.publish_nowait(upsert(CELL_A))
    broadcaster.publish_nowait(upsert(CELL_B))
    assert client_id not in broadcaster.client_ids

def test_source_freshness_matches_sixty_second_polling() -> None:
    settings = ApiSettings()
    assert settings.source_live_max_age_seconds == 75
    assert settings.source_offline_max_age_seconds == 180
```

- [ ] **Step 2: Verify RED**

Run: `uv run --group dev python -m pytest tests/unit/api/test_mobility_repository.py tests/unit/api/test_mobility_feed.py tests/unit/api/test_mobility_routes.py tests/unit/api/test_mobility_websocket.py -q`  
Expected: FAIL because the mobility API modules do not exist.

- [ ] **Step 3: Implement repository and snapshot route**

Read active IDs from `aether:mobility:index`, load and validate joined payloads, sort by `cell_id`, and return `MobilitySnapshot` with the existing OpenSky `SourceStatus`. Use one Lua expiry operation to delete expired cell hashes/index members and return removed IDs.

- [ ] **Step 4: Implement one Redis feed and bounded WebSocket fan-out**

Subscribe once to `aether:mobility:updates`, parse only `MobilityCellUpsert | MobilityCellRemove`, and fan out through per-client bounded queues. The WebSocket validates origin, sends `mobility.stream.ready`, then queue messages or `MobilityHeartbeat` using the same configured heartbeat interval as aircraft.

- [ ] **Step 5: Wire lifecycle, routes, dependencies, and metrics**

Create mobility repository/broadcaster state in `app.py`, start `MobilityFeed.run()` beside `ProjectionFeed.run()`, cancel both on shutdown, and expose active-cell, snapshot-duration, Redis-error, connected-client, and dropped-client metrics.

- [ ] **Step 6: Verify and commit**

Run: `uv run --group dev python -m pytest tests/unit/api -q && uv run --group dev ruff check services/api tests/unit/api && uv run --group dev mypy services/api`  
Expected: PASS.

```bash
git add services/api tests/unit/api
git commit -m "feat: serve mobility cell snapshots and updates"
```

## Task 6: Add Browser Mobility Contracts, Store, and Reconnect Client

**Files:**
- Create: `web/lib/mobility-contracts.ts`
- Create: `web/lib/mobility-store.ts`
- Create: `web/lib/mobility-client.ts`
- Test: `web/lib/mobility-contracts.test.ts`
- Test: `web/lib/mobility-store.test.ts`
- Test: `web/lib/mobility-client.test.ts`
- Modify: `web/lib/runtime-client.ts`
- Modify: `web/lib/runtime-client.test.ts`

**Interfaces:**
- Produces: `MobilityStore`, `MobilityClient`, `MobilityLayerMode = "aircraft" | "density" | "demand"`, and combined `OperationsSnapshot.mobilityCells`.

- [ ] **Step 1: Write failing snapshot-first and stale-message tests**

```typescript
it("replaces the store from a snapshot before applying stream deltas", async () => {
  await client.start();
  expect(store.ids()).toEqual(["841f1d7ffffffff"]);
  socket.message(upsert("841f1d5ffffffff"));
  expect(store.ids()).toEqual(["841f1d5ffffffff", "841f1d7ffffffff"]);
});

it("does not regress a cell with an older aggregate window", () => {
  store.replaceSnapshot(snapshot(WINDOW_2));
  store.apply(upsertAt(WINDOW_1));
  expect(store.get(CELL)?.density.window_ended_at).toBe(WINDOW_2);
});
```

- [ ] **Step 2: Verify RED**

Run: `npm --prefix web test -- mobility-contracts.test.ts mobility-store.test.ts mobility-client.test.ts runtime-client.test.ts`  
Expected: FAIL because mobility browser modules do not exist.

- [ ] **Step 3: Implement strict guards and immutable store updates**

Validate discriminator, schema version, cell ID, counts, demand values, provenance, and timestamp strings before accepting network data. `replaceSnapshot` replaces all cells; upsert accepts only an equal-or-newer `window_ended_at`; removal deletes named IDs; heartbeat updates freshness.

- [ ] **Step 4: Implement snapshot-first reconnection and runtime composition**

Follow the existing `AircraftClient` backoff/abort pattern with URLs `/api/v1/mobility/cells` and `/api/v1/stream/mobility`. Start both clients from the runtime facade, stop both, merge subscriptions, and expose independent `aircraftStreamConnected` and `mobilityStreamConnected` flags.

- [ ] **Step 5: Verify and commit**

Run: `npm --prefix web test -- mobility-contracts.test.ts mobility-store.test.ts mobility-client.test.ts runtime-client.test.ts && npm --prefix web run typecheck`  
Expected: PASS.

```bash
git add web/lib
git commit -m "feat: maintain live mobility cells in the browser"
```

## Task 7: Render H3 Density and Demand Polygon Layers

**Files:**
- Modify: `web/package.json`
- Modify: `web/package-lock.json`
- Create: `web/lib/mobility-geometry.ts`
- Test: `web/lib/mobility-geometry.test.ts`
- Modify: `web/components/airspace-map.tsx`
- Modify: `web/lib/maplibre-map.ts`
- Modify: `web/lib/maplibre-map.test.ts`

**Interfaces:**
- Consumes: mobility cells and layer mode from Task 6.
- Produces: `toMobilityFeatureCollection(cells, mode, now)` and map callback `onSelectCell(cellId)`.

- [ ] **Step 1: Write failing geometry and layer-style tests**

```typescript
it("closes H3 polygons using GeoJSON longitude-latitude order", () => {
  const feature = toMobilityFeatureCollection([CELL], "density", NOW).features[0];
  const ring = feature.geometry.coordinates[0];
  expect(ring[0]).toEqual(ring.at(-1));
  expect(ring.every(([lon, lat]) => lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90)).toBe(true);
});

it("rejects invalid and antimeridian-broken rings without crashing", () => {
  expect(toMobilityFeatureCollection([INVALID_CELL], "density", NOW).features).toEqual([]);
  expect(() => toMobilityFeatureCollection([ANTIMERIDIAN_CELL], "demand", NOW)).not.toThrow();
});
```

- [ ] **Step 2: Verify RED**

Run: `npm --prefix web test -- mobility-geometry.test.ts maplibre-map.test.ts`  
Expected: FAIL because geometry conversion and layers are missing.

- [ ] **Step 3: Install h3-js and build polygon features**

Run: `npm --prefix web install h3-js@^4`. Use `cellToBoundary(cellId, true)` for GeoJSON longitude/latitude order, close each ring, normalize antimeridian segments, skip invalid cells, and attach `aircraft_count`, `demand_units`, `stale`, and `cell_id` properties.

- [ ] **Step 4: Add shared fill/outline layers and selection**

Create one `mobility-cells` GeoJSON source. Add density and demand fill layers with mutually exclusive `visibility`, data-driven color/opacity, a subtle outline, pointer cursor, and click selection. Update source data only when cells/mode change; keep the aircraft animation frame independent.

- [ ] **Step 5: Verify and commit**

Run: `npm --prefix web test -- mobility-geometry.test.ts maplibre-map.test.ts && npm --prefix web run lint && npm --prefix web run typecheck`  
Expected: PASS.

```bash
git add web/package.json web/package-lock.json web/lib web/components/airspace-map.tsx
git commit -m "feat: render sparse H3 mobility layers"
```

## Task 8: Add Layer Controls, Cell Details, and Provenance UI

**Files:**
- Create: `web/components/layer-control.tsx`
- Create: `web/components/mobility-cell-detail.tsx`
- Test: `web/components/layer-control.test.tsx`
- Test: `web/components/mobility-cell-detail.test.tsx`
- Modify: `web/components/operations-shell.tsx`
- Modify: `web/components/operations-shell.test.tsx`
- Modify: `web/app/globals.css`
- Modify: `web/tests/e2e/aether-live-map.spec.ts`

**Interfaces:**
- Consumes: Task 6 runtime snapshot and Task 7 cell selection.
- Produces: accessible three-mode control and selected-cell operational details.

- [ ] **Step 1: Write failing interaction and provenance tests**

```typescript
it("shows modelled provenance when demand mode is selected", async () => {
  render(<OperationsShell client={clientWithCells()} mapFactory={mapFactory} />);
  await userEvent.click(screen.getByRole("radio", { name: "Modelled demand" }));
  expect(screen.getByText("Modelled demand—not measured infrastructure load.")).toBeVisible();
});

it("shows density counts and the aggregate window for a selected cell", () => {
  render(<MobilityCellDetail cell={CELL} />);
  expect(screen.getByText("12 aircraft")).toBeVisible();
  expect(screen.getByText(/Derived from OpenSky aircraft positions/)).toBeVisible();
});
```

- [ ] **Step 2: Verify RED**

Run: `npm --prefix web test -- layer-control.test.tsx mobility-cell-detail.test.tsx operations-shell.test.tsx`  
Expected: FAIL because controls/details are missing.

- [ ] **Step 3: Implement the control-room interaction**

Use a labelled radio group for Aircraft only, Aircraft density, and Modelled demand. Keep aircraft visible in all modes. Clear a selected cell when it is removed. Show count, airborne/on-ground split, averages, window, watermark age, demand units, model/coefficient names, and provenance in the detail panel.

- [ ] **Step 4: Add responsive, stale, and reduced-motion styling**

Use the existing control-room tokens, not generic cards. Add cyan-blue density and amber-magenta demand legends, fade stale polygons, move details below the map at the existing narrow breakpoint, and disable transitions under `prefers-reduced-motion: reduce`.

- [ ] **Step 5: Extend the browser fixture journey**

Mock mobility snapshot and WebSocket separately from aircraft. Assert density selection renders a polygon, demand selection changes provenance/legend, selecting the polygon reveals details, and no `.map-alert` appears.

- [ ] **Step 6: Verify and commit**

Run: `npm --prefix web test && npm --prefix web run test:e2e && npm --prefix web run build`  
Expected: all unit/component tests, one browser journey, and production build PASS.

```bash
git add web/components web/app/globals.css web/tests/e2e
git commit -m "feat: add mobility heatmap controls and provenance"
```

## Task 9: Wire Local Infrastructure, Integration Tests, Metrics, and Documentation

**Files:**
- Modify: `infra/redpanda/create-topics.sh`
- Modify: `infra/prometheus/prometheus.yaml`
- Modify: `compose.yaml`
- Modify: `.env.example`
- Modify: `Makefile`
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/data-provenance.md`
- Create: `docs/adr/0005-use-h3-for-mobility-aggregation.md`
- Create: `tests/integration/test_mobility_pipeline.py`
- Create: `tests/load/test_density_builder_load.py`

**Interfaces:**
- Consumes: the complete Phase 3 pipeline.
- Produces: one-command local startup, observable services, and final acceptance evidence.

- [ ] **Step 1: Write the failing integration and bounded-load tests**

```python
import time

async def test_aircraft_event_reaches_mobility_snapshot_and_stream(stack) -> None:
    await stack.publish_aircraft(event("abc123", 50.0, 8.0, at=NOW))
    cell = await stack.wait_for_mobility_cell()
    assert cell.density.provenance == "derived"
    assert cell.demand.provenance == "modelled"
    assert cell.demand.demand_units == 1.0

def test_ten_thousand_updates_keep_one_sample_per_aircraft() -> None:
    started = time.perf_counter()
    aggregator = DensityAggregator(settings())
    for index in range(10_000):
        aggregator.apply(generated_event(
            icao24=f"{index % 1000:06x}",
            observed_at=NOW + timedelta(milliseconds=index),
        ))
    batch = aggregator.prepare(NOW + timedelta(seconds=10))
    assert aggregator.sample_count == 1_000
    assert sum(cell.aircraft_count for cell in batch.densities) == 1_000
    assert len(batch.densities) <= 1_000
    assert time.perf_counter() - started < 2.0
```

- [ ] **Step 2: Verify RED against the not-yet-wired stack**

Run: `uv run --group dev python -m pytest tests/integration/test_mobility_pipeline.py tests/load/test_density_builder_load.py -q`  
Expected: integration FAIL because topics/service wiring are absent; load test PASS once Task 2 is present.

- [ ] **Step 3: Create topics and compose the new service**

Create `mobility.density.v1` and `demand.region.v1` with three partitions, one replica, and 24-hour retention. Add `density-builder` after topic initialization, expose metrics port 9103, pass all Task 2 settings, and make state-builder depend on topic initialization. Preserve host web port `${WEB_PORT:-3001}`.

- [ ] **Step 4: Add configuration, scrape target, and stable verification commands**

Set the exact defaults from Global Constraints in `.env.example`. Add `density-builder:9103` to Prometheus. Extend `make test`, `make lint`, and `make smoke` so density-builder tests/types, mobility integration, and the browser test cannot be skipped or converted to success.

- [ ] **Step 5: Document operation and provenance**

Update the architecture flow, Redis keys, topics, recovery behavior, 60-second quota rationale, short-demo override, map controls, and troubleshooting. The ADR records why H3 resolution 4 was selected over latitude/longitude squares and client-only aggregation.

- [ ] **Step 6: Run the full verification matrix**

Run:

```bash
make test
make lint
uv run --group dev python -m pytest --cov=packages --cov=services --cov-report=term-missing
npm --prefix web run build
npm --prefix web audit --audit-level=high
```

Expected: all tests and checks PASS, backend coverage remains at least 80%, the production build succeeds, and npm reports zero high/critical vulnerabilities.

When Docker is available, additionally run:

```bash
cp .env.example .env
OPEN_SKY_POLL_INTERVAL_SECONDS=10 AETHER_SOURCE_MODE=fixture docker compose up --build -d
make smoke
curl --fail http://localhost:8000/health/ready
curl --fail http://localhost:8000/api/v1/mobility/cells
curl --fail http://localhost:3001
docker compose down
```

Expected: the fixture aircraft produces at least one derived density cell and one modelled demand cell; API readiness and both HTTP endpoints return success. If Docker remains unavailable, record this exact acceptance check as the only environmental blocker rather than claiming it ran.

- [ ] **Step 7: Commit**

```bash
git add infra compose.yaml .env.example Makefile README.md docs tests/integration tests/load
git commit -m "feat: complete geographic demand heatmap slice"
```

## Final Review Gate

- [ ] Run `git status --short` and confirm only intentional files remain.
- [ ] Compare every requirement in the approved spec with Tasks 1–9.
- [ ] Review the complete branch for correctness, security, provenance, operability, and accidental scope growth.
- [ ] Fix every Critical or Important finding and rerun the affected focused tests plus the full verification matrix.
- [ ] Use `superpowers:finishing-a-development-branch` only after verification evidence is current and the Docker-only limitation, if still present, is explicitly reported.
