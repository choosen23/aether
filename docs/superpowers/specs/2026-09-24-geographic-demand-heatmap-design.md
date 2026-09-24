# Aether Geographic Density and Demand Heatmap Design

**Date:** 2026-09-24  
**Status:** Approved

## 1. Purpose

This specification covers Aether's next architectural increment after the live
aircraft vertical slice. It converts real aircraft movement into a reusable
geographic density signal and a separate, explicitly modelled demand signal,
then presents both in the operations map.

The increment supports Aether's long-term research question: whether
mobility-aware predictive orchestration can outperform reactive orchestration
as demand moves geographically. It establishes the spatial contracts that
future workload generation, mobility prediction, cloud-region simulation, and
controllers will consume without introducing those systems prematurely.

## 2. Scope and Success Criteria

The increment is complete when:

- a dedicated service consumes `aircraft.position.v1` and maintains bounded,
  event-time geographic aggregates;
- aircraft density and modelled demand are published as separate versioned
  contracts with explicit provenance;
- only occupied H3 cells are projected, served, and rendered;
- snapshot recovery and live cell updates work through FastAPI and WebSockets;
- the control-room map can switch between aircraft-only, density, and demand
  views while retaining aircraft symbols;
- stale inputs visibly fade and expire rather than being silently fabricated;
- unit, contract, integration, frontend, browser, and bounded-load tests pass
  without calling OpenSky; and
- anonymous OpenSky operation defaults to a quota-conscious 60-second cadence,
  with 10 seconds retained as an explicit short-demo setting.

Trajectory prediction, workload classes, cloud-region agents, infrastructure
telemetry, Kubernetes execution, controllers, replay, chaos, and MARL remain
outside this increment.

## 3. Provenance

The two map layers must not blur the distinction between observed and modelled
data.

### 3.1 Aircraft density

Aircraft density is **derived** from real OpenSky observations. The system uses
the newest valid position for each aircraft, assigns that position to an H3
cell, and reports aggregate counts and summary statistics. It does not claim
that density is directly measured by infrastructure.

### 3.2 Demand index

Demand is **modelled**. Version 1 applies deterministic, configurable
coefficients to density:

```text
demand_units = airborne_count * airborne_weight
             + on_ground_count * ground_weight
```

The coefficients are configuration values, not learned parameters. The API and
interface label the result as modelled demand and never present it as measured
compute load.

## 4. Architecture

```text
OpenSky (60-second anonymous default; 10-second short-demo option)
  -> ingestor
  -> Redpanda aircraft.position.v1
  -> density-builder
       - latest valid position per aircraft
       - event-time ordering and deduplication
       - three-minute freshness window
       - H3 resolution 4 by default
       - ten-second aggregate emission
  -> Redpanda mobility.density.v1     (derived)
  -> Redpanda demand.region.v1        (modelled)
  -> state-builder mobility consumer
  -> Redis latest-cell projection + transient update channel
  -> FastAPI mobility snapshot + dedicated mobility WebSocket
  -> MapLibre H3 polygon layers
```

`density-builder` is an independently restartable consumer. Redpanda remains
the retained event boundary, and Redis remains rebuildable query state. The
service reconstructs its three-minute working set on startup by seeking each
aircraft-topic partition to the configured window start, catching up, and then
resuming normal consumption. This phase does not add a historical database.
The existing state-builder gains an independent mobility consumer that projects
the two aggregate topics only after their publication; the density-builder does
not dual-write Redis.

H3 resolution is configurable and defaults to resolution 4 for a sparse,
Europe-wide operational view. Only occupied cells are emitted. Later phases
may aggregate these cells into cloud-region catchments without changing the
source aircraft contract.

## 5. Aggregation Semantics

The service uses event time rather than broker arrival time.

- Keep only the newest accepted observation per aircraft.
- Reject duplicates and observations that are not newer than stored state.
- Ignore observations without coordinates for spatial aggregation while
  counting them in rejection/omission metrics.
- Assign each aircraft to exactly one H3 cell using its newest position.
- Count an aircraft at most once in each emission, regardless of source event
  frequency.
- Retain an aircraft for three minutes after its observation timestamp.
- Move an aircraft atomically from its previous cell to its new cell.
- Emit changed cells and explicit removals for cells that become empty.
- Attach the newest incorporated source-event timestamp as a watermark.
- Expire all cells naturally when the upstream source stops producing current
  observations.

The ten-second emission cadence is local computation and does not trigger
additional OpenSky requests.

## 6. Event Contracts

All events use UTC RFC 3339 timestamps, `schema_version: 1`, strict validation,
and a deterministic event identifier.

### 6.1 `mobility.density.v1`

One event describes one occupied H3 cell for an aggregate emission:

```text
event_id
event_type = "mobility.density"
schema_version = 1
generated_at
window_started_at
window_ended_at
source_event_watermark
cell_id
h3_resolution
aircraft_count
airborne_count
on_ground_count
average_altitude_m
average_ground_speed_mps
provenance = "derived"
```

Nullable averages remain null when no contributing observation supplies the
underlying measurement. Counts must be non-negative and internally consistent.

### 6.2 `demand.region.v1`

The demand event uses the same H3 cell identity and aggregate interval:

```text
event_id
event_type = "demand.region"
schema_version = 1
generated_at
window_started_at
window_ended_at
cell_id
h3_resolution
aircraft_count
demand_units
model_version
coefficient_set
provenance = "modelled"
```

`model_version` identifies the deterministic formula. `coefficient_set`
records the named configuration used for reproducibility. Future workload
generation may consume this event, but no workload is created in this phase.

### 6.3 Removal and browser messages

Cell removal is explicit and keyed by H3 cell ID. The mobility WebSocket uses
discriminated messages for readiness, density upsert, demand upsert, cell
removal, and heartbeat/freshness status. Per-client queues are bounded.

## 7. Redis Projection

Redis stores the latest accepted density and demand payload for each active
cell, an index ordered by aggregate end time, source watermarks, and a transient
notification channel. Projection writes compare aggregate timestamps
atomically so delayed or replayed events cannot regress current state.

The projection supports:

- fetching all active cells for snapshot recovery;
- expiring cells whose input window is no longer current;
- publishing changed-cell and removal notifications; and
- rebuilding from Redpanda without treating Redis as authoritative history.

## 8. API

The API adds:

- `GET /api/v1/mobility/cells` for an atomic density-and-demand snapshot;
- `WS /api/v1/stream/mobility` for readiness, upserts, removals, and freshness
  heartbeats; and
- Prometheus metrics for consumer lag, active cells, aggregate age, rejected
  events, projection outcomes, WebSocket clients, and emission duration.

Aircraft and mobility streams remain separate. This prevents aggregate update
frequency and future contract evolution from coupling to individual-aircraft
delivery. A reconnect replaces the browser's mobility state from the snapshot
before applying new deltas.

## 9. Control-Room Experience

A compact map control offers three modes:

1. Aircraft only.
2. Aircraft density.
3. Modelled demand.

Aircraft symbols may remain visible over either aggregate mode. Density and
demand shading are mutually exclusive because simultaneous color scales would
be misleading.

The browser converts H3 cell boundaries into one shared GeoJSON polygon source.
Density uses a cool cyan-to-blue scale based on aircraft count. Modelled demand
uses an amber-to-magenta scale based on demand units. Empty cells are absent.
Stale aggregates fade before expiry.

Selecting a cell opens a focused detail panel containing its aircraft counts,
observation window, watermark age, demand units, model version, and provenance.
The legend says either “Derived from OpenSky aircraft positions” or “Modelled
demand—not measured infrastructure load.” Source and stream health remain
visible when the cell set is empty.

On narrow screens, details collapse below the map. Reduced-motion preferences
disable animated color transitions.

## 10. Configuration Defaults

```text
OPEN_SKY_POLL_INTERVAL_SECONDS=60
SOURCE_LIVE_MAX_AGE_SECONDS=75
SOURCE_OFFLINE_MAX_AGE_SECONDS=180
AGGREGATION_EMIT_INTERVAL_SECONDS=10
AGGREGATION_WINDOW_SECONDS=180
H3_RESOLUTION=4
DEMAND_MODEL_VERSION=density-linear-v1
DEMAND_COEFFICIENT_SET=default-v1
DEMAND_AIRBORNE_WEIGHT=1.0
DEMAND_ON_GROUND_WEIGHT=0.35
```

The named `default-v1` coefficient set contains these exact values and is
covered by contract examples. Operators may set OpenSky polling to 10 seconds
for a short live demonstration, but automated tests always use recorded
fixtures or generated internal events. The source freshness thresholds are
longer than the normal poll interval so a healthy anonymous source remains
live between observations and becomes offline at the aggregate expiry horizon.

## 11. Failure Behavior

- A Redpanda outage pauses aggregation; it never activates a synthetic feed.
- A Redis outage makes mobility readiness fail but does not remove retained
  source events from Redpanda.
- Poison messages are observable and committed after rejection so they cannot
  halt a partition indefinitely.
- Duplicate and out-of-order events cannot regress aircraft or aggregate state.
- Stale source input freezes new updates, fades existing cells, and removes them
  after the configured three-minute window.
- Invalid coordinates or H3 conversion failures are counted and skipped without
  discarding valid sibling events.
- Slow WebSocket clients are evicted from bounded queues without blocking
  healthy clients.
- Snapshot or stream failure retains the last valid browser state only until
  its visible expiry horizon.

## 12. Testing Strategy

Tests must not contact OpenSky.

- Contract tests validate density, demand, removal, snapshot, and stream
  examples against the canonical models and AsyncAPI document.
- Aggregation unit tests cover duplicates, reordering, aircraft movement,
  expiry, unique-aircraft counting, nullable averages, watermarks, and cell
  removals.
- H3 tests cover known coordinates, boundary behavior, configured resolution,
  and invalid input.
- Projection tests verify atomic timestamp monotonicity and expiry.
- Integration tests exercise Redpanda through density-builder, Redis, snapshot,
  and WebSocket delivery.
- Frontend tests cover layer selection, provenance, cell details, stale state,
  and snapshot-first reconnects.
- Browser tests use recorded aircraft fixtures and validate both aggregate map
  modes without using the provider.
- Generated internal-event load tests verify bounded memory, sparse emissions,
  and acceptable aggregation latency.

## 13. Deferred Work

This phase does not implement trajectory forecasts, workload classes,
historical geospatial storage, named cloud/edge regions, capacity models,
Kubernetes workloads, real infrastructure telemetry, controller actions,
controller comparison, replay, weather, chaos, or reinforcement learning.
Those phases consume the versioned density and demand contracts introduced
here.
