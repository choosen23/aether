# Aether Live Aircraft Vertical Slice Design

**Date:** 2026-09-24  
**Status:** Approved

## 1. Purpose

Aether is a real-time geospatial digital twin for mobility-driven cloud
workloads. Its long-term research goal is to test whether mobility-aware,
predictive orchestration outperforms reactive cloud orchestration as compute
demand moves geographically.

This specification covers only the first runnable vertical slice:

1. Fetch live aircraft observations over Europe.
2. Normalize observations into a typed, versioned internal event.
3. Publish events to a local event stream.
4. Maintain the latest aircraft state in Redis.
5. Expose snapshot and live-update APIs.
6. Render smoothly moving aircraft on a Europe map.

No demand model, cloud-region simulation, Kubernetes workload, controller, or
reinforcement-learning code belongs in this slice. The design must nevertheless
create durable boundaries that those later phases can consume without replacing
the ingestion path.

## 2. Success Criteria

The slice is complete when:

- `docker compose up --build` starts all required services locally.
- Anonymous OpenSky observations for Europe flow through Redpanda and Redis to
  the browser.
- Aircraft move smoothly between source observations.
- The interface exposes source freshness, stream health, and stale data.
- Unit, contract, integration, frontend, and browser smoke tests pass without
  consuming the OpenSky quota.
- Logs are structured and Prometheus metrics are available.
- Documentation explicitly distinguishes real, derived, and simulated data.

## 3. Data Provenance

This slice displays the following real data supplied by OpenSky:

- aircraft transponder identity;
- callsign when available;
- latitude and longitude when available;
- velocity, track, vertical rate, and altitude when available;
- on-ground state, source timestamps, and position-source metadata.

Smooth positions between observations are derived through bounded client-side
interpolation. The interface must label freshness and stop interpolating beyond
the configured horizon. It must not invent aircraft, extrapolate indefinitely,
or silently substitute synthetic data when OpenSky is unavailable.

## 4. Technical Decisions

### 4.1 Live source

OpenSky is the initial provider. Anonymous access is the default and requires no
credentials. Polling occurs every 10 seconds, matching anonymous data
resolution. The provider interface and configuration reserve optional OAuth
client credentials for later use without changing downstream contracts.

The Europe query uses a configurable WGS84 bounding box. The default should
cover the intended European map while remaining explicit and adjustable.

### 4.2 Event transport

Redpanda is the Kafka-compatible local event backbone. It provides retained,
ordered events and independent consumers for later historical storage, demand
generation, forecasts, and replay. The initial topic is
`aircraft.position.v1`.

Redis Streams are not used as the primary event backbone because combining
transport and projection state would couple the first slice to a storage model
that later phases would outgrow.

### 4.3 Projection state

Redis stores only the latest accepted state for each aircraft and projection
metadata. It is not the authoritative event history. The state builder rejects
updates that are not newer than the stored observation timestamp.

### 4.4 Frontend map

Next.js, React, TypeScript, and MapLibre GL render the interface. Aircraft are
drawn in a shared GeoJSON symbol layer rather than one React marker per
aircraft. This keeps browser work proportional and makes several thousand
moving objects practical.

### 4.5 Deliberately deferred components

PostgreSQL/PostGIS, TimescaleDB, Kubernetes manifests, demand aggregation,
weather, workload generation, cloud-region agents, controllers, experiments,
and MARL are deferred. Their future inputs are the versioned event stream and
stable query/stream contracts established here.

## 5. Repository Structure

```text
aether/
├── README.md
├── Makefile
├── compose.yaml
├── .env.example
├── pyproject.toml
├── packages/
│   └── aether-contracts/
│       └── src/aether_contracts/
├── services/
│   ├── ingestor/
│   │   └── src/aether_ingestor/
│   ├── state-builder/
│   │   └── src/aether_state_builder/
│   └── api/
│       └── src/aether_api/
├── web/
│   ├── app/
│   ├── components/
│   ├── lib/
│   └── tests/
├── contracts/
│   └── asyncapi.yaml
├── infra/
│   ├── prometheus/
│   └── redpanda/
├── tests/
│   ├── contract/
│   ├── integration/
│   └── fixtures/opensky/
└── docs/
    ├── architecture.md
    └── adr/
```

The Python package uses one workspace configuration while preserving separate
process entry points. Shared schemas live in `aether-contracts`; services may
depend on contracts, but contracts must not depend on services.

## 6. Architecture and Data Flow

```text
OpenSky REST API (Europe bbox, 10-second poll)
        |
        v
ingestor
  fetch -> validate -> normalize -> deduplicate
        |
        v
Redpanda topic: aircraft.position.v1
        |
        v
state-builder
  reject stale updates -> update Redis latest-state projection
                       -> publish Redis live-update notification
        |
        v
FastAPI gateway
  GET /api/v1/aircraft
  WS  /api/v1/stream/aircraft
  GET /health/live
  GET /health/ready
  GET /metrics
        |
        v
Next.js + MapLibre
  snapshot -> WebSocket deltas -> bounded interpolation
```

Each backend process is independently restartable. Redpanda retention allows a
new state builder to rebuild Redis after projection loss. The WebSocket gateway
reads snapshot state from Redis and subscribes to projection notifications; it
does not consume the durable topic independently in version 1.

## 7. Event Contract

The canonical event is a typed JSON document:

```json
{
  "event_id": "01J...",
  "event_type": "aircraft.position",
  "schema_version": 1,
  "observed_at": "2026-09-24T09:10:00Z",
  "ingested_at": "2026-09-24T09:10:01.234Z",
  "source": "opensky",
  "aircraft": {
    "icao24": "3c6444",
    "callsign": "DLH4YA",
    "origin_country": "Germany",
    "position": {
      "latitude": 50.04,
      "longitude": 8.56,
      "barometric_altitude_m": 10668.0,
      "on_ground": false
    },
    "motion": {
      "ground_speed_mps": 232.1,
      "track_degrees": 271.4,
      "vertical_rate_mps": -1.2
    },
    "last_contact_at": "2026-09-24T09:10:00Z",
    "position_source": "ads_b"
  }
}
```

### 7.1 Validation

- `icao24` is a normalized lowercase six-character hexadecimal identifier.
- Timestamps are timezone-aware UTC values serialized in RFC 3339 form.
- Latitude is within `[-90, 90]`; longitude is within `[-180, 180]`.
- Track is normalized to `[0, 360)` degrees.
- Callsigns are trimmed; blank callsigns become null.
- Position, callsign, altitude, motion values, and source metadata are nullable
  when OpenSky omits them.
- Events without usable coordinates remain valid telemetry events but are not
  included in the map projection.

### 7.2 Identity and ordering

`event_id` is a deterministic hash-derived identifier over source, ICAO24, and
the source observation timestamp. Re-fetching the same observation therefore
produces the same event ID. The Kafka message key is ICAO24, which preserves
per-aircraft partition ordering. The state builder still compares timestamps
because retries, reconnects, and future multi-source ingestion can deliver
duplicates or stale updates.

The AsyncAPI document is the checked-in transport contract. Python Pydantic
models and TypeScript representations must match it through contract tests.

## 8. HTTP and WebSocket Contracts

### 8.1 Snapshot

`GET /api/v1/aircraft`

```json
{
  "generated_at": "2026-09-24T09:10:02Z",
  "source_status": {
    "source": "opensky",
    "status": "live",
    "last_observation_at": "2026-09-24T09:10:00Z",
    "age_seconds": 2.0
  },
  "aircraft": []
}
```

The snapshot includes only aircraft with valid coordinates that have not passed
the removal threshold. Each projected aircraft includes the source event ID and
observation timestamp required for ordering and interpolation. It supports
initial page load and WebSocket recovery.

### 8.2 Live stream

`WS /api/v1/stream/aircraft`

After connection, the server sends `stream.ready`. It then sends:

- `aircraft.upsert`: batches of current changed aircraft states;
- `aircraft.remove`: ICAO24 identifiers removed after the stale timeout;
- `heartbeat`: server time, source freshness, and connection liveness.

Every message has `message_type`, `schema_version`, and `sent_at`. The client
must replace its state from the HTTP snapshot after reconnecting rather than
assuming it received every transient WebSocket message.

### 8.3 Health and metrics

- `GET /health/live` reports process liveness only.
- `GET /health/ready` checks Redis connectivity and gateway subscription
  readiness.
- `GET /metrics` emits Prometheus text format.

FastAPI's generated OpenAPI document defines HTTP payloads. AsyncAPI defines
Redpanda and WebSocket message payloads.

## 9. Reliability and Failure Handling

### 9.1 OpenSky

The ingestor uses bounded exponential backoff with jitter for transient network
and server failures. It honors `429` retry guidance and records remaining quota
headers when present. Authentication failure handling exists only as an
extension seam until OAuth is enabled.

No failure path starts a synthetic feed. Last-known state remains visible with
an explicit stale or offline source status.

### 9.2 Redpanda and Redis

The producer enables acknowledgements and bounded retries. A source observation
is not treated as published until Redpanda acknowledges it. The state builder
commits consumer offsets only after the Redis projection update succeeds.
Projection writes are atomic and conditional on a newer observation timestamp.

Consumers and Redis clients reconnect with bounded exponential backoff. Failed
messages are logged with event identifiers and counted. A dead-letter topic is
not introduced until failure evidence justifies it; malformed messages are
rejected and observable in this slice.

### 9.3 Browser

The browser reconnects WebSockets with jittered backoff and refreshes the full
snapshot after reconnect. Interpolation stops after its configured horizon.
Aircraft are marked stale before removal. Connection and source state remain
visible without obscuring the map.

## 10. Frontend Experience

The map fills the viewport and is the primary instrument. The design is based
on modern European air-operations displays: dark, spatial, restrained, and
precise, without copying legacy radar styling or using generic dashboard cards.

```text
┌ AETHER / LIVE EUROPE - source age - aircraft count - stream status ┐
│                                                                    │
│                         INTERACTIVE MAP                            │
│   aircraft glyphs + heading vectors + interpolated movement       │
│                                                                    │
│  ┌ selected aircraft ┐                         ┌ source health ┐   │
│  │ callsign / ICAO   │                         │ OpenSky live  │   │
│  │ speed / altitude  │                         │ last update   │   │
│  │ heading / age     │                         │ event rate    │   │
│  └───────────────────┘                         └───────────────┘   │
│                                                                    │
├ timeline freshness strip ---------------- map legend / attribution ┤
```

### 10.1 Visual tokens

- Night Flight `#07131D`: map chrome and background.
- Airspace Blue `#10283A`: operational surfaces.
- Track Cyan `#64D8E8`: live aircraft and healthy connections.
- Amber Vector `#F2B84B`: stale telemetry and attention states.
- Fault Coral `#F26B5E`: disconnected and failed state.
- Cloud White `#DCE8EC`: text and cartographic detail.

IBM Plex Sans is the interface typeface, using tabular numerals for telemetry.
Aircraft use directional vector glyphs, not generic map pins.

### 10.2 Interaction and motion

The browser keeps the two most recent observations for each aircraft and
interpolates longitude, latitude, altitude where displayed, and shortest-path
heading. The animation horizon is bounded by configuration. Newly observed and
selected aircraft receive purposeful visual emphasis; routine panels remain
quiet.

Selecting an aircraft opens a detail surface with callsign, ICAO24, origin,
altitude, speed, heading, vertical rate, source, and observation age. The
source-health surface shows provider, last successful poll, event rate, and
connection state.

The layout collapses safely on narrow screens. Keyboard focus is visible,
contrast meets WCAG AA where applicable, reduced-motion mode disables
interpolation and nonessential animation, and map-source attribution remains
visible.

## 11. Configuration

`.env.example` documents all settings without secrets:

- Europe bounding box;
- OpenSky base URL and 10-second poll interval;
- optional future OAuth client ID and secret variables;
- Redpanda brokers, topic, partitions, and retention;
- Redis URL and key namespace;
- stale-mark and removal thresholds;
- HTTP, WebSocket, metrics, and frontend ports;
- log level and environment name;
- public map style URL and attribution.

Defaults are suitable for local Docker Compose. Production-like values are
configuration, not source edits.

## 12. Observability

All Python services emit JSON logs with timestamp, service, level, message,
environment, and relevant event ID, ICAO24, poll ID, or connection ID. Secrets
and raw authorization headers are never logged.

Prometheus metrics include:

- OpenSky request counts, duration, response status, quota, and observations;
- normalization rejects and coordinate omissions;
- Redpanda publish counts, errors, latency, and consumer lag where available;
- accepted, duplicate, and stale projection events;
- active projected aircraft and source age;
- Redis operation failures and reconnects;
- active WebSocket clients, sent messages, dropped connections, and reconnects.

OpenTelemetry tracing is prepared through trace/context fields and dependency
boundaries, but a full collector and cross-process trace deployment is deferred
until infrastructure telemetry phases unless it is trivial within the selected
libraries.

## 13. Testing Strategy

### 13.1 Unit tests

- OpenSky vector normalization, including null and malformed fields.
- deterministic event identifiers and callsign cleanup.
- duplicate and stale/out-of-order projection handling.
- expiration and source-freshness transitions.
- frontend state reduction and removal.
- position interpolation and heading wraparound.

### 13.2 Contract tests

- Pydantic serialization conforms to checked-in JSON examples and AsyncAPI.
- Snapshot, control, upsert, removal, and heartbeat payloads remain compatible.
- TypeScript consumers accept the canonical contract fixtures.

### 13.3 Integration tests

Real Redis and Redpanda test containers prove:

1. a fixture source vector is published;
2. the state builder projects it;
3. the API snapshot exposes it;
4. a WebSocket client receives a later upsert;
5. duplicate and older observations do not regress state.

### 13.4 Browser smoke test

A local fixture-backed test mode—not an automatic runtime fallback—injects a
known aircraft. The browser test verifies map initialization, aircraft
selection, a displayed position update, and visible connection state.

Automated tests never call OpenSky. `make test`, `make lint`, and `make smoke`
provide repeatable top-level verification.

## 14. Local Operations

Docker Compose runs:

- Redpanda;
- Redis;
- ingestor;
- state builder;
- FastAPI gateway;
- Next.js web application;
- Prometheus when the observability profile is enabled.

Health checks and dependency readiness prevent brittle startup ordering. The
README documents first run, service URLs, stopping, resetting local projection
state, troubleshooting provider quotas, and switching to optional authenticated
OpenSky later.

## 15. Evolution Toward the Full Aether System

The first slice deliberately establishes extension seams for every later phase:

- A historical sink can consume `aircraft.position.v1` into PostGIS without
  changing ingestion.
- Geographic aggregation and mobility prediction can consume the same stream
  and emit `aircraft.state`, `mobility.forecast`, and `demand.region` events.
- Workload generation can consume versioned demand events while preserving the
  real/derived/simulated distinction.
- Regional simulators, Kubernetes executors, and telemetry collectors can add
  their own event families without sharing database internals.
- Deterministic baselines and later MARL controllers can run against identical
  replayed scenarios and metrics.
- The future deterministic action-validation layer remains between every
  controller and Kubernetes; no policy receives unrestricted cluster access.

This evolution must follow the requested phase order. Stable contracts are a
foundation for later phases, not permission to implement them early.

## 16. Non-Goals

This slice does not:

- guarantee commercial-grade OpenSky availability or quota;
- identify schedules, routes, passengers, or aircraft ownership beyond source
  fields;
- persist historical trajectories beyond Redpanda's configured local retention;
- predict aircraft trajectories;
- generate compute demand;
- model or deploy cloud regions;
- perform Kubernetes actions;
- implement controller baselines, replay experiments, chaos, or MARL.

## 17. Security and Privacy

Only public aircraft telemetry required by the product is processed. Provider
credentials, if later configured, enter through environment/secrets management
and are never sent to the browser. Services expose only necessary ports in
Compose. The API validates payloads and bounds batch/message sizes. CORS and
WebSocket origins are explicit configuration rather than unrestricted defaults.
