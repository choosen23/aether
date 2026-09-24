# Aether architecture: aircraft + mobility heatmap slice

## Data flow

```text
OpenSky /states/all (Europe, default 60-second poll; 10-second demo override)
  -> ingestor: validate + normalize + deterministic event ID
  -> Redpanda aircraft.position.v1 (keyed by ICAO24)
  -> density-builder: event-time latest-sample window (180s), H3 aggregation (r=4)
  -> Redpanda mobility.density.v1 + demand.region.v1 (keyed by H3 cell)
  -> state builder: reject duplicate/stale events
  -> Redis latest-state hashes/index + update channels for aircraft and mobility
  -> API projection feeds: aircraft + mobility WebSocket upsert/remove fan-out
  -> Next.js: snapshot-first recovery, interpolation, and mobility cell reducer
  -> MapLibre symbol + polygon layers (aircraft, density, demand)
```

Redpanda is the retained event boundary. Redis is disposable query state. A
state builder can replay retained events to reconstruct Redis without changing
the provider or API contracts.

## Process ownership

- `services/ingestor` owns provider access, row-level validation, normalization,
  retry/backoff, quota handling, and acknowledged publication.
- `services/density-builder` owns event-time aircraft-to-H3 aggregation,
  deterministic demand modelling, replay-window seek, and sparse publish/retry.
- `services/state-builder` owns consumer offsets and atomic per-aircraft/source
  timestamp monotonicity. It commits only after projection success; malformed
  contract messages are observable and committed so a poison record cannot halt
  a partition.
- `services/api` owns Redis reads, source freshness, expiry removal, HTTP health,
  Prometheus metrics, one Redis Pub/Sub subscription, and bounded client fan-out.
- `web` owns reconnect snapshots, delta reduction, delayed interpolation,
  mobility mode selection, polygon selection, stale styling, and map/source
  failure presentation.

## Contracts and ordering

`contracts/asyncapi.yaml` defines `aircraft.position.v1`, `mobility.density.v1`,
`demand.region.v1`, and both WebSocket streams. The aircraft Kafka key is
normalized ICAO24; mobility and demand are keyed by H3 `cell_id`. Event IDs are
deterministic and timestamp monotonicity prevents stale regressions.

Every browser connection starts with `stream.ready`. A reconnect first replaces
local state from `GET /api/v1/aircraft`, then applies `aircraft.upsert`,
`aircraft.remove`, and heartbeat messages. WebSocket delivery is transient;
Redpanda, not WebSocket or Redis Pub/Sub, is the retained stream.

## Redis layout

- `aether:aircraft:{icao24}`: observation timestamp, event ID, projected JSON.
- `aether:aircraft:index`: sorted set scored by observation epoch milliseconds.
- `aether:source:opensky`: newest source observation and event ID.
- `aether:aircraft:updates`: transient accepted-projection notifications.
- `aether:mobility:{cell_id}`: joined density+demand payload plus per-topic
  window fields.
- `aether:mobility:index`: sorted set scored by joined window epoch
  milliseconds.
- `aether:mobility:updates`: transient mobility upsert/remove notifications.

Lua scripts make timestamp comparison plus projection writes atomic. The API
periodically removes expired hashes/index members atomically and emits a removal
message to connected browsers.

## Failure behavior

- OpenSky transport and broker errors use bounded jittered backoff.
- Provider `429` retry guidance takes precedence over the normal poll interval.
- Missing coordinates remain valid telemetry but do not enter the map projection.
- Redis readiness failure returns `503`; liveness remains independent.
- Slow WebSocket clients have bounded queues and cannot block other clients.
- Map-style failure does not hide source or gateway status.
- Fixture mode is explicit and rejected in production.

## Observability

Prometheus scrapes ingestor `:9101`, state builder `:9102`, and API `:8000`.
The services expose ingestion counts/duration/rejections, consumer/projection
outcomes, snapshot metrics, Redis errors, and process metrics. Ingestor logs are
structured JSON. Cross-process OpenTelemetry collection is deferred to the
infrastructure-telemetry phase.

## Evolution

Phase 3 adds a separate consumer that derives geographic density without
changing `aircraft.position.v1`. Later consumers add mobility forecasts,
workload demand, historical PostGIS storage, replay, and controller inputs.
Controller proposals will pass through a deterministic safety layer before any
future Kubernetes executor receives them.
