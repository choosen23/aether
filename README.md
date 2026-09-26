# Aether

Aether is a real-time geospatial digital twin for mobility-driven cloud
workloads. This repository now includes geographic density and modelled demand:
anonymous OpenSky aircraft observations flow through Redpanda, a density
aggregator derives sparse H3 cells, and a Redis latest-state projection powers
FastAPI snapshot/WebSocket APIs and live MapLibre layers for aircraft, density,
and demand over Europe.

The long-term research question is whether mobility-aware predictive
orchestration can outperform reactive cloud orchestration as demand moves
geographically. Demand generation, regional simulation, Kubernetes control,
controller baselines, and MARL remain deliberately outside this slice.

## Data provenance

- **Real:** OpenSky identity, position, velocity, heading, altitude, and source
  timestamps when the provider supplies them.
- **Derived:** freshness classification and bounded browser interpolation
  between two real observations.
- **Simulated:** none at runtime. Fixture mode is explicit, test-only, and
  forbidden when `AETHER_ENVIRONMENT=production`.

The interface stops interpolation when it cannot safely bridge observations;
it never silently replaces a failed live source with fixture data.

## Prerequisites

- Docker Engine with Docker Compose v2
- Node.js 22+ and npm for frontend-only development
- Python 3.13 and `uv` for backend-only development

## Start locally

```bash
cp .env.example .env
docker compose up --build
```

Open:

- Control room: <http://localhost:3001>
- API and OpenAPI: <http://localhost:8000/docs>
- Readiness: <http://localhost:8000/health/ready>
- API metrics: <http://localhost:8000/metrics>
- Prometheus: `docker compose --profile observability up --build`, then
  <http://localhost:9090>

Anonymous OpenSky queries over the Europe bounding box cost four credits per
poll under the current provider rules. With a 400-credit daily anonymous
allowance, a continuously running stack at the default 60-second interval has
materially lower quota pressure than short-demo polling. A `429` response is
honored and shown as stale/offline data; it does not activate fixtures.
Optional OAuth environment variables are reserved for a later authenticated
provider increment and are not used by this implementation.

The basemap uses OpenFreeMap's public service and requires internet access.
Override `NEXT_PUBLIC_MAP_STYLE_URL` before building the web image to use a
different MapLibre-compatible style.

For frontend-only development, run `npm --prefix web run dev -- --port 3001`
and open <http://localhost:3001>. The API and stream will remain offline unless
the backend services are also running.

## Verify

```bash
make test
make lint
make smoke
```

`make test` runs Python unit/contract tests and Vitest. `make lint` runs Ruff,
mypy, ESLint, and TypeScript. `make smoke` requires Redis/Redpanda-compatible
dependencies plus an installed Playwright browser; failures are not converted
to successful exits.

Automated tests use recorded OpenSky fixtures and must not call the live API.

## Configuration

All local settings are documented in `.env.example`. Important groups are:

- OpenSky base URL and poll interval;
- Redpanda broker/topic and Redis namespace;
- live/stale/offline/removal thresholds;
- allowed HTTP/WebSocket origins;
- frontend API, WebSocket, and map-style URLs;
- explicit `AETHER_SOURCE_MODE=fixture` for local test journeys only.

Never commit `.env`; it is ignored because later authenticated provider values
will be secrets.

## Operations and troubleshooting

- `docker compose ps` shows dependency health.
- `docker compose logs -f ingestor state-builder api` follows the data path.
- If aircraft stop updating, inspect ingestor logs for `429`, then check source
  age in the UI. Retain last-known state; do not fabricate updates.
- If the map reports unavailable, verify the configured style URL and internet
  access. Stream/source health remains visible independently.
- If readiness is `503`, check Redis health and the configured `REDIS_URL`.
- To reset only Aether's disposable local containers and volumes, run
  `docker compose down --volumes`. This deletes the local Redis projection and
  Redpanda retention; both are recoverable only from a still-running upstream
  feed in this slice.

See [architecture](docs/architecture.md), [data provenance](docs/data-provenance.md),
the [H3 ADR](docs/adr/0005-use-h3-for-mobility-aggregation.md), and the accepted
design docs under `docs/superpowers/specs/`.

Simulation experiment runbooks:

- [Trace preparation](docs/experiments/trace-preparation.md)
- [Acceptance run](docs/experiments/acceptance-run.md)
- [Deterministic boundary ADR](docs/adr/0006-deterministic-simulation-boundary.md)

## Phase order

The next increment is geographic aggregation and a demand heatmap. Workload
generation, regional cloud simulation, Kubernetes execution, deterministic
controllers, replay, MARL, and chaos follow in that order; reinforcement
learning is not introduced before deterministic environments and baselines work.
