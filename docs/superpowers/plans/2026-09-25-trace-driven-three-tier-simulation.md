# Trace-Driven Three-Tier Simulation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic, trace-driven experimental platform that replays separate Azure Functions and Google Borg workloads across a fixed European far-edge, edge, and cloud topology and compares four scheduling policies with fully attributable metrics.

**Architecture:** Add a focused `aether-simulator` Python workspace service whose offline preparation CLI converts official-schema source data into canonical Parquet artifacts and whose discrete-event engine consumes those artifacts, recorded mobility, topology, and run configuration without depending on the live ingestor. The engine writes immutable run bundles first and publishes versioned observation events to Redpanda second; the existing state-builder, FastAPI, WebSocket, and Next.js layers project those events into a read-only experiment workspace.

**Tech Stack:** Python 3.13, Pydantic 2, PyArrow/Parquet, aiokafka/Redpanda, Redis 7, FastAPI, Prometheus, pytest, Next.js 16, React 19, TypeScript 5.9, MapLibre GL 6, Vitest, Playwright

**Spec:** `docs/superpowers/specs/2026-09-25-trace-driven-three-tier-simulation-design.md`

## Global Constraints

- Azure Functions and Google Borg are separate scenarios; never mix both traces into one run.
- OpenSky supplies observed mobility only; geographic assignment is always `modelled`, and infrastructure behavior is always `simulated`.
- The topology schema must support arbitrary node and directed-link counts; no tier count may be hard-coded.
- `eu-small-v1` contains exactly 20 far-edge sites, 8 edge sites, and 4 cloud sites; `eu-medium-v1` remains compatible with the same schema.
- Sampling changes workload count, not resource requests; time acceleration is an independent recorded value.
- Identical inputs, configuration, simulator version, and seed must produce byte-identical prepared artifacts and equivalent result summaries.
- Compute resources and execution slots are reserved atomically only after required input transfer completes; physical capacity must never be exceeded.
- Network simulation is flow-level, directed, congestion-aware, failure-aware, and deterministic; packet and protocol simulation are excluded.
- Completed run directories are immutable and contain configuration, manifests, checksums, time series, summary metrics, provenance, and stopping reason.
- The web application is read-only; run start, pause, resume, and cancellation are CLI operations only.
- Automated tests use repository fixtures and generated mobility snapshots; they never download public traces or call OpenSky.
- Existing Python constraints remain `>=3.13,<3.14`; existing frontend major versions and project lint/type-check rules remain unchanged.

## Review Focus

- A Parquet artifact containing duplicate `workload_id` values must fail before simulation starts and identify the duplicate.
- A live mobility distribution older than its configured maximum age must pause the run at the last committed simulated timestamp without inventing origins.
- A topology with valid nodes but a directionally disconnected origin/destination must queue until deadline or reject deterministically, never reverse a directed link implicitly.
- Simultaneous flow completion and link failure at the same simulated timestamp must use the documented event priority and produce the same result across runs.
- A workload individually feasible on a node but blocked by concurrent CPU, memory, GPU, or slot use must queue without partially reserving any resource.

---

## File Structure

The simulator package is split by scientific responsibility rather than by framework layer:

- `packages/aether-contracts/src/aether_contracts/simulation.py`: canonical workload, topology, run configuration, event, snapshot, and result contracts.
- `services/simulator/src/aether_simulator/preparation.py`: deterministic preparation pipeline and manifest/checksum writer.
- `services/simulator/src/aether_simulator/adapters/{azure_functions,borg}.py`: source-specific normalization only.
- `services/simulator/src/aether_simulator/mobility.py`: recorded mobility loading and deterministic origin assignment.
- `services/simulator/src/aether_simulator/topology.py`: topology validation, directed adjacency, and cached path selection.
- `services/simulator/src/aether_simulator/network.py`: flow state, fair bandwidth allocation, progress, failures, and recovery.
- `services/simulator/src/aether_simulator/compute.py`: node resource ledger and workload lifecycle transitions.
- `services/simulator/src/aether_simulator/policies.py`: immutable scheduling view and four pure policy implementations.
- `services/simulator/src/aether_simulator/engine.py`: event queue and deterministic orchestration.
- `services/simulator/src/aether_simulator/metrics.py`: component metrics, percentiles, and combined objective.
- `services/simulator/src/aether_simulator/artifacts.py`: immutable run-bundle serialization and comparison reports.
- `services/simulator/src/aether_simulator/publisher.py`: best-effort Redpanda observation publishing after deterministic state commits.
- `services/simulator/src/aether_simulator/cli.py`: `prepare`, `run`, `pause`, `resume`, `cancel`, and `compare` commands.
- `topologies/*.json`: versioned external topology documents.
- `services/state-builder/src/aether_state_builder/simulation_*`: rebuildable Redis projection of experiment events.
- `services/api/src/aether_api/simulation_*`: read-only REST/WebSocket experiment API.
- `web/lib/simulation-*` and `web/components/experiment-*`: typed client/store and read-only experiment workspace.

### Task 1: Canonical Simulation Contracts and Workspace Registration

**Files:**
- Create: `packages/aether-contracts/src/aether_contracts/simulation.py`
- Modify: `packages/aether-contracts/src/aether_contracts/__init__.py`
- Create: `services/simulator/pyproject.toml`
- Create: `services/simulator/src/aether_simulator/__init__.py`
- Modify: `pyproject.toml`
- Create: `tests/unit/contracts/test_simulation_contracts.py`

**Interfaces:**
- Consumes: existing Pydantic UTC serialization conventions from `aether_contracts.mobility`.
- Produces: `CanonicalWorkload`, `FieldProvenance`, `TopologyDocument`, `TopologyNode`, `TopologyLink`, `ObjectiveWeights`, `RunConfiguration`, `SimulationEvent`, `RunSnapshot`, and `RunSummary`.

- [ ] **Step 1: Write failing contract tests**

```python
def test_canonical_workload_rejects_negative_resources() -> None:
    with pytest.raises(ValidationError, match="greater than or equal to 0"):
        CanonicalWorkload.model_validate({**WORKLOAD, "requested_memory_mb": -1})


def test_topology_rejects_dangling_link() -> None:
    with pytest.raises(ValidationError, match="unknown destination node missing"):
        TopologyDocument.model_validate({**TOPOLOGY, "links": [{**LINK, "destination": "missing"}]})


def test_run_configuration_rejects_mixed_trace_sources() -> None:
    with pytest.raises(ValidationError, match="exactly one source trace"):
        RunConfiguration.model_validate({**RUN, "source_traces": ["azure_functions", "google_borg"]})
```

- [ ] **Step 2: Run the focused tests and confirm missing imports**

Run: `uv run --group dev pytest tests/unit/contracts/test_simulation_contracts.py -q`

Expected: collection fails because `aether_contracts.simulation` does not exist.

- [ ] **Step 3: Implement strict versioned contracts**

```python
class CanonicalWorkload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[1] = 1
    workload_id: str
    scenario_id: str
    source_trace: Literal["azure_functions", "google_borg"]
    source_record_id: str
    arrival_time_ms: Annotated[int, Field(ge=0)]
    expected_duration_ms: Annotated[int, Field(gt=0)]
    deadline_ms: Annotated[int, Field(gt=0)]
    priority: int
    requested_cpu_millicores: Annotated[int, Field(ge=0)]
    requested_memory_mb: Annotated[int, Field(ge=0)]
    requested_gpu_units: Annotated[int, Field(ge=0)] = 0
    input_data_bytes: Annotated[int, Field(ge=0)]
    output_data_bytes: Annotated[int, Field(ge=0)]
    movable_state_bytes: Annotated[int, Field(ge=0)]
    preemptible: bool
    restartable: bool
    migration_allowed: bool
    origin_h3_cell: str | None = None
    origin_latitude: float | None = None
    origin_longitude: float | None = None
    field_provenance: dict[str, Literal["observed", "trace_derived", "modelled", "simulated"]]
    source_trace_version: str
    adapter_version: str
```

Add validators for UTC-normalized run timestamps, unique node/link identifiers, coordinate ranges, non-negative capacity/network/cost/energy fields, dangling links, positive weight sums, and the single-trace rule. Export every public contract from `aether_contracts.__init__` and register `aether-simulator` in the uv workspace, pytest path, mypy path, and source table.

- [ ] **Step 4: Run contract tests and static checks**

Run: `uv run --group dev pytest tests/unit/contracts/test_simulation_contracts.py -q && uv run --group dev ruff check packages/aether-contracts services/simulator tests/unit/contracts/test_simulation_contracts.py && uv run --group dev mypy packages/aether-contracts services/simulator`

Expected: all commands pass.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml packages/aether-contracts services/simulator tests/unit/contracts/test_simulation_contracts.py uv.lock
git commit -m "feat: add simulation contracts"
```

### Task 2: Deterministic Trace Preparation Core

**Files:**
- Create: `services/simulator/src/aether_simulator/preparation.py`
- Create: `services/simulator/src/aether_simulator/checksums.py`
- Create: `services/simulator/src/aether_simulator/manifest.py`
- Create: `tests/unit/simulator/test_preparation.py`
- Modify: `pyproject.toml`
- Modify: `services/simulator/pyproject.toml`

**Interfaces:**
- Consumes: `CanonicalWorkload` from Task 1 and adapter callable `normalize(record: Mapping[str, object], context: AdapterContext) -> CanonicalWorkload`.
- Produces: `prepare_trace(config: PreparationConfig, adapter: TraceAdapter) -> PreparationResult`, deterministic Parquet bytes, and `PreparationManifest` JSON.

- [ ] **Step 1: Write failing determinism and duplicate tests**

```python
def test_preparation_is_byte_identical(tmp_path: Path) -> None:
    first = prepare_trace(CONFIG.with_output(tmp_path / "a"), FakeAdapter())
    second = prepare_trace(CONFIG.with_output(tmp_path / "b"), FakeAdapter())
    assert first.artifact_sha256 == second.artifact_sha256
    assert (tmp_path / "a/workloads.parquet").read_bytes() == (tmp_path / "b/workloads.parquet").read_bytes()


def test_preparation_rejects_duplicate_workload_ids(tmp_path: Path) -> None:
    with pytest.raises(DuplicateWorkloadIdError, match="workload-001"):
        prepare_trace(CONFIG.with_output(tmp_path), DuplicateAdapter())


def test_malformed_rows_are_reported_by_reason(tmp_path: Path) -> None:
    result = prepare_trace(CONFIG.with_output(tmp_path), OneMalformedAdapter())
    assert result.manifest.rejected_rows == {"missing_duration": 1}
```

- [ ] **Step 2: Run tests and confirm the preparation module is absent**

Run: `uv run --group dev pytest tests/unit/simulator/test_preparation.py -q`

Expected: collection fails with `ModuleNotFoundError`.

- [ ] **Step 3: Implement deterministic preparation**

```python
class TraceAdapter(Protocol):
    name: str
    version: str
    def validate_schema(self, columns: frozenset[str]) -> None: ...
    def records(self, source: Path, partition: str) -> Iterator[Mapping[str, object]]: ...
    def normalize(self, record: Mapping[str, object], context: AdapterContext) -> CanonicalWorkload: ...


def selected(source_record_id: str, scale_factor: float, seed: int) -> bool:
    digest = sha256(f"{seed}:{source_record_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64 < scale_factor
```

Verify the source checksum before reading, validate exact required columns, sort accepted workloads by `(arrival_time_ms, workload_id)`, write PyArrow with a fixed schema and stable row-group settings, normalize manifest JSON with sorted keys and compact separators, fsync temporary files, and atomically rename them. Store source and output checksums, source version, adapter version, partition, time window, scale factor, seed, counts, and rejection reason codes. Add `pyarrow>=18,<19` to the root and simulator dependencies.

- [ ] **Step 4: Run focused tests twice**

Run: `uv run --group dev pytest tests/unit/simulator/test_preparation.py -q && uv run --group dev pytest tests/unit/simulator/test_preparation.py -q`

Expected: both runs pass with identical fixture checksums.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml services/simulator tests/unit/simulator/test_preparation.py uv.lock
git commit -m "feat: add deterministic trace preparation"
```

### Task 3: Azure Functions and Google Borg Adapters

**Files:**
- Create: `services/simulator/src/aether_simulator/adapters/__init__.py`
- Create: `services/simulator/src/aether_simulator/adapters/azure_functions.py`
- Create: `services/simulator/src/aether_simulator/adapters/borg.py`
- Create: `tests/fixtures/traces/azure_functions_official_schema.csv`
- Create: `tests/fixtures/traces/borg_official_schema.csv`
- Create: `tests/fixtures/prepared/azure/workloads.parquet`
- Create: `tests/fixtures/prepared/azure/manifest.json`
- Create: `tests/fixtures/prepared/borg/workloads.parquet`
- Create: `tests/fixtures/prepared/borg/manifest.json`
- Create: `tests/unit/simulator/test_azure_adapter.py`
- Create: `tests/unit/simulator/test_borg_adapter.py`

**Interfaces:**
- Consumes: `TraceAdapter`, `AdapterContext`, and `CanonicalWorkload` from Tasks 1–2.
- Produces: `AzureFunctionsAdapter(version="azure-v1")` and `BorgAdapter(version="borg-v1")`.

- [ ] **Step 1: Add official-schema fixture tests**

```python
def test_azure_preserves_trace_fields_and_models_missing_fields() -> None:
    adapter = AzureFunctionsAdapter()
    record = next(adapter.records(FIXTURE, partition="fixture"))
    item = adapter.normalize(record, CONTEXT)
    assert item.source_trace == "azure_functions"
    assert item.field_provenance["expected_duration_ms"] == "trace_derived"
    assert item.field_provenance["requested_cpu_millicores"] == "modelled"
    assert item.restartable is True


def test_borg_uses_trace_resources_and_priority() -> None:
    adapter = BorgAdapter()
    record = next(adapter.records(FIXTURE, partition="fixture"))
    item = adapter.normalize(record, CONTEXT)
    assert item.source_trace == "google_borg"
    assert item.requested_cpu_millicores == 500
    assert item.priority == 7
    assert item.field_provenance["requested_cpu_millicores"] == "trace_derived"
```

Also assert stable IDs equal `sha256(f"{scenario_id}:{source_record_id}:{adapter_version}:{seed}")[:26]`, malformed numeric rows receive explicit reason codes, and neither adapter changes resource requests during sampling.

- [ ] **Step 2: Run tests and confirm missing adapters**

Run: `uv run --group dev pytest tests/unit/simulator/test_azure_adapter.py tests/unit/simulator/test_borg_adapter.py -q`

Expected: collection fails because both adapter modules are absent.

- [ ] **Step 3: Implement named adapter models**

```python
AZURE_MODEL_VERSION = "azure-missing-fields-v1"
BORG_MODEL_VERSION = "borg-missing-fields-v1"

def stable_workload_id(context: AdapterContext, source_record_id: str) -> str:
    value = f"{context.scenario_id}:{source_record_id}:{context.adapter_version}:{context.seed}"
    return sha256(value.encode()).hexdigest()[:26]
```

For Azure, derive arrival/duration/application memory from trace columns and deterministically model CPU, deadline, input/output/state bytes, preemption, and migration from stable record hashes. For Borg, derive submission/duration/requested CPU/memory/priority and scheduling flags when present; model only absent deadline and transfer/state fields. Populate provenance for every canonical field and reject non-finite or invalid source values with a reason code. Run `prepare_trace` against both tiny fixtures and commit their deterministic prepared outputs under `tests/fixtures/prepared`; these are the only prepared artifacts used by automated compare tests.

- [ ] **Step 4: Run adapter and preparation tests**

Run: `uv run --group dev pytest tests/unit/simulator/test_azure_adapter.py tests/unit/simulator/test_borg_adapter.py tests/unit/simulator/test_preparation.py -q`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/simulator/src/aether_simulator/adapters tests/fixtures/traces tests/unit/simulator
git commit -m "feat: normalize azure and borg traces"
```

### Task 4: Recorded Mobility and Deterministic Origin Assignment

**Files:**
- Create: `services/simulator/src/aether_simulator/mobility.py`
- Create: `tests/fixtures/simulation/mobility.jsonl`
- Create: `tests/unit/simulator/test_mobility.py`

**Interfaces:**
- Consumes: canonical workload identity and existing `MobilityDensityEvent` JSON shape.
- Produces: `MobilityTimeline.from_jsonl(path)`, `distribution_at(simulated_ms, max_age_ms)`, and `assign_origin(workload, distribution, seed) -> CanonicalWorkload`.

- [ ] **Step 1: Write failing weighted-assignment and stale-bound tests**

```python
def test_assignment_is_stable_and_marked_modelled() -> None:
    first = assign_origin(WORKLOAD, DISTRIBUTION, seed=42)
    second = assign_origin(WORKLOAD, DISTRIBUTION, seed=42)
    assert first == second
    assert first.origin_h3_cell in {"85194ad3fffffff", "85194e67fffffff"}
    assert first.field_provenance["origin_h3_cell"] == "modelled"


def test_distribution_pauses_after_last_valid_bound() -> None:
    timeline = MobilityTimeline.from_jsonl(FIXTURE)
    with pytest.raises(MobilityExpiredError, match="last valid snapshot exceeded 30000 ms"):
        timeline.distribution_at(simulated_ms=90_001, max_age_ms=30_000)
```

- [ ] **Step 2: Run tests and verify failure**

Run: `uv run --group dev pytest tests/unit/simulator/test_mobility.py -q`

Expected: collection fails because `aether_simulator.mobility` does not exist.

- [ ] **Step 3: Implement weighted selection without mutable RNG state**

```python
def deterministic_unit_interval(seed: int, workload_id: str, window_ms: int) -> float:
    digest = sha256(f"{seed}:{workload_id}:{window_ms}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64
```

Validate non-empty positive density weights, sort cells by H3 ID before cumulative selection, obtain cell centers through `h3.cell_to_latlng`, copy the frozen workload with all three origin fields populated, and mark those fields `modelled`. Select the newest snapshot not newer than simulated time; raise `MobilityExpiredError` once the age bound is exceeded.

- [ ] **Step 4: Run mobility tests**

Run: `uv run --group dev pytest tests/unit/simulator/test_mobility.py -q`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/simulator/src/aether_simulator/mobility.py tests/fixtures/simulation/mobility.jsonl tests/unit/simulator/test_mobility.py
git commit -m "feat: assign deterministic workload origins"
```

### Task 5: Versioned European Topology and Directed Routing

**Files:**
- Create: `topologies/schema-v1.json`
- Create: `topologies/eu-small-v1.json`
- Create: `topologies/eu-medium-v1.json`
- Create: `services/simulator/src/aether_simulator/topology.py`
- Create: `tests/unit/simulator/test_topology.py`
- Create: `tests/contract/test_topology_documents.py`

**Interfaces:**
- Consumes: `TopologyDocument`, `TopologyNode`, and `TopologyLink`.
- Produces: `TopologyGraph(document)`, `shortest_path(source, destination, criterion, failed_nodes, failed_links) -> PathResult | None`, and `invalidate(failure_domain)`.

- [ ] **Step 1: Write failing profile, asymmetry, cache, and validation tests**

```python
def test_eu_small_has_required_tier_counts() -> None:
    topology = TopologyDocument.model_validate_json(EU_SMALL.read_text())
    counts = Counter(node.tier for node in topology.nodes)
    assert counts == {"far_edge": 20, "edge": 8, "cloud": 4}


def test_paths_are_directional() -> None:
    graph = TopologyGraph(ASYMMETRIC_TOPOLOGY)
    assert graph.shortest_path("a", "b", "latency").node_ids == ("a", "b")
    assert graph.shortest_path("b", "a", "latency") is None


def test_link_failure_invalidates_only_affected_cached_paths() -> None:
    before = graph.shortest_path("a", "c", "latency")
    graph.fail_link("ab")
    after = graph.shortest_path("a", "c", "latency")
    assert before.link_ids != after.link_ids
```

Also cover duplicate IDs, missing nodes, invalid coordinates, negative values, and disconnected paths.

- [ ] **Step 2: Run tests and verify missing topology files/module**

Run: `uv run --group dev pytest tests/unit/simulator/test_topology.py tests/contract/test_topology_documents.py -q`

Expected: failures report missing topology documents and module.

- [ ] **Step 3: Add topology documents and deterministic routing**

Use real airport/metro/cloud-region coordinates, explicit `provenance: "simulated"` on capacities/cost/energy/network values, and explicit directed links including redundant lateral paths. Implement adjacency lists and Dijkstra with stable `(score, node_id, path_link_ids)` tie-breaking; support `latency`, `transfer_time`, `cost`, `energy`, and weighted criteria. Cache by source, destination, criterion, topology failure revision, and congestion revision.

```python
@dataclass(frozen=True)
class PathResult:
    node_ids: tuple[str, ...]
    link_ids: tuple[str, ...]
    propagation_latency_ms: float
    bottleneck_bandwidth_bps: float
    transfer_cost: float
    network_energy_joules: float
```

- [ ] **Step 4: Run topology contract and unit tests**

Run: `uv run --group dev pytest tests/unit/simulator/test_topology.py tests/contract/test_topology_documents.py -q`

Expected: all tests pass and both profiles validate against `schema-v1.json`.

- [ ] **Step 5: Commit**

```bash
git add topologies services/simulator/src/aether_simulator/topology.py tests/unit/simulator/test_topology.py tests/contract/test_topology_documents.py
git commit -m "feat: add european simulation topologies"
```

### Task 6: Flow-Level Network Simulator

**Files:**
- Create: `services/simulator/src/aether_simulator/network.py`
- Create: `tests/unit/simulator/test_network.py`

**Interfaces:**
- Consumes: `TopologyGraph` and `PathResult`.
- Produces: `NetworkState.start_flow(...) -> Flow`, `advance(to_ms) -> tuple[FlowCompletion, ...]`, `fail_link(link_id, at_ms)`, and `recover_link(link_id, at_ms)`.

- [ ] **Step 1: Write failing sharing, reroute, and timestamp-order tests**

```python
def test_two_flows_share_the_bottleneck_equally() -> None:
    network = NetworkState(TEN_MEGABIT_GRAPH)
    network.start_flow("f1", "a", "b", byte_count=625_000, at_ms=0)
    network.start_flow("f2", "a", "b", byte_count=625_000, at_ms=0)
    assert network.advance(1_000) == (FlowCompletion("f1", 1_000), FlowCompletion("f2", 1_000))


def test_failure_at_completion_timestamp_is_applied_first() -> None:
    network = NetworkState(REDUNDANT_GRAPH)
    network.start_flow("f1", "a", "c", byte_count=1_000, at_ms=0)
    network.fail_link("direct", at_ms=100)
    assert network.flow("f1").path.link_ids == ("ab", "bc")
```

Document event priority as `failure`, `recovery`, `flow_completion`, `workload_arrival`, `execution_completion`, `deadline`, then stable event ID.

- [ ] **Step 2: Run tests and confirm module absence**

Run: `uv run --group dev pytest tests/unit/simulator/test_network.py -q`

Expected: collection fails because the network module does not exist.

- [ ] **Step 3: Implement deterministic max-min link sharing**

Represent each flow with remaining bytes and a current directed path. Before every arrival, failure, recovery, route change, or completion, advance all flows using rates from the previous interval. Recompute each flow rate as the minimum equal share on every link in its path, schedule the next exact completion using integer microseconds, and update topology congestion revision. Failed-path flows reroute if possible or remain blocked with zero bandwidth.

```python
@dataclass(frozen=True)
class Flow:
    flow_id: str
    source: str
    destination: str
    path: PathResult
    started_at_ms: int
    remaining_bytes: int
    purpose: Literal["input", "output", "migration"]
```

- [ ] **Step 4: Run network and topology tests**

Run: `uv run --group dev pytest tests/unit/simulator/test_network.py tests/unit/simulator/test_topology.py -q`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/simulator/src/aether_simulator/network.py tests/unit/simulator/test_network.py
git commit -m "feat: simulate directed network flows"
```

### Task 7: Atomic Compute Ledger and Workload Lifecycle

**Files:**
- Create: `services/simulator/src/aether_simulator/compute.py`
- Create: `tests/unit/simulator/test_compute.py`

**Interfaces:**
- Consumes: `CanonicalWorkload` and topology node capacities.
- Produces: `ComputeState.can_fit`, `reserve`, `release`, `enqueue`, `start`, `complete`, `reject`, `fail`, and `begin_migration` with immutable `WorkloadRuntime` snapshots.

- [ ] **Step 1: Write failing atomicity and lifecycle tests**

```python
def test_failed_reservation_changes_no_resource() -> None:
    state = ComputeState.from_nodes([NODE_WITH_ONE_GPU])
    state.reserve("node-a", GPU_WORKLOAD)
    before = state.node("node-a")
    with pytest.raises(CapacityUnavailableError):
        state.reserve("node-a", SECOND_GPU_WORKLOAD)
    assert state.node("node-a") == before


def test_oversized_workload_is_unschedulable() -> None:
    state = ComputeState.from_nodes([SMALL_NODE])
    result = state.classify(OVERSIZED_WORKLOAD, eligible_node_ids=("small",))
    assert result == "unschedulable"
```

Also assert valid transition paths, queue deadline expiry, migration bandwidth handoff, preemption eligibility, restart behavior, and maximum CPU/memory/GPU/slot use never exceeding capacity.

- [ ] **Step 2: Run tests and verify module absence**

Run: `uv run --group dev pytest tests/unit/simulator/test_compute.py -q`

Expected: collection fails because the compute module does not exist.

- [ ] **Step 3: Implement resource vectors and guarded transitions**

```python
@dataclass(frozen=True)
class ResourceVector:
    cpu_millicores: int
    memory_mb: int
    gpu_units: int
    slots: int = 1


ALLOWED_TRANSITIONS = {
    "arrived": {"placed", "rejected", "unschedulable"},
    "placed": {"transferring", "queued", "failed"},
    "transferring": {"queued", "failed", "rejected"},
    "queued": {"running", "rejected", "failed"},
    "running": {"completed", "transferring", "failed"},
}
```

Compute a proposed complete resource vector before mutation; commit all four dimensions together or raise without mutation. Release exactly once on completion, migration, preemption, or failure. Keep queue order external so policies own admission order.

- [ ] **Step 4: Run compute tests**

Run: `uv run --group dev pytest tests/unit/simulator/test_compute.py -q`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/simulator/src/aether_simulator/compute.py tests/unit/simulator/test_compute.py
git commit -m "feat: add atomic compute simulation"
```

### Task 8: Four Pure Baseline Scheduling Policies

**Files:**
- Create: `services/simulator/src/aether_simulator/policies.py`
- Create: `tests/unit/simulator/test_policies.py`

**Interfaces:**
- Consumes: frozen `SchedulingView(workload, nodes, paths, queues, weights, now_ms)` built from Tasks 5–7.
- Produces: `PlacementDecision(node_id, action, explanation, score_components)` through `Scheduler.decide(view)`.

- [ ] **Step 1: Write failing policy tests over the same immutable view**

```python
@pytest.mark.parametrize("policy,expected", [
    (CloudOnly(), "cloud-near"),
    (NearestFeasible(), "far-edge"),
    (ResourceFirst(), "edge-empty"),
    (Balanced(), "edge-balanced"),
])
def test_baseline_policy_choice(policy: Scheduler, expected: str) -> None:
    original = deepcopy(VIEW)
    assert policy.decide(VIEW).node_id == expected
    assert VIEW == original


def test_ties_are_broken_by_node_id() -> None:
    decision = NearestFeasible().decide(TIED_VIEW)
    assert decision.node_id == "edge-a"
```

- [ ] **Step 2: Run tests and confirm policy module is absent**

Run: `uv run --group dev pytest tests/unit/simulator/test_policies.py -q`

Expected: collection fails because the policies module does not exist.

- [ ] **Step 3: Implement four deterministic policies**

`cloud_only` filters tier `cloud` then ranks feasible nodes by end-to-end latency. `nearest_feasible` ranks every feasible node by propagation + transfer + queue + execution estimate. `resource_first` ranks descending normalized post-placement headroom and queue capacity, then latency. `balanced` computes the recorded weighted normalized sum of compute pressure, path latency, transfer time, queue delay, deadline risk, migration cost, transfer cost, and energy. Each returns component scores and a human-readable explanation; no policy mutates state.

```python
class Scheduler(Protocol):
    name: str
    version: str
    def decide(self, view: SchedulingView) -> PlacementDecision: ...
```

- [ ] **Step 4: Run policy, network, and compute tests**

Run: `uv run --group dev pytest tests/unit/simulator/test_policies.py tests/unit/simulator/test_network.py tests/unit/simulator/test_compute.py -q`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/simulator/src/aether_simulator/policies.py tests/unit/simulator/test_policies.py
git commit -m "feat: add baseline scheduling policies"
```

### Task 9: Discrete-Event Engine, Failures, Pause, and Migration

**Files:**
- Create: `services/simulator/src/aether_simulator/events.py`
- Create: `services/simulator/src/aether_simulator/engine.py`
- Create: `tests/unit/simulator/test_engine.py`
- Create: `tests/fixtures/simulation/failure-schedule.json`

**Interfaces:**
- Consumes: prepared workloads, `MobilityTimeline`, `TopologyGraph`, `NetworkState`, `ComputeState`, and `Scheduler`.
- Produces: `SimulationEngine.run(until_ms=None) -> EngineResult`, `pause(reason)`, `resume()`, `cancel(reason)`, and ordered `SimulationEvent` records.

- [ ] **Step 1: Write failing end-to-end engine invariants**

```python
def test_same_inputs_produce_equivalent_events() -> None:
    first = engine_for(seed=17).run()
    second = engine_for(seed=17).run()
    assert [e.model_dump(mode="json") for e in first.events] == [e.model_dump(mode="json") for e in second.events]


def test_expired_mobility_pauses_at_last_committed_time() -> None:
    result = engine_with_expired_mobility().run()
    assert result.status == "paused"
    assert result.stopping_reason == "mobility_snapshot_expired"
    assert result.simulated_time_ms == result.last_committed_time_ms


def test_disconnected_workload_rejects_at_deadline() -> None:
    result = disconnected_engine().run()
    assert result.workload("w1").status == "rejected"
    assert result.workload("w1").reason == "deadline_expired_while_disconnected"
```

Cover node/link failure and recovery, restartable and non-restartable work, migration state transfer before destination reservation, preemption, unschedulable classification, cancellation, and event-priority ties.

- [ ] **Step 2: Run engine tests and confirm missing engine**

Run: `uv run --group dev pytest tests/unit/simulator/test_engine.py -q`

Expected: collection fails because the engine module does not exist.

- [ ] **Step 3: Implement the stable event queue**

```python
EVENT_PRIORITY = {
    "node_failed": 0,
    "link_failed": 0,
    "node_recovered": 1,
    "link_recovered": 1,
    "flow_completed": 2,
    "workload_arrived": 3,
    "execution_completed": 4,
    "deadline_reached": 5,
}

@dataclass(order=True, frozen=True)
class QueuedEvent:
    at_ms: int
    priority: int
    event_id: str
    kind: str = field(compare=False)
    payload: Mapping[str, object] = field(compare=False)
```

Commit engine state before emitting an observation. Treat a node failure as resource release plus restart/migrate/fail policy evaluation. Migration starts a network flow for `movable_state_bytes`; destination compute reservation occurs only after flow completion. Persist the last committed time on every processed event and stop cleanly on pause/cancel.

- [ ] **Step 4: Run all simulator unit tests**

Run: `uv run --group dev pytest tests/unit/simulator -q`

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/simulator/src/aether_simulator/events.py services/simulator/src/aether_simulator/engine.py tests/unit/simulator/test_engine.py tests/fixtures/simulation/failure-schedule.json
git commit -m "feat: add deterministic simulation engine"
```

### Task 10: Metrics, Immutable Run Bundles, CLI, and Eight-Run Comparison

**Files:**
- Create: `services/simulator/src/aether_simulator/metrics.py`
- Create: `services/simulator/src/aether_simulator/artifacts.py`
- Create: `services/simulator/src/aether_simulator/cli.py`
- Modify: `services/simulator/pyproject.toml`
- Create: `tests/unit/simulator/test_metrics.py`
- Create: `tests/unit/simulator/test_artifacts.py`
- Create: `tests/unit/simulator/test_cli.py`
- Create: `configs/experiments/eu-small-acceptance-v1.yaml`

**Interfaces:**
- Consumes: `EngineResult`, preparation/topology manifests, configuration, and four scheduler names.
- Produces: `summarize(result, weights) -> RunSummary`, `write_run_bundle(...) -> Path`, CLI entry point `aether-sim`, and `comparison.json`/`comparison.md`.

- [ ] **Step 1: Write failing metric and immutability tests**

```python
def test_summary_reports_components_with_combined_score() -> None:
    summary = summarize(ENGINE_RESULT, WEIGHTS)
    assert summary.latency_ms.p50 == 10
    assert summary.latency_ms.p95 == 95
    assert summary.combined_score is not None
    assert summary.objective_weights == WEIGHTS


def test_completed_bundle_cannot_be_overwritten(tmp_path: Path) -> None:
    write_run_bundle(tmp_path / "run-1", BUNDLE)
    with pytest.raises(CompletedRunImmutableError, match="run-1"):
        write_run_bundle(tmp_path / "run-1", BUNDLE)
```

Also assert the bundle contains `run-config.json`, `trace-manifest.json`, `topology-manifest.json`, `events.jsonl`, `metrics.parquet`, `summary.json`, `provenance.json`, and `completion.json`; checksum each file; confirm incomplete runs store a stopping reason and last committed timestamp.

- [ ] **Step 2: Run tests and confirm missing modules/entry point**

Run: `uv run --group dev pytest tests/unit/simulator/test_metrics.py tests/unit/simulator/test_artifacts.py tests/unit/simulator/test_cli.py -q`

Expected: collection fails because metrics, artifacts, and CLI modules do not exist.

- [ ] **Step 3: Implement summaries, artifact sealing, and CLI**

Use nearest-rank percentiles over integer microseconds and report completion/rejection/failure/unschedulable rates, deadline misses, queue/network/execution/end-to-end latency, tier/site resource utilization, link utilization, transferred bytes, congestion duration, migration counts/time/bytes, transfer/operating cost, compute/network energy, and the weighted score. Write into a temporary run directory, checksum the complete contents, then atomically rename and create a read-only `COMPLETED` marker.

```toml
[project.scripts]
aether-sim = "aether_simulator.cli:main"
```

The CLI commands are:

```text
aether-sim prepare --source azure-functions|borg --input PATH --expected-sha256 HEX --partition NAME --start ISO --end ISO --scale FLOAT --seed INT --output DIR
aether-sim run --config FILE --artifact DIR --topology FILE --mobility FILE --output DIR
aether-sim pause --run DIR
aether-sim resume --run DIR
aether-sim cancel --run DIR --reason TEXT
aether-sim compare --config FILE --azure-artifact DIR --borg-artifact DIR --output DIR
```

`compare` expands exactly two trace scenarios × four policies and rejects any config that changes topology, mobility, failure schedule, capacity profile, scale, acceleration, or seed between policies.

- [ ] **Step 4: Run CLI tests and a fixture comparison**

Run: `uv run --group dev pytest tests/unit/simulator/test_metrics.py tests/unit/simulator/test_artifacts.py tests/unit/simulator/test_cli.py -q && uv run aether-sim compare --config configs/experiments/eu-small-acceptance-v1.yaml --azure-artifact tests/fixtures/prepared/azure --borg-artifact tests/fixtures/prepared/borg --output /tmp/aether-comparison-test`

Expected: tests pass; command reports eight completed fixture runs and writes one comparison bundle.

- [ ] **Step 5: Commit**

```bash
git add services/simulator configs/experiments tests/unit/simulator pyproject.toml uv.lock
git commit -m "feat: add simulation metrics and run bundles"
```

### Task 11: Redpanda Simulation Events and Rebuildable Redis Projection

**Files:**
- Create: `services/simulator/src/aether_simulator/publisher.py`
- Modify: `packages/aether-contracts/src/aether_contracts/simulation.py`
- Modify: `infra/redpanda/create-topics.sh`
- Modify: `compose.yaml`
- Modify: `.env.example`
- Create: `services/state-builder/src/aether_state_builder/simulation_consumer.py`
- Create: `services/state-builder/src/aether_state_builder/simulation_projection.py`
- Modify: `services/state-builder/src/aether_state_builder/config.py`
- Modify: `services/state-builder/src/aether_state_builder/main.py`
- Create: `tests/unit/simulator/test_publisher.py`
- Create: `tests/unit/state_builder/test_simulation_projection.py`

**Interfaces:**
- Consumes: committed `SimulationEvent` and `RunSnapshot` objects.
- Produces: Redpanda topic `simulation.event.v1` keyed by `run_id`, Redis keys `aether:simulation:runs`, `aether:simulation:run:{run_id}`, and stream `aether:simulation:changes`.

- [ ] **Step 1: Write failing publication/projection tests**

```python
async def test_publish_failure_does_not_change_engine_result() -> None:
    publisher = SimulationPublisher(FailingProducer())
    outcome = await publisher.publish(COMMITTED_EVENT)
    assert outcome.delivered is False
    assert COMMITTED_EVENT.sequence == 17


async def test_projection_ignores_duplicate_sequence(redis: FakeRedis) -> None:
    projection = SimulationProjection(redis, namespace="aether")
    await projection.apply(EVENT_SEQUENCE_4)
    await projection.apply(EVENT_SEQUENCE_4)
    assert await redis.hget("aether:simulation:run:run-1", "last_sequence") == "4"
    assert redis.xadd_count == 1
```

Also test out-of-order sequence rejection and rebuild from retained events.

- [ ] **Step 2: Run tests and confirm missing components**

Run: `uv run --group dev pytest tests/unit/simulator/test_publisher.py tests/unit/state_builder/test_simulation_projection.py -q`

Expected: collection fails on missing publisher/projection modules.

- [ ] **Step 3: Implement observational publishing and projection**

Create the topic with 3 partitions, one replica, and 7-day retention. Publish canonical compact JSON only after the engine has committed local state; retries use stable event IDs and never re-run simulation logic. Project the latest run snapshot, metadata, and summary into Redis with monotonic sequence checks, add run IDs to a sorted set by update time, and append accepted changes to the Redis stream. Extend state-builder with an independent consumer task and its own group ID.

- [ ] **Step 4: Run projection tests and compose validation**

Run: `uv run --group dev pytest tests/unit/simulator/test_publisher.py tests/unit/state_builder/test_simulation_projection.py -q && docker compose config --quiet`

Expected: tests pass and compose configuration is valid.

- [ ] **Step 5: Commit**

```bash
git add services/simulator packages/aether-contracts infra/redpanda compose.yaml .env.example services/state-builder tests/unit/state_builder tests/unit/simulator/test_publisher.py
git commit -m "feat: project simulation events"
```

### Task 12: Read-Only Experiment REST and WebSocket API

**Files:**
- Create: `services/api/src/aether_api/simulation_repository.py`
- Create: `services/api/src/aether_api/simulation_broadcaster.py`
- Create: `services/api/src/aether_api/simulation_feed.py`
- Create: `services/api/src/aether_api/simulation_routes.py`
- Create: `services/api/src/aether_api/simulation_websocket.py`
- Modify: `services/api/src/aether_api/app.py`
- Modify: `services/api/src/aether_api/config.py`
- Create: `tests/unit/api/test_simulation_repository.py`
- Create: `tests/unit/api/test_simulation_routes.py`
- Create: `tests/unit/api/test_simulation_websocket.py`

**Interfaces:**
- Consumes: Redis simulation projection from Task 11.
- Produces: `GET /api/v1/experiments`, `GET /api/v1/experiments/{run_id}`, `GET /api/v1/experiments/{run_id}/results`, and `WS /ws/v1/experiments/{run_id}`.

- [ ] **Step 1: Write failing REST/read-only/WebSocket tests**

```python
async def test_experiment_snapshot_returns_live_and_completed_shape(client: AsyncClient) -> None:
    response = await client.get("/api/v1/experiments/run-1")
    assert response.status_code == 200
    assert response.json()["run_id"] == "run-1"
    assert response.json()["configuration"]["scheduler_policy"] == "balanced"


async def test_mutating_experiment_route_does_not_exist(client: AsyncClient) -> None:
    assert (await client.post("/api/v1/experiments", json={})).status_code == 405
```

WebSocket tests assert `simulation.stream.ready`, monotonic `simulation.run.updated`, heartbeat, slow-client queue replacement, unknown-run close code `4404`, and completed summary delivery.

- [ ] **Step 2: Run tests and verify endpoints are absent**

Run: `uv run --group dev pytest tests/unit/api/test_simulation_repository.py tests/unit/api/test_simulation_routes.py tests/unit/api/test_simulation_websocket.py -q`

Expected: collection fails because simulation API modules do not exist.

- [ ] **Step 3: Implement API using existing feed/broadcaster patterns**

Load ordered run IDs from Redis, validate stored payloads through shared contracts, return `404` for unknown runs and `503` for unavailable projections, and expose no write route. Subscribe to `aether:simulation:changes` in a dedicated lifespan task, broadcast only a run's updates to that run's subscribers, and reuse configured bounded WebSocket queues and heartbeat intervals.

- [ ] **Step 4: Run all API tests**

Run: `uv run --group dev pytest tests/unit/api -q`

Expected: all API tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/api/src/aether_api tests/unit/api
git commit -m "feat: expose read-only experiment api"
```

### Task 13: Read-Only Experiment Workspace and Map Layers

**Files:**
- Create: `web/lib/simulation-contracts.ts`
- Create: `web/lib/simulation-store.ts`
- Create: `web/lib/simulation-client.ts`
- Create: `web/lib/simulation-geometry.ts`
- Create: `web/components/experiment-workspace.tsx`
- Create: `web/components/experiment-map.tsx`
- Create: `web/components/experiment-metrics.tsx`
- Create: `web/components/experiment-comparison.tsx`
- Create: `web/components/experiment-provenance.tsx`
- Modify: `web/components/operations-shell.tsx`
- Modify: `web/app/globals.css`
- Create: `web/lib/simulation-store.test.ts`
- Create: `web/components/experiment-workspace.test.tsx`
- Create: `web/components/experiment-comparison.test.tsx`

**Interfaces:**
- Consumes: Task 12 REST/WebSocket representations.
- Produces: `SimulationClient`, `SimulationStore`, and a read-only `ExperimentWorkspace` reachable from the current operations shell.

- [ ] **Step 1: Write failing store and component tests**

```typescript
it("ignores an older run sequence", () => {
  const store = new SimulationStore();
  store.apply(runUpdate(8, "running"));
  store.apply(runUpdate(7, "paused"));
  expect(store.getSnapshot().run?.sequence).toBe(8);
});

it("shows provenance and no mutating controls", () => {
  render(<ExperimentWorkspace client={fakeClient(completedRun)} mapFactory={fakeMapFactory} />);
  expect(screen.getByText("Azure Functions")).toBeInTheDocument();
  expect(screen.getByText("modelled origin assignment")).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: /start|pause|cancel/i })).not.toBeInTheDocument();
});
```

Add comparison tests that require all component metrics beside the combined score and level-of-detail tests that aggregate `eu-medium-v1` nodes while zoomed out.

- [ ] **Step 2: Run frontend tests and verify missing modules**

Run: `npm --prefix web test -- simulation-store.test.ts experiment-workspace.test.tsx experiment-comparison.test.tsx`

Expected: tests fail because the simulation client/components do not exist.

- [ ] **Step 3: Implement typed store, reconnecting client, and workspace**

Parse unknown API input with explicit type guards before store mutation. Fetch the initial snapshot, connect the run-specific WebSocket, discard stale sequences, reconnect with capped exponential backoff, and keep completed snapshots usable offline. Render tier-specific nodes, directed links, active paths/transfers, queues, utilization, failures, clock/configuration, latency by tier, and provenance. At low zoom, group nodes by tier and geographic grid; expand individual sites only past the configured zoom threshold.

```typescript
export type SimulationStoreSnapshot = {
  runs: readonly RunListItem[];
  run: RunSnapshot | null;
  streamConnected: boolean;
  lastHeartbeatAt: string | null;
};
```

- [ ] **Step 4: Run frontend tests, type-check, and lint**

Run: `npm --prefix web test && npm --prefix web run typecheck && npm --prefix web run lint`

Expected: all commands pass.

- [ ] **Step 5: Commit**

```bash
git add web/lib web/components web/app/globals.css
git commit -m "feat: add experiment control room"
```

### Task 14: Pipeline Integration, Performance Bounds, and Acceptance Documentation

**Files:**
- Create: `tests/integration/test_simulation_pipeline.py`
- Create: `tests/load/test_simulator_profiles.py`
- Create: `tests/e2e/experiment-workspace.spec.ts`
- Modify: `tests/integration/compose.dependencies.yaml`
- Modify: `Makefile`
- Modify: `README.md`
- Create: `docs/experiments/trace-preparation.md`
- Create: `docs/experiments/acceptance-run.md`
- Create: `docs/adr/0004-deterministic-simulation-boundary.md`

**Interfaces:**
- Consumes: every production interface from Tasks 1–13.
- Produces: verified trace-to-engine-to-Redpanda-to-Redis-to-API-to-browser coverage and operator instructions for the real one-hour acceptance inputs.

- [ ] **Step 1: Write the failing full-pipeline and profile tests**

```python
async def test_fixture_trace_reaches_experiment_api(redpanda, redis, api_client) -> None:
    result = await run_fixture_scenario("azure_functions", "balanced")
    response = await eventually_get(api_client, f"/api/v1/experiments/{result.run_id}")
    assert response.json()["status"] == "completed"
    assert response.json()["summary"]["component_metrics"]["completion_rate"] == 1.0


@pytest.mark.parametrize("profile,max_seconds", [("eu-small-v1", 10), ("eu-medium-v1", 60)])
def test_profile_finishes_within_bound(profile: str, max_seconds: int) -> None:
    elapsed = benchmark_fixture(profile, workloads=10_000, seed=9)
    assert elapsed < max_seconds
```

The browser test selects a completed run, inspects a far-edge node, confirms a directed path and failure indicator, opens provenance, and compares four policies without exposing start/pause/cancel controls.

- [ ] **Step 2: Run the new tests and establish their initial failures**

Run: `uv run --group dev pytest tests/integration/test_simulation_pipeline.py tests/load/test_simulator_profiles.py -q && npm --prefix web run test:e2e -- experiment-workspace.spec.ts`

Expected: integration fixtures and browser route wiring fail until completed in this task.

- [ ] **Step 3: Complete orchestration and operational documentation**

Add simulator dependencies to the integration compose file, wire health checks and topic creation, add `make simulation-smoke`, and document exact preparation and acceptance commands. The trace guide names official source versions, required columns, checksum acquisition, partitions, one-hour window selection, scale factors, provenance meanings, and the rule that raw datasets remain outside git. The acceptance guide records how to provide the two prepared artifact directories and one recorded mobility file, run eight scenarios, verify bundle checksums, and read `comparison.md` without treating simulated capacity/cost/energy as provider facts.

- [ ] **Step 4: Run the complete verification suite**

Run: `make test && make lint && make smoke && make simulation-smoke`

Expected: Python unit/contract/load/integration tests, frontend unit/E2E tests, lint, type checks, compose pipeline, and both topology performance bounds pass.

- [ ] **Step 5: Inspect reproducibility outputs**

Run: `uv run aether-sim compare --config configs/experiments/eu-small-acceptance-v1.yaml --azure-artifact tests/fixtures/prepared/azure --borg-artifact tests/fixtures/prepared/borg --output /tmp/aether-acceptance-a && uv run aether-sim compare --config configs/experiments/eu-small-acceptance-v1.yaml --azure-artifact tests/fixtures/prepared/azure --borg-artifact tests/fixtures/prepared/borg --output /tmp/aether-acceptance-b && diff -ru /tmp/aether-acceptance-a /tmp/aether-acceptance-b`

Expected: eight completed runs per bundle and no `diff` output.

- [ ] **Step 6: Commit**

```bash
git add tests Makefile README.md docs/experiments docs/adr/0004-deterministic-simulation-boundary.md
git commit -m "test: verify simulation platform end to end"
```

## Completion Gate

Before calling the increment complete:

- [ ] Prepare the real one-hour Azure Functions artifact from the documented official source and record its source checksum outside git.
- [ ] Prepare the real one-hour Google Borg artifact from the documented official source and record its source checksum outside git.
- [ ] Record or select one bounded OpenSky mobility timeline used unchanged for all eight runs.
- [ ] Execute `aether-sim compare` with the approved topology, capacity profile, failure schedule, seed, scale factors, time acceleration, and objective weights.
- [ ] Confirm all eight runs are complete, immutable, and checksum-valid.
- [ ] Confirm `comparison.md` includes every component metric and the combined score with exact weights.
- [ ] Confirm the live API and browser can read the completed comparison while no mutating endpoint or UI control exists.
- [ ] Run `git status --short` and confirm only intentionally untracked external artifact directories remain.
