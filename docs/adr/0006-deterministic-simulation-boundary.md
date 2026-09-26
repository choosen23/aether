# ADR 0006: Deterministic Simulation Boundary

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

Aether now includes deterministic experiment simulation alongside live mobility
visualization. We need a strict boundary so runtime APIs and UI can observe
results without mutating simulation state.

## Decision

1. Simulation run control remains CLI-driven (`prepare`, `run`, `pause`,
   `resume`, `cancel`, `compare`).
2. API experiment routes are read-only.
3. Web experiment workspace is read-only and exposes no mutating controls.
4. Redis projections are rebuildable from retained simulation events.

## Consequences

- Determinism is preserved by avoiding UI/API write paths.
- Operational complexity is reduced for public deployment.
- Future remote orchestration requires a separate reviewed interface.
