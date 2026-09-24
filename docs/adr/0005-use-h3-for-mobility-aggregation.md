# ADR 0005: Use H3 for mobility aggregation

## Status

Accepted

## Context

Phase 3 introduces regional density and modelled demand derived from live
aircraft positions. We need a stable global cell system for sparse aggregation,
incremental updates, and browser polygon rendering.

## Decision

Use H3 cells at resolution 4 as the canonical regional key (`cell_id`) for
`mobility.density.v1` and `demand.region.v1`.

## Consequences

- Pros:
  - Stable cell identity for deterministic keys and joins.
  - Good global tiling for Europe without custom latitude/longitude bin logic.
  - Native conversion to polygon boundaries for MapLibre rendering.
  - Sparse change publication works naturally with keyed cell updates/removals.
- Trade-offs:
  - Adds `h3` (backend) and `h3-js` (frontend) dependencies.
  - Requires defensive handling of invalid cells and antimeridian edge cases.

## Alternatives considered

1. Fixed latitude/longitude squares
   - Rejected: distortion and custom edge handling complexity.
2. Client-only aggregation from aircraft points
   - Rejected: duplicates compute per client, no server-side retained mobility
     state, and inconsistent provenance for demand modelling.
