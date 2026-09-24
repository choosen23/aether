# Data provenance

- **Real: OpenSky** observations and source timestamps.
- **Derived: interpolation** and freshness statuses computed from real observations.
- **Derived: mobility density** sparse H3 aggregates from aircraft positions.
- **Modelled: regional demand** deterministic linear demand units from density
  counts (`airborne*1.0 + on_ground*0.35`).
- **Simulated: none in this slice**; fixture mode is test-only.

OpenSky fields may be absent; null remains null. Client interpolation is delayed
by one observation interval, uses the two newest real points, and freezes after
its bounded horizon. Stale/offline labels are derived from source timestamps.
The test fixture is recorded provider-shaped data, never an automatic fallback.
