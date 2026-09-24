# ADR 0004: Render aircraft with one shared map layer

## Context
Need scalable browser rendering for many moving aircraft.

## Decision
Use one shared GeoJSON/source-layer approach rather than per-aircraft React markers.

## Consequences
Lower per-aircraft overhead and smoother updates.

## Alternatives
React marker per aircraft.
