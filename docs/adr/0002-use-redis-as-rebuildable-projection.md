# ADR 0002: Redis as rebuildable projection

## Context
Need low-latency latest-state query model for API and streaming.

## Decision
Use Redis as disposable latest-state projection, rebuilt from stream.

## Consequences
Simple reads and pub/sub fan-out; no historical authority in Redis.

## Alternatives
Store full history in Redis.
