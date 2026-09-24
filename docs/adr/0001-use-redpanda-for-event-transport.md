# ADR 0001: Use Redpanda for event transport

## Context
Need Kafka-compatible retained event transport for current and future consumers.

## Decision
Use Redpanda and topic `aircraft.position.v1`.

## Consequences
Durable replay and independent consumers are available.

## Alternatives
Redis Streams as primary transport.
