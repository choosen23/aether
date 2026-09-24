#!/usr/bin/env sh
set -eu

rpk --brokers "${REDPANDA_BROKERS}" topic create "${REDPANDA_AIRCRAFT_TOPIC}" \
  --partitions 3 \
  --replicas 1 \
  --topic-config retention.ms=86400000 \
  || rpk --brokers "${REDPANDA_BROKERS}" topic describe "${REDPANDA_AIRCRAFT_TOPIC}" >/dev/null

rpk --brokers "${REDPANDA_BROKERS}" topic create "${REDPANDA_MOBILITY_DENSITY_TOPIC}" \
  --partitions 3 \
  --replicas 1 \
  --topic-config retention.ms=86400000 \
  || rpk --brokers "${REDPANDA_BROKERS}" topic describe "${REDPANDA_MOBILITY_DENSITY_TOPIC}" >/dev/null

rpk --brokers "${REDPANDA_BROKERS}" topic create "${REDPANDA_DEMAND_REGION_TOPIC}" \
  --partitions 3 \
  --replicas 1 \
  --topic-config retention.ms=86400000 \
  || rpk --brokers "${REDPANDA_BROKERS}" topic describe "${REDPANDA_DEMAND_REGION_TOPIC}" >/dev/null
