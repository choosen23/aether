from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

snapshot_duration_seconds = Histogram(
    "aether_api_snapshot_duration_seconds",
    "Duration of snapshot requests.",
)

snapshot_aircraft_count = Histogram(
    "aether_api_snapshot_aircraft_count",
    "Aircraft count returned by snapshot requests.",
)

mobility_snapshot_duration_seconds = Histogram(
    "aether_api_mobility_snapshot_duration_seconds",
    "Duration of mobility snapshot requests.",
)

mobility_cell_count = Histogram(
    "aether_api_mobility_snapshot_cell_count",
    "Cell count returned by mobility snapshot requests.",
)

mobility_connected_clients = Counter(
    "aether_api_mobility_connected_clients_total",
    "Count of mobility websocket client subscribe events.",
)

mobility_dropped_clients = Counter(
    "aether_api_mobility_dropped_clients_total",
    "Count of mobility websocket client evictions.",
)

redis_errors_total = Counter(
    "aether_api_redis_errors_total",
    "Count of Redis errors observed by API endpoints.",
)


def render_metrics() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
