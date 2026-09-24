from __future__ import annotations

from prometheus_client import Counter, Histogram

ingestor_polls_total = Counter(
    "aether_ingestor_polls_total",
    "Count of ingestion poll cycles.",
)

ingestor_poll_duration_seconds = Histogram(
    "aether_ingestor_poll_duration_seconds",
    "Duration of ingestion poll cycles.",
)

ingestor_rows_rejected_total = Counter(
    "aether_ingestor_rows_rejected_total",
    "Rows rejected while normalizing OpenSky responses.",
)

ingestor_events_published_total = Counter(
    "aether_ingestor_events_published_total",
    "Events successfully published to Redpanda.",
)
