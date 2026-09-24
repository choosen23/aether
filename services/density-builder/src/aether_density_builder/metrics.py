from prometheus_client import Counter, Gauge

records_consumed_total = Counter(
    "aether_density_builder_records_consumed_total",
    "Records consumed by density-builder grouped by result.",
    ["result"],
)

records_published_total = Counter(
    "aether_density_builder_records_published_total",
    "Records published by density-builder grouped by topic.",
    ["topic"],
)

working_set_size = Gauge(
    "aether_density_builder_working_set_size",
    "Current number of latest-aircraft samples retained in memory.",
)
