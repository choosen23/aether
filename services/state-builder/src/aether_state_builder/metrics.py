from prometheus_client import Counter

projection_events_total = Counter(
    "aether_state_builder_projection_events_total",
    "Aircraft events handled by projection result.",
    ["result"],
)

consumer_records_total = Counter(
    "aether_state_builder_consumer_records_total",
    "Kafka records handled by consumer outcome.",
    ["result"],
)

mobility_projection_events_total = Counter(
    "aether_state_builder_mobility_projection_events_total",
    "Mobility events handled by projection result.",
    ["result"],
)

mobility_consumer_records_total = Counter(
    "aether_state_builder_mobility_consumer_records_total",
    "Kafka mobility records handled by consumer outcome.",
    ["result"],
)
