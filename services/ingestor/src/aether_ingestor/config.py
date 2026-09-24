from __future__ import annotations

from pydantic import BaseModel, Field, ValidationError, ValidationInfo, field_validator


class BoundingBox(BaseModel):
    min_lat: float = Field(ge=-90, le=90)
    min_lon: float = Field(ge=-180, le=180)
    max_lat: float = Field(ge=-90, le=90)
    max_lon: float = Field(ge=-180, le=180)

    @field_validator("max_lat")
    @classmethod
    def validate_max_lat(cls, value: float, info: ValidationInfo) -> float:
        minimum = info.data.get("min_lat")
        if minimum is not None and value <= minimum:
            msg = "max_lat must be greater than min_lat"
            raise ValueError(msg)
        return value

    @field_validator("max_lon")
    @classmethod
    def validate_max_lon(cls, value: float, info: ValidationInfo) -> float:
        minimum = info.data.get("min_lon")
        if minimum is not None and value <= minimum:
            msg = "max_lon must be greater than min_lon"
            raise ValueError(msg)
        return value


class IngestorSettings(BaseModel):
    opensky_base_url: str = "https://opensky-network.org/api"
    poll_interval_seconds: int = 60
    request_timeout_seconds: float = 10.0
    opensky_client_id: str | None = None
    opensky_client_secret: str | None = None
    redpanda_topic: str = "aircraft.position.v1"
    redpanda_brokers: str = "localhost:19092"
    publish_delivery_timeout_seconds: float = 5.0
    backoff_initial_seconds: float = 1.0
    backoff_max_seconds: float = 30.0
    source_mode: str = "opensky"
    environment: str = "development"
    metrics_port: int = 9101

    europe_bbox: BoundingBox = BoundingBox(min_lat=34.0, min_lon=-11.0, max_lat=72.0, max_lon=32.0)

    @field_validator("poll_interval_seconds")
    @classmethod
    def validate_poll_interval(cls, value: int) -> int:
        if value < 10:
            msg = "poll_interval_seconds must be at least 10"
            raise ValueError(msg)
        return value

    @classmethod
    def from_env(cls) -> IngestorSettings:
        import os

        default_base_url = cls.model_fields["opensky_base_url"].default
        default_poll_interval = cls.model_fields["poll_interval_seconds"].default
        default_timeout = cls.model_fields["request_timeout_seconds"].default

        data: dict[str, object] = {
            "opensky_base_url": os.getenv("OPEN_SKY_BASE_URL", default_base_url),
            "poll_interval_seconds": os.getenv(
                "OPEN_SKY_POLL_INTERVAL_SECONDS", default_poll_interval
            ),
            "request_timeout_seconds": os.getenv(
                "OPEN_SKY_REQUEST_TIMEOUT_SECONDS", default_timeout
            ),
            "opensky_client_id": os.getenv("OPEN_SKY_CLIENT_ID") or None,
            "opensky_client_secret": os.getenv("OPEN_SKY_CLIENT_SECRET") or None,
            "redpanda_topic": os.getenv(
                "REDPANDA_AIRCRAFT_TOPIC", cls.model_fields["redpanda_topic"].default
            ),
            "redpanda_brokers": os.getenv(
                "REDPANDA_BROKERS", cls.model_fields["redpanda_brokers"].default
            ),
            "publish_delivery_timeout_seconds": os.getenv(
                "PUBLISH_DELIVERY_TIMEOUT_SECONDS",
                cls.model_fields["publish_delivery_timeout_seconds"].default,
            ),
            "backoff_initial_seconds": os.getenv(
                "BACKOFF_INITIAL_SECONDS", cls.model_fields["backoff_initial_seconds"].default
            ),
            "backoff_max_seconds": os.getenv(
                "BACKOFF_MAX_SECONDS", cls.model_fields["backoff_max_seconds"].default
            ),
            "source_mode": os.getenv("AETHER_SOURCE_MODE", cls.model_fields["source_mode"].default),
            "environment": os.getenv(
                "AETHER_ENVIRONMENT", cls.model_fields["environment"].default
            ),
            "metrics_port": os.getenv(
                "INGESTOR_METRICS_PORT", cls.model_fields["metrics_port"].default
            ),
            "europe_bbox": {
                "min_lat": os.getenv("EUROPE_MIN_LAT", "34.0"),
                "min_lon": os.getenv("EUROPE_MIN_LON", "-11.0"),
                "max_lat": os.getenv("EUROPE_MAX_LAT", "72.0"),
                "max_lon": os.getenv("EUROPE_MAX_LON", "32.0"),
            },
        }
        try:
            return cls.model_validate(data)
        except ValidationError as exc:
            msg = "invalid ingestor settings"
            raise ValueError(msg) from exc
