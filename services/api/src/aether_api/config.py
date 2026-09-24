from __future__ import annotations

import os

from pydantic import BaseModel


class ApiSettings(BaseModel):
    redis_url: str = "redis://localhost:6379/0"
    redis_namespace: str = "aether"
    source_name: str = "opensky"
    source_live_max_age_seconds: float = 75.0
    source_offline_max_age_seconds: float = 180.0
    aircraft_removal_seconds: int = 180
    max_snapshot_aircraft: int = 5000
    websocket_queue_size: int = 100
    heartbeat_seconds: float = 15.0
    removal_interval_seconds: float = 5.0
    allowed_origins: list[str] = ["http://localhost:3000"]

    @classmethod
    def from_env(cls) -> ApiSettings:
        raw_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000")
        allowed_origins = [item.strip() for item in raw_origins.split(",") if item.strip()]
        return cls(
            redis_url=os.getenv("REDIS_URL", cls.model_fields["redis_url"].default),
            redis_namespace=os.getenv(
                "REDIS_NAMESPACE", cls.model_fields["redis_namespace"].default
            ),
            source_name=os.getenv("SOURCE_NAME", cls.model_fields["source_name"].default),
            source_live_max_age_seconds=float(
                os.getenv(
                    "SOURCE_LIVE_MAX_AGE_SECONDS",
                    cls.model_fields["source_live_max_age_seconds"].default,
                )
            ),
            source_offline_max_age_seconds=float(
                os.getenv(
                    "SOURCE_OFFLINE_MAX_AGE_SECONDS",
                    cls.model_fields["source_offline_max_age_seconds"].default,
                )
            ),
            aircraft_removal_seconds=int(
                os.getenv(
                    "AIRCRAFT_REMOVAL_SECONDS",
                    cls.model_fields["aircraft_removal_seconds"].default,
                )
            ),
            max_snapshot_aircraft=int(
                os.getenv(
                    "MAX_SNAPSHOT_AIRCRAFT",
                    cls.model_fields["max_snapshot_aircraft"].default,
                )
            ),
            websocket_queue_size=int(
                os.getenv(
                    "WEBSOCKET_QUEUE_SIZE", cls.model_fields["websocket_queue_size"].default
                )
            ),
            heartbeat_seconds=float(
                os.getenv("HEARTBEAT_SECONDS", cls.model_fields["heartbeat_seconds"].default)
            ),
            removal_interval_seconds=float(
                os.getenv(
                    "REMOVAL_INTERVAL_SECONDS",
                    cls.model_fields["removal_interval_seconds"].default,
                )
            ),
            allowed_origins=allowed_origins or ["http://localhost:3000"],
        )
