from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aether_contracts import AircraftPositionEvent
from aether_state_builder.projection import AircraftProjection, ApplyResult


def make_event(*, observed_at: datetime, event_id: str) -> AircraftPositionEvent:
    return AircraftPositionEvent.model_validate(
        {
            "event_id": event_id,
            "event_type": "aircraft.position",
            "schema_version": 1,
            "observed_at": observed_at,
            "ingested_at": observed_at,
            "source": "opensky",
            "aircraft": {
                "icao24": "3c6444",
                "callsign": "DLH4YA",
                "origin_country": "Germany",
                "position": {
                    "latitude": 50.04,
                    "longitude": 8.56,
                    "barometric_altitude_m": 10668.0,
                    "on_ground": False,
                },
                "motion": {
                    "ground_speed_mps": 232.1,
                    "track_degrees": 271.4,
                    "vertical_rate_mps": -1.2,
                },
                "last_contact_at": observed_at,
                "position_source": "ads_b",
            },
        }
    )


EVENT_100 = make_event(
    observed_at=datetime(2026, 9, 24, 9, 10, 0, tzinfo=UTC),
    event_id="event-100",
)
EVENT_110 = make_event(
    observed_at=datetime(2026, 9, 24, 9, 10, 10, tzinfo=UTC),
    event_id="event-110",
)


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}

    async def hset(self, name: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(name, {}).update(mapping)
        return 1

    async def hget(self, name: str, key: str) -> object | None:
        return self.hashes.get(name, {}).get(key)

    async def eval(self, _script: str, numkeys: int, *keys_and_args: object) -> int:
        if numkeys == 1:
            source_key = str(keys_and_args[0])
            observed_ms = int(keys_and_args[1])
            current_value = self.hashes.get(source_key, {}).get("last_observation_epoch_ms")
            if current_value is not None and int(current_value) >= observed_ms:
                return 0
            self.hashes.setdefault(source_key, {}).update(
                {
                    "last_observation_epoch_ms": observed_ms,
                    "event_id": str(keys_and_args[2]),
                }
            )
            return 1
        assert numkeys == 3
        aircraft_key = str(keys_and_args[0])
        observed_ms = int(keys_and_args[3])
        event_id = str(keys_and_args[4])
        payload = str(keys_and_args[5])

        current_value = self.hashes.get(aircraft_key, {}).get("observed_at_epoch_ms")
        current = int(current_value) if current_value is not None else None
        if current is not None and current > observed_ms:
            return 2
        if current is not None and current == observed_ms:
            return 1

        self.hashes.setdefault(aircraft_key, {}).update(
            {
                "observed_at_epoch_ms": observed_ms,
                "event_id": event_id,
                "payload": payload,
            }
        )
        return 0


@pytest.mark.asyncio
async def test_newer_event_replaces_projection() -> None:
    projection = AircraftProjection(FakeRedis())
    assert await projection.apply(EVENT_100) is ApplyResult.ACCEPTED
    assert await projection.apply(EVENT_110) is ApplyResult.ACCEPTED
    assert (await projection.get("3c6444")).observed_at == EVENT_110.observed_at


@pytest.mark.asyncio
async def test_equal_and_older_events_do_not_regress_state() -> None:
    projection = AircraftProjection(FakeRedis())
    assert await projection.apply(EVENT_110) is ApplyResult.ACCEPTED
    assert await projection.apply(EVENT_110) is ApplyResult.DUPLICATE
    assert await projection.apply(EVENT_100) is ApplyResult.STALE
    assert (await projection.get("3c6444")).event_id == EVENT_110.event_id


@pytest.mark.asyncio
async def test_older_event_does_not_regress_source_freshness() -> None:
    redis = FakeRedis()
    projection = AircraftProjection(redis)

    await projection.apply(EVENT_110)
    await projection.apply(EVENT_100)

    source = redis.hashes["aether:source:opensky"]
    assert source["last_observation_epoch_ms"] == int(EVENT_110.observed_at.timestamp() * 1000)
    assert source["event_id"] == EVENT_110.event_id
