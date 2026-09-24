from aether_ingestor.config import IngestorSettings


def test_europe_bbox_is_configurable_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("EUROPE_MIN_LAT", "35.5")
    monkeypatch.setenv("EUROPE_MIN_LON", "-12.5")
    monkeypatch.setenv("EUROPE_MAX_LAT", "71.5")
    monkeypatch.setenv("EUROPE_MAX_LON", "31.5")

    settings = IngestorSettings.from_env()

    assert settings.europe_bbox.model_dump() == {
        "min_lat": 35.5,
        "min_lon": -12.5,
        "max_lat": 71.5,
        "max_lon": 31.5,
    }


def test_anonymous_polling_defaults_to_sixty_seconds() -> None:
    assert IngestorSettings().poll_interval_seconds == 60
    assert IngestorSettings(poll_interval_seconds=10).poll_interval_seconds == 10
