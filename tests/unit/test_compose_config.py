from __future__ import annotations

import re
from pathlib import Path

import yaml


def compose_config() -> dict[str, object]:
    return yaml.safe_load(Path("compose.yaml").read_text())


def env_example() -> str:
    return Path(".env.example").read_text()


def test_compose_has_health_checked_dependencies() -> None:
    services = compose_config()["services"]
    assert {"redpanda", "redis", "ingestor", "state-builder", "api", "web"} <= services.keys()
    assert services["state-builder"]["depends_on"]["redpanda"]["condition"] == "service_healthy"
    assert "healthcheck" in services["api"]


def test_redpanda_has_distinct_internal_listener_and_topic_initializer() -> None:
    services = compose_config()["services"]
    command = " ".join(services["redpanda"]["command"])
    assert "internal://0.0.0.0:9092" in command
    assert "external://0.0.0.0:19092" in command
    assert "internal://redpanda:9092" in command
    assert "redpanda-init" in services
    assert services["ingestor"]["depends_on"]["redpanda-init"]["condition"] == (
        "service_completed_successfully"
    )


def test_env_example_contains_no_secret_values() -> None:
    text = env_example()
    assert "OPEN_SKY_CLIENT_SECRET=" in text
    assert not re.search(r"OPEN_SKY_CLIENT_SECRET=.+", text)


def test_python_image_installs_all_workspace_packages_into_runtime_path() -> None:
    dockerfile = Path("docker/python.Dockerfile").read_text()
    assert "uv sync --frozen --no-dev --all-packages" in dockerfile
    assert 'ENV PATH="/app/.venv/bin:$PATH"' in dockerfile
