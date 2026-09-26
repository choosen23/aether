from __future__ import annotations

import json
from typing import Protocol, cast


class RedisSimulationReadProtocol(Protocol):
    async def zrange(self, key: str, start: int, stop: int) -> list[object]: ...

    async def hgetall(self, key: str) -> dict[object, object]: ...


def _decode(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


class SimulationRepository:
    def __init__(self, redis: RedisSimulationReadProtocol, namespace: str = "aether") -> None:
        self._redis = redis
        self._namespace = namespace

    @property
    def _runs_key(self) -> str:
        return f"{self._namespace}:simulation:runs"

    def _run_key(self, run_id: str) -> str:
        return f"{self._namespace}:simulation:run:{run_id}"

    async def list_experiments(self) -> list[dict[str, object]]:
        run_ids = [_decode(item) for item in await self._redis.zrange(self._runs_key, 0, -1)]
        run_ids.reverse()
        items: list[dict[str, object]] = []
        for run_id in run_ids:
            snapshot = await self.get_experiment(run_id)
            if snapshot is None:
                continue
            configuration = snapshot.get("configuration")
            scheduler_policy = "balanced"
            if isinstance(configuration, dict):
                value = configuration.get("scheduler_policy")
                if value is not None:
                    scheduler_policy = str(value)
            items.append(
                {
                    "run_id": run_id,
                    "status": snapshot.get("status", "running"),
                    "sequence": snapshot.get("sequence", 0),
                    "scheduler_policy": scheduler_policy,
                }
            )
        return items

    async def get_experiment(self, run_id: str) -> dict[str, object] | None:
        raw = await self._redis.hgetall(self._run_key(run_id))
        payload = {str(_decode(k)): _decode(v) for k, v in raw.items()}
        if not payload:
            return None

        sequence = int(payload.get("last_sequence", "0"))
        simulated_time_ms = int(payload.get("simulated_time_ms", "0"))
        status = payload.get("status") or _status_from_event(payload.get("last_event_type", ""))

        configuration = _maybe_json(payload.get("configuration"))
        if not isinstance(configuration, dict):
            configuration = {"scheduler_policy": payload.get("scheduler_policy", "balanced")}

        summary = _maybe_json(payload.get("summary"))
        if not isinstance(summary, dict):
            summary = {
                "component_metrics": {},
                "combined_score": None,
            }

        return {
            "run_id": run_id,
            "sequence": sequence,
            "status": status,
            "simulated_time_ms": simulated_time_ms,
            "configuration": configuration,
            "summary": summary,
        }

    async def get_results(self, run_id: str) -> dict[str, object] | None:
        experiment = await self.get_experiment(run_id)
        if experiment is None:
            return None
        return {
            "run_id": experiment["run_id"],
            "status": experiment["status"],
            "summary": experiment["summary"],
        }


def _maybe_json(value: str | None) -> object | None:
    if value is None or value == "":
        return None
    try:
        return cast(object, json.loads(value))
    except json.JSONDecodeError:
        return None


def _status_from_event(last_event_type: str) -> str:
    if last_event_type in {"execution_completed", "run_completed"}:
        return "completed"
    if last_event_type == "run_paused":
        return "paused"
    if last_event_type == "run_cancelled":
        return "cancelled"
    if last_event_type == "run_failed":
        return "failed"
    return "running"
