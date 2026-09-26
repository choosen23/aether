from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, WebSocket
from starlette.websockets import WebSocketDisconnect

from aether_api.config import ApiSettings
from aether_api.simulation_broadcaster import SimulationBroadcaster
from aether_api.simulation_repository import SimulationRepository
from aether_api.websocket import validate_origin

router = APIRouter()


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        msg = "boolean is not a valid sequence value"
        raise TypeError(msg)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, bytes):
        return int(value.decode("utf-8"))
    if isinstance(value, str):
        return int(value)
    return 0


def get_settings() -> ApiSettings:
    msg = "api settings dependency is not configured"
    raise RuntimeError(msg)


def get_simulation_broadcaster() -> SimulationBroadcaster:
    msg = "simulation broadcaster dependency is not configured"
    raise RuntimeError(msg)


def get_simulation_repository() -> SimulationRepository:
    msg = "simulation repository dependency is not configured"
    raise RuntimeError(msg)


@router.websocket("/ws/v1/experiments/{run_id}")
async def experiment_stream(
    run_id: str,
    socket: WebSocket,
    settings: ApiSettings = Depends(get_settings),  # noqa: B008
    broadcaster: SimulationBroadcaster = Depends(get_simulation_broadcaster),  # noqa: B008
    repository: SimulationRepository = Depends(get_simulation_repository),  # noqa: B008
) -> None:
    if not validate_origin(socket.headers.get("origin"), settings.allowed_origins):
        await socket.close(code=1008)
        return

    initial = await repository.get_experiment(run_id)
    if initial is None:
        await socket.close(code=4404)
        return

    await socket.accept()
    await socket.send_json(
        {
            "message_type": "simulation.stream.ready",
            "sent_at": _utc_now_iso(),
            "run_id": run_id,
        }
    )
    await socket.send_json(
        {
            "message_type": "simulation.run.updated",
            "sent_at": _utc_now_iso(),
            "run": initial,
        }
    )

    client_id, queue = broadcaster.subscribe(run_id)
    last_sequence = _coerce_int(initial.get("sequence", 0))
    try:
        while True:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=settings.heartbeat_seconds)
            except TimeoutError:
                await socket.send_json(
                    {
                        "message_type": "simulation.heartbeat",
                        "sent_at": _utc_now_iso(),
                        "run_id": run_id,
                    }
                )
                continue

            if message.get("message_type") == "simulation.run.updated":
                run = message.get("run")
                if isinstance(run, dict):
                    sequence = _coerce_int(run.get("sequence", 0))
                    if sequence <= last_sequence:
                        continue
                    last_sequence = sequence
            await socket.send_json(message)
    except WebSocketDisconnect:
        return
    finally:
        broadcaster.unsubscribe(run_id, client_id)
