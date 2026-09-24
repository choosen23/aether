from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from aether_contracts import Heartbeat, StreamReady
from fastapi import APIRouter, Depends, WebSocket
from starlette.websockets import WebSocketDisconnect

from aether_api.broadcaster import AircraftBroadcaster
from aether_api.config import ApiSettings
from aether_api.repository import AircraftRepository
from aether_api.routes import get_repository

router = APIRouter()


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def get_settings() -> ApiSettings:
    msg = "api settings dependency is not configured"
    raise RuntimeError(msg)


def get_broadcaster() -> AircraftBroadcaster:
    msg = "broadcaster dependency is not configured"
    raise RuntimeError(msg)


def validate_origin(origin: str | None, allowed_origins: list[str]) -> bool:
    if "*" in allowed_origins:
        return True
    if origin is None:
        return False
    return origin in allowed_origins


@router.websocket("/api/v1/stream/aircraft")
async def aircraft_stream(
    socket: WebSocket,
    settings: ApiSettings = Depends(get_settings),  # noqa: B008
    broadcaster: AircraftBroadcaster = Depends(get_broadcaster),  # noqa: B008
    repository: AircraftRepository = Depends(get_repository),  # noqa: B008
) -> None:
    if not validate_origin(socket.headers.get("origin"), settings.allowed_origins):
        await socket.close(code=1008)
        return

    await socket.accept()
    await socket.send_json(StreamReady(sent_at=utc_now()).model_dump(mode="json"))

    client_id, queue = broadcaster.subscribe()
    try:
        while True:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=settings.heartbeat_seconds)
            except TimeoutError:
                now = utc_now()
                heartbeat = Heartbeat(
                    sent_at=now,
                    source_status=await repository.source_status(now),
                )
                await socket.send_json(heartbeat.model_dump(mode="json"))
                continue

            await socket.send_json(message.model_dump(mode="json"))
    except WebSocketDisconnect:
        return
    finally:
        broadcaster.unsubscribe(client_id)
