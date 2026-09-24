from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from aether_contracts import MobilityHeartbeat, MobilityStreamReady
from fastapi import APIRouter, Depends, WebSocket
from starlette.websockets import WebSocketDisconnect

from aether_api.config import ApiSettings
from aether_api.metrics import mobility_connected_clients
from aether_api.mobility_broadcaster import MobilityBroadcaster
from aether_api.mobility_repository import MobilityRepository
from aether_api.websocket import validate_origin

router = APIRouter()


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def get_settings() -> ApiSettings:
    msg = "api settings dependency is not configured"
    raise RuntimeError(msg)


def get_mobility_broadcaster() -> MobilityBroadcaster:
    msg = "mobility broadcaster dependency is not configured"
    raise RuntimeError(msg)


def get_mobility_repository() -> MobilityRepository:
    msg = "mobility repository dependency is not configured"
    raise RuntimeError(msg)


@router.websocket("/api/v1/stream/mobility")
async def mobility_stream(
    socket: WebSocket,
    settings: ApiSettings = Depends(get_settings),  # noqa: B008
    broadcaster: MobilityBroadcaster = Depends(get_mobility_broadcaster),  # noqa: B008
    repository: MobilityRepository = Depends(get_mobility_repository),  # noqa: B008
) -> None:
    if not validate_origin(socket.headers.get("origin"), settings.allowed_origins):
        await socket.close(code=1008)
        return

    await socket.accept()
    await socket.send_json(MobilityStreamReady(sent_at=utc_now()).model_dump(mode="json"))

    client_id, queue = broadcaster.subscribe()
    mobility_connected_clients.inc()
    try:
        while True:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=settings.heartbeat_seconds)
            except TimeoutError:
                now = utc_now()
                heartbeat = MobilityHeartbeat(
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
