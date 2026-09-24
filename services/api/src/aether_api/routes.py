from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse, Response

from aether_api.metrics import (
    redis_errors_total,
    render_metrics,
    snapshot_aircraft_count,
    snapshot_duration_seconds,
)
from aether_api.repository import AircraftRepository

router = APIRouter()


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def get_repository() -> AircraftRepository:
    msg = "repository dependency is not configured"
    raise RuntimeError(msg)


@router.get("/api/v1/aircraft")
async def aircraft_snapshot(
    repository: AircraftRepository = Depends(get_repository),  # noqa: B008
) -> dict[str, object]:
    start = datetime.now(tz=UTC)
    try:
        snapshot = await repository.snapshot(utc_now())
    except Exception as exc:
        redis_errors_total.inc()
        raise HTTPException(status_code=503, detail="snapshot unavailable") from exc

    elapsed = max(0.0, (datetime.now(tz=UTC) - start).total_seconds())
    snapshot_duration_seconds.observe(elapsed)
    snapshot_aircraft_count.observe(len(snapshot.aircraft))
    return snapshot.model_dump(mode="json")


@router.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/health/ready")
async def ready(
    repository: AircraftRepository = Depends(get_repository),  # noqa: B008
) -> JSONResponse:
    try:
        is_ready = await repository.ready()
    except Exception:
        redis_errors_total.inc()
        return JSONResponse(status_code=503, content={"status": "not_ready"})
    if not is_ready:
        return JSONResponse(status_code=503, content={"status": "not_ready"})
    return JSONResponse(status_code=200, content={"status": "ready"})


@router.get("/metrics")
async def metrics() -> Response:
    body, content_type = render_metrics()
    return Response(content=body, media_type=content_type)
