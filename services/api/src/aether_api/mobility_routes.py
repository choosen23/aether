from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException

from aether_api.metrics import (
    mobility_cell_count,
    mobility_snapshot_duration_seconds,
    redis_errors_total,
)
from aether_api.mobility_repository import MobilityRepository

router = APIRouter()


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def get_mobility_repository() -> MobilityRepository:
    msg = "mobility repository dependency is not configured"
    raise RuntimeError(msg)


@router.get("/api/v1/mobility/cells")
async def mobility_snapshot(
    repository: MobilityRepository = Depends(get_mobility_repository),  # noqa: B008
) -> dict[str, object]:
    start = datetime.now(tz=UTC)
    try:
        snapshot = await repository.snapshot(utc_now())
    except Exception as exc:
        redis_errors_total.inc()
        raise HTTPException(status_code=503, detail="mobility snapshot unavailable") from exc

    elapsed = max(0.0, (datetime.now(tz=UTC) - start).total_seconds())
    mobility_snapshot_duration_seconds.observe(elapsed)
    mobility_cell_count.observe(len(snapshot.cells))
    return snapshot.model_dump(mode="json")
