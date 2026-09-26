from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from aether_api.simulation_repository import SimulationRepository

router = APIRouter()


def get_simulation_repository() -> SimulationRepository:
    msg = "simulation repository dependency is not configured"
    raise RuntimeError(msg)


@router.get("/api/v1/experiments")
async def experiments_list(
    repository: SimulationRepository = Depends(get_simulation_repository),  # noqa: B008
) -> dict[str, object]:
    try:
        runs = await repository.list_experiments()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="simulation projection unavailable") from exc
    return {"runs": runs}


@router.get("/api/v1/experiments/{run_id}")
async def experiment_snapshot(
    run_id: str,
    repository: SimulationRepository = Depends(get_simulation_repository),  # noqa: B008
) -> dict[str, object]:
    try:
        snapshot = await repository.get_experiment(run_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="simulation projection unavailable") from exc
    if snapshot is None:
        raise HTTPException(status_code=404, detail="experiment not found")
    return snapshot


@router.get("/api/v1/experiments/{run_id}/results")
async def experiment_results(
    run_id: str,
    repository: SimulationRepository = Depends(get_simulation_repository),  # noqa: B008
) -> dict[str, object]:
    try:
        result = await repository.get_results(run_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="simulation projection unavailable") from exc
    if result is None:
        raise HTTPException(status_code=404, detail="experiment not found")
    return result
