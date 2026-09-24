from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis import asyncio as redis_async

from aether_api.broadcaster import AircraftBroadcaster
from aether_api.config import ApiSettings
from aether_api.mobility_broadcaster import MobilityBroadcaster
from aether_api.mobility_feed import MobilityFeed
from aether_api.mobility_repository import MobilityRepository
from aether_api.mobility_routes import (
    get_mobility_repository,
)
from aether_api.mobility_routes import (
    router as mobility_router,
)
from aether_api.mobility_websocket import (
    get_mobility_broadcaster,
)
from aether_api.mobility_websocket import (
    get_mobility_repository as get_mobility_ws_repository,
)
from aether_api.mobility_websocket import (
    get_settings as get_mobility_settings,
)
from aether_api.mobility_websocket import (
    router as mobility_websocket_router,
)
from aether_api.projection_feed import ProjectionFeed
from aether_api.repository import AircraftRepository
from aether_api.routes import get_repository, router
from aether_api.websocket import (
    get_broadcaster,
    get_settings,
)
from aether_api.websocket import (
    router as websocket_router,
)


class _NoopMobilityRedis:
    async def zrangebyscore(
        self,
        _key: str,
        _min_score: int,
        _max_score: str,
    ) -> list[object]:
        return []

    async def hget(self, _key: str, _field: str) -> object | None:
        return None

    async def eval(self, _script: str, _numkeys: int, *keys_and_args: object) -> object:
        return []


def _build_app(
    settings: ApiSettings,
    repository: AircraftRepository,
    mobility_repository: MobilityRepository,
    redis: object | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        if redis is None:
            yield
            return
        feed = ProjectionFeed(
            redis=redis,
            broadcaster=_app.state.broadcaster,
            repository=repository,
            namespace=settings.redis_namespace,
            removal_interval_seconds=settings.removal_interval_seconds,
        )
        mobility_feed = MobilityFeed(
            redis=redis,
            broadcaster=_app.state.mobility_broadcaster,
            repository=mobility_repository,
            namespace=settings.redis_namespace,
            removal_interval_seconds=settings.removal_interval_seconds,
        )
        task = asyncio.create_task(feed.run())
        mobility_task = asyncio.create_task(mobility_feed.run())
        try:
            yield
        finally:
            task.cancel()
            mobility_task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            with suppress(asyncio.CancelledError):
                await mobility_task

    app = FastAPI(title="Aether API", version="0.1.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_methods=["GET"],
        allow_headers=["Content-Type"],
    )
    broadcaster = AircraftBroadcaster(queue_size=settings.websocket_queue_size)
    mobility_broadcaster = MobilityBroadcaster(queue_size=settings.websocket_queue_size)

    app.state.settings = settings
    app.state.repository = repository
    app.state.broadcaster = broadcaster
    app.state.mobility_repository = mobility_repository
    app.state.mobility_broadcaster = mobility_broadcaster

    app.dependency_overrides[get_repository] = lambda: repository
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_broadcaster] = lambda: broadcaster
    app.dependency_overrides[get_mobility_repository] = lambda: mobility_repository
    app.dependency_overrides[get_mobility_settings] = lambda: settings
    app.dependency_overrides[get_mobility_broadcaster] = lambda: mobility_broadcaster
    app.dependency_overrides[get_mobility_ws_repository] = lambda: mobility_repository

    app.include_router(router)
    app.include_router(websocket_router)
    app.include_router(mobility_router)
    app.include_router(mobility_websocket_router)
    return app


def create_app(
    settings: ApiSettings | None = None,
    repository: AircraftRepository | None = None,
) -> FastAPI:
    resolved_settings = settings or ApiSettings.from_env()
    if repository is not None:
        if isinstance(repository, AircraftRepository):
            mobility_repository = MobilityRepository(
                repository._redis,
                repository,
                resolved_settings,
            )
            return _build_app(resolved_settings, repository, mobility_repository)
        mobility_repository = MobilityRepository(
            _NoopMobilityRedis(),
            repository,
            resolved_settings,
        )
        return _build_app(resolved_settings, repository, mobility_repository)

    redis = redis_async.from_url(resolved_settings.redis_url)  # type: ignore[no-untyped-call]
    resolved_repository = AircraftRepository(redis, resolved_settings)
    resolved_mobility_repository = MobilityRepository(redis, resolved_repository, resolved_settings)
    app = _build_app(resolved_settings, resolved_repository, resolved_mobility_repository, redis)

    @app.on_event("shutdown")
    async def close_redis() -> None:
        await redis.aclose()

    return app


def create_app_from_env() -> FastAPI:
    return create_app()
