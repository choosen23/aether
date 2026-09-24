from __future__ import annotations

from dataclasses import dataclass

import httpx
from pydantic import BaseModel

from aether_ingestor.config import BoundingBox, IngestorSettings


class OpenSkyResponse(BaseModel):
    time: int
    states: list[list[object | None]] | None = None


@dataclass(slots=True)
class OpenSkyRateLimited(Exception):  # noqa: N818
    retry_after_seconds: int

    def __str__(self) -> str:
        return f"OpenSky rate limited; retry after {self.retry_after_seconds}s"


def parse_retry_after(headers: httpx.Headers) -> int:
    raw = headers.get("X-Rate-Limit-Retry-After-Seconds")
    if raw is None:
        return 0
    try:
        parsed = int(raw)
    except ValueError:
        return 0
    return max(parsed, 0)


class OpenSkyClient:
    def __init__(
        self,
        settings: IngestorSettings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(
            base_url=str(settings.opensky_base_url),
            timeout=settings.request_timeout_seconds,
            transport=transport,
        )

    async def fetch_states(self, bbox: BoundingBox) -> OpenSkyResponse:
        response = await self._client.get(
            "/states/all",
            params={
                "lamin": bbox.min_lat,
                "lomin": bbox.min_lon,
                "lamax": bbox.max_lat,
                "lomax": bbox.max_lon,
            },
        )
        if response.status_code == 429:
            raise OpenSkyRateLimited(parse_retry_after(response.headers))

        response.raise_for_status()
        return OpenSkyResponse.model_validate(response.json())

    async def aclose(self) -> None:
        await self._client.aclose()
