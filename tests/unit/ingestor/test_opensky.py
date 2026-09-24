from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from aether_ingestor.config import BoundingBox, IngestorSettings
from aether_ingestor.opensky import OpenSkyClient, OpenSkyRateLimited

EUROPE = BoundingBox(min_lat=34.0, min_lon=-11.0, max_lat=72.0, max_lon=32.0)


def settings() -> IngestorSettings:
    return IngestorSettings()


def assert_anonymous_request(request: httpx.Request) -> httpx.Response:
    assert request.method == "GET"
    assert request.url.path == "/api/states/all"

    params = parse_qs(urlparse(str(request.url)).query)
    assert params["lamin"] == [str(EUROPE.min_lat)]
    assert params["lomin"] == [str(EUROPE.min_lon)]
    assert params["lamax"] == [str(EUROPE.max_lat)]
    assert params["lomax"] == [str(EUROPE.max_lon)]
    assert "Authorization" not in request.headers

    return httpx.Response(
        200,
        json={
            "time": 1_798_000_000,
            "states": [],
        },
    )


@pytest.mark.asyncio
async def test_fetch_uses_bbox_and_no_auth_by_default() -> None:
    transport = httpx.MockTransport(assert_anonymous_request)
    client = OpenSkyClient(settings(), transport=transport)
    response = await client.fetch_states(EUROPE)
    assert response.time == 1_798_000_000
    await client.aclose()


@pytest.mark.asyncio
async def test_rate_limit_exposes_retry_after() -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            429,
            headers={"X-Rate-Limit-Retry-After-Seconds": "37"},
            request=request,
        )
    )

    client = OpenSkyClient(settings(), transport=transport)
    with pytest.raises(OpenSkyRateLimited) as error:
        await client.fetch_states(EUROPE)

    assert error.value.retry_after_seconds == 37
    await client.aclose()
