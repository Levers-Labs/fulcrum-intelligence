import urllib.parse
from typing import Any

import httpx
from pydantic import HttpUrl


class AsyncHttpClient:
    def __init__(self, base_url: HttpUrl):
        self.base_url = base_url

    async def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.get(urllib.parse.urljoin(self.base_url, endpoint), params=params)
            return response.json()

    async def post(self, endpoint: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.post(urllib.parse.urljoin(self.base_url, endpoint), json=data)
            return response.json()
