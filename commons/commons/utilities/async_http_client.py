import urllib.parse
from typing import Any

import httpx
from pydantic import AnyHttpUrl


class AsyncHttpClient:
    def __init__(self, base_url: str | AnyHttpUrl):
        self.base_url = base_url

    async def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Makes a async http get request.
        endpoint: absolute or relative url
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(urllib.parse.urljoin(str(self.base_url), endpoint), params=params)
            return response.json()

    async def post(self, endpoint: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Makes a async http post request.
        endpoint: absolute or relative url
        data: request body
        """
        async with httpx.AsyncClient() as client:
            response = await client.post(urllib.parse.urljoin(str(self.base_url), endpoint), json=data)
            return response.json()
