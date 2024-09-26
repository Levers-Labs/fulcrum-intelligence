from unittest.mock import AsyncMock, patch

import pytest
from httpx import Response

from commons.utilities.async_http_client import AsyncHttpClient


@pytest.mark.asyncio
async def test_get():
    client = AsyncHttpClient(base_url="http://test.com")

    # Mock the AsyncClient
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = Response(200, json={"key": "value"})

        result = await client.get("/endpoint", params={"param": "value"})

        assert result == {"key": "value"}
        mock_get.assert_called_once_with("http://test.com/endpoint", params={"param": "value"})


@pytest.mark.asyncio
async def test_post():
    client = AsyncHttpClient(base_url="http://test.com")

    # Mock the AsyncClient
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = Response(200, json={"success": True})

        result = await client.post("/endpoint", data={"key": "value"})

        assert result == {"success": True}
        mock_post.assert_called_once_with("http://test.com/endpoint", json={"key": "value"})
