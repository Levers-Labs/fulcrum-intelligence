from unittest.mock import AsyncMock, MagicMock

import pytest

from commons.clients.base import AsyncHttpClient


@pytest.mark.asyncio
async def test_get_request(mocker):
    # Setup
    client = AsyncHttpClient("https://api.example.com")
    endpoint = "/data"
    params = {"query": "test"}

    # Mock httpx.AsyncClient.get using mocker
    base_client_mock = MagicMock()
    client_mock = AsyncMock(name="client_mock")
    base_client_mock.__aenter__.return_value = client_mock

    client_mock.get = AsyncMock(return_value=MagicMock(json=MagicMock(return_value={"result": "success"})))

    mocker.patch("httpx.AsyncClient", return_value=base_client_mock)

    # Exercise
    response = await client.get(endpoint, params)

    # Verify
    assert response == {"result": "success"}
    client_mock.get.assert_called_once_with("https://api.example.com/data", params=params)


@pytest.mark.asyncio
async def test_post_request(mocker):
    # Setup
    client = AsyncHttpClient("https://api.example.com")
    endpoint = "/submit"
    data = {"key": "value"}

    # Mock httpx.AsyncClient.post using mocker
    base_client_mock = MagicMock()
    client_mock = AsyncMock(name="client_mock")
    base_client_mock.__aenter__.return_value = client_mock

    client_mock.post = AsyncMock(return_value=MagicMock(json=MagicMock(return_value={"result": "success"})))

    mocker.patch("httpx.AsyncClient", return_value=base_client_mock)

    # Exercise
    response = await client.post(endpoint, data)

    # Verify
    assert response == {"result": "success"}
    client_mock.post.assert_called_once_with("https://api.example.com/submit", json=data)
