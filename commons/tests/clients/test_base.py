from json import JSONDecodeError
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import (
    HTTPStatusError,
    InvalidURL,
    Response,
    TimeoutException,
)

from commons.clients.base import AsyncHttpClient, HttpClientError


@pytest.fixture
def async_http_client():
    base_url = "https://example.com"
    auth = MagicMock()
    return AsyncHttpClient(base_url, auth)


@pytest.mark.asyncio
async def test_get_url(async_http_client):
    base_url = "https://example.com"
    endpoint = "/api/users"
    expected_url = "https://example.com/api/users"
    assert async_http_client._get_url(base_url, endpoint) == expected_url


@pytest.mark.asyncio
async def test_make_request_success(async_http_client):
    endpoint = "/api/users"
    response_mock = AsyncMock(spec=Response)
    response_mock.json.return_value = {"data": "test"}
    with patch.object(async_http_client, "_get_url", return_value="https://example.com/api/users"), patch(
        "httpx.AsyncClient.request", return_value=response_mock
    ) as request_mock:
        response = await async_http_client._make_request("GET", endpoint)
        assert response == response_mock
        request_mock.assert_awaited_once_with("GET", "https://example.com/api/users", timeout=60)


@pytest.mark.asyncio
async def test_make_request_with_error_json_decode(async_http_client):
    endpoint = "/api/users"
    response_mock = AsyncMock(spec=Response)
    response_mock.status_code = 400
    response_mock.json.side_effect = JSONDecodeError("Failed to decode JSON", "", 0)
    with patch.object(async_http_client, "_get_url", return_value="https://example.com/api/users"), patch(
        "httpx.AsyncClient.request",
        side_effect=HTTPStatusError("Bad request", request=MagicMock(), response=response_mock),
    ):
        with pytest.raises(HttpClientError) as exc_info:
            await async_http_client._make_request("GET", endpoint)
        assert str(exc_info.value) == "HTTP error occurred: Bad request"
        assert exc_info.value.status_code == 400
        assert exc_info.value.content is None
        assert exc_info.value.url == "https://example.com/api/users"


@pytest.mark.asyncio
async def test_make_request_unauthorized(async_http_client):
    endpoint = "/api/users"
    response_mock = AsyncMock(spec=Response)
    response_mock.status_code = 401
    response_mock.json.return_value = {"description": "Token is expired"}
    with patch.object(async_http_client, "_get_url", return_value="https://example.com/api/users"), patch(
        "httpx.AsyncClient.request",
        side_effect=HTTPStatusError("Token is expired", request=MagicMock(), response=response_mock),
    ):
        with pytest.raises(HttpClientError) as exc_info:
            await async_http_client._make_request("GET", endpoint)
        assert str(exc_info.value) == "Token is expired"
        assert exc_info.value.status_code == 401
        assert exc_info.value.content == {"description": "Token is expired"}
        assert exc_info.value.url == "https://example.com/api/users"


@pytest.mark.asyncio
async def test_make_request_not_found(async_http_client):
    endpoint = "/api/users"
    response_mock = AsyncMock(spec=Response)
    response_mock.status_code = 404
    with patch.object(async_http_client, "_get_url", return_value="https://example.com/api/users"), patch(
        "httpx.AsyncClient.request",
        side_effect=HTTPStatusError("Resource not found", request=MagicMock(), response=response_mock),
    ):
        with pytest.raises(HttpClientError) as exc_info:
            await async_http_client._make_request("GET", endpoint)
        assert str(exc_info.value) == "Resource not found"
        assert exc_info.value.status_code == 404
        assert exc_info.value.url == "https://example.com/api/users"


@pytest.mark.asyncio
async def test_make_request_invalid_url(async_http_client):
    endpoint = "/api/users"
    with patch.object(async_http_client, "_get_url", return_value="invalid_url"), patch(
        "httpx.AsyncClient.request", side_effect=InvalidURL("Invalid URL")
    ):
        with pytest.raises(HttpClientError) as exc_info:
            await async_http_client._make_request("GET", endpoint)
        assert str(exc_info.value) == "Invalid URL: Invalid URL"
        assert exc_info.value.url == "invalid_url"


@pytest.mark.asyncio
async def test_make_request_unprocessable_entity(async_http_client):
    endpoint = "/api/users"
    response_mock = AsyncMock(spec=Response)
    response_mock.status_code = 422
    with patch.object(async_http_client, "_get_url", return_value="https://example.com/api/users"), patch(
        "httpx.AsyncClient.request",
        side_effect=HTTPStatusError("Invalid input", request=MagicMock(), response=response_mock),
    ):
        with pytest.raises(HttpClientError) as exc_info:
            await async_http_client._make_request("GET", endpoint)
        assert str(exc_info.value) == "Invalid input"
        assert exc_info.value.status_code == 422
        assert exc_info.value.url == "https://example.com/api/users"


@pytest.mark.asyncio
async def test_make_request_timeout(async_http_client):
    endpoint = "/api/users"
    with patch.object(async_http_client, "_get_url", return_value="https://example.com/api/users"), patch(
        "httpx.AsyncClient.request", side_effect=TimeoutException("Request timed out")
    ):
        with pytest.raises(HttpClientError) as exc_info:
            await async_http_client._make_request("GET", endpoint)
        assert str(exc_info.value) == "Request timed out"
        assert exc_info.value.url == "https://example.com/api/users"


@pytest.mark.asyncio
async def test_get(async_http_client):
    endpoint = "/api/users"
    response_mock = AsyncMock(spec=Response)
    response_mock.json.return_value = {"data": "test"}
    with patch.object(async_http_client, "_make_request", return_value=response_mock) as make_request_mock:
        response = await async_http_client.get(endpoint, params={"page": 1})
        assert response == {"data": "test"}
        make_request_mock.assert_awaited_once_with("GET", endpoint, params={"page": 1})


@pytest.mark.asyncio
async def test_post(async_http_client):
    endpoint = "/api/users"
    data = {"name": "John"}
    response_mock = AsyncMock(spec=Response)
    response_mock.json.return_value = {"id": 1, "name": "John"}
    with patch.object(async_http_client, "_make_request", return_value=response_mock) as make_request_mock:
        response = await async_http_client.post(endpoint, data=data)
        assert response == {"id": 1, "name": "John"}
        make_request_mock.assert_awaited_once_with("POST", endpoint, json=data)
