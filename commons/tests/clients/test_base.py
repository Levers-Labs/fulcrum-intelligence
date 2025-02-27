from json import JSONDecodeError
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import (
    HTTPStatusError,
    InvalidURL,
    Response,
    TimeoutException,
)

from commons.auth.auth import UnauthorizedException
from commons.clients.base import AsyncHttpClient, HttpClient, HttpClientError


@pytest.fixture
def async_http_client():
    base_url = "https://example.com"
    auth = MagicMock()
    return AsyncHttpClient(base_url=base_url, api_version="v1", auth=auth)


@pytest.fixture
def sync_http_client():
    base_url = "https://example.com"
    auth = MagicMock()
    return HttpClient(base_url=base_url, api_version="v1", auth=auth)


@pytest.fixture(autouse=True)
def mock_get_tenant_id():
    with patch("commons.clients.base.get_tenant_id", return_value="test_tenant_id"):
        yield


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
        request_mock.assert_awaited_once_with(
            "GET", "https://example.com/api/users", timeout=60, headers={"X-Tenant-Id": "test_tenant_id"}
        )


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
        assert str(exc_info.value) == "Request failed: Request timed out"
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


@pytest.mark.parametrize("client_fixture", ["async_http_client", "sync_http_client"])
@pytest.mark.parametrize(
    "test_case",
    [
        ("https://example.com", "v1", "https://example.com/v1"),
        ("https://example.com/", "v1", "https://example.com/v1"),
        ("https://example.com/v1", "v1", "https://example.com/v1"),
        ("https://example.com", "", "https://example.com"),
    ],
)
def test_get_base_url_with_version(request, client_fixture, test_case):
    client = request.getfixturevalue(client_fixture)
    base_url, api_version, expected = test_case
    result = client.get_base_url_with_version(base_url, api_version)
    assert result == expected


@pytest.mark.parametrize("client_fixture", ["async_http_client", "sync_http_client"])
@pytest.mark.parametrize(
    "endpoint,expected_headers",
    [
        ("v1/tenants/all", {}),  # bypass endpoint
        ("/api/users", {"X-Tenant-Id": "test_tenant_id"}),  # regular endpoint
    ],
)
def test_add_tenant_id_to_headers(request, client_fixture, endpoint, expected_headers):
    client = request.getfixturevalue(client_fixture)
    headers = {}
    result = client._add_tenant_id_to_headers(headers, endpoint)
    assert result == expected_headers


@pytest.mark.parametrize("client_fixture", ["async_http_client", "sync_http_client"])
def test_add_tenant_id_to_headers_no_tenant(request, client_fixture):
    client = request.getfixturevalue(client_fixture)
    with patch("commons.clients.base.get_tenant_id", return_value=None):
        with pytest.raises(UnauthorizedException) as exc_info:
            client._add_tenant_id_to_headers({}, "/api/users")
        assert str(exc_info.value.detail) == "Tenant ID not found in context"


# Sync client tests
def test_make_sync_request_success(sync_http_client):
    endpoint = "/api/users"
    response_mock = MagicMock(spec=Response)
    response_mock.json.return_value = {"data": "test"}
    with patch.object(sync_http_client, "_get_url", return_value="https://example.com/api/users"), patch(
        "httpx.Client.request", return_value=response_mock
    ) as request_mock:
        response = sync_http_client._make_request("GET", endpoint)
        assert response == response_mock
        request_mock.assert_called_once_with(
            "GET", "https://example.com/api/users", timeout=60, headers={"X-Tenant-Id": "test_tenant_id"}
        )


def test_sync_get(sync_http_client):
    endpoint = "/api/users"
    response_mock = MagicMock(spec=Response)
    response_mock.json.return_value = {"data": "test"}

    with patch.object(sync_http_client, "_make_request", return_value=response_mock) as make_request_mock:
        response = sync_http_client.get(endpoint, params={"page": 1})
        assert response == {"data": "test"}
        make_request_mock.assert_called_once_with("GET", endpoint, params={"page": 1})


def test_sync_post(sync_http_client):
    endpoint = "/api/users"
    data = {"name": "John"}
    response_mock = MagicMock(spec=Response)
    response_mock.json.return_value = {"id": 1, "name": "John"}

    with patch.object(sync_http_client, "_make_request", return_value=response_mock) as make_request_mock:
        response = sync_http_client.post(endpoint, data=data)
        assert response == {"id": 1, "name": "John"}
        make_request_mock.assert_called_once_with("POST", endpoint, json=data)


def test_sync_delete(sync_http_client):
    endpoint = "/api/users/1"
    response_mock = MagicMock(spec=Response)
    response_mock.json.return_value = {"status": "success"}

    with patch.object(sync_http_client, "_make_request", return_value=response_mock) as make_request_mock:
        response = sync_http_client.delete(endpoint)
        assert response == {"status": "success"}
        make_request_mock.assert_called_once_with("DELETE", endpoint)
