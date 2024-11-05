from unittest.mock import AsyncMock

import pytest

from commons.clients.base import HttpClientError
from commons.clients.insight_backend import InsightBackendClient
from commons.exceptions import InvalidTenantError


@pytest.fixture
def mock_client():
    return InsightBackendClient(base_url="http://test-url")


@pytest.mark.asyncio
async def test_get_user_success(mock_client):
    # Arrange
    user_id = 123
    expected_response = {"id": user_id, "name": "Test User"}
    mock_client.get = AsyncMock(return_value=expected_response)

    # Act
    result = await mock_client.get_user(user_id=user_id, token="test-token")  # noqa

    # Assert
    assert result == expected_response
    mock_client.get.assert_called_once_with(f"/users/{user_id}")


@pytest.mark.asyncio
async def test_get_tenant_config_success(mock_client):
    # Arrange
    expected_config = {"tenant_id": "test-tenant", "settings": {}}
    mock_client.get = AsyncMock(return_value=expected_config)

    # Act
    result = await mock_client.get_tenant_config()

    # Assert
    assert result == expected_config
    mock_client.get.assert_called_once_with("/tenant/config")


@pytest.mark.asyncio
async def test_get_tenant_config_not_found(mock_client):
    # Arrange
    mock_client.get = AsyncMock(side_effect=HttpClientError("Resource not found", status_code=404))

    # Act & Assert
    with pytest.raises(InvalidTenantError):
        await mock_client.get_tenant_config()

    mock_client.get.assert_called_once_with("/tenant/config")


@pytest.mark.asyncio
async def test_get_tenant_config_other_error(mock_client):
    # Arrange
    mock_client.get = AsyncMock(side_effect=HttpClientError("Internal server error", status_code=500))

    # Act & Assert
    with pytest.raises(HttpClientError):
        await mock_client.get_tenant_config()

    mock_client.get.assert_called_once_with("/tenant/config")
