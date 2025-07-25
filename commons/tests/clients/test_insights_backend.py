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
    mock_client.get.assert_called_once_with("/tenant/config/internal")


@pytest.mark.asyncio
async def test_get_tenant_config_not_found(mock_client):
    # Arrange
    mock_client.get = AsyncMock(side_effect=HttpClientError("Resource not found", status_code=404))

    # Act & Assert
    with pytest.raises(InvalidTenantError):
        await mock_client.get_tenant_config()

    mock_client.get.assert_called_once_with("/tenant/config/internal")


@pytest.mark.asyncio
async def test_get_tenant_config_other_error(mock_client):
    # Arrange
    mock_client.get = AsyncMock(side_effect=HttpClientError("Internal server error", status_code=500))

    # Act & Assert
    with pytest.raises(HttpClientError):
        await mock_client.get_tenant_config()

    mock_client.get.assert_called_once_with("/tenant/config/internal")


@pytest.mark.asyncio
async def test_list_alerts_success(mock_client):
    # Arrange
    expected_response = {"items": [], "total": 0, "page": 1, "size": 10}
    mock_client.get = AsyncMock(return_value=expected_response)

    # Act
    result = await mock_client.list_alerts(
        page=1,
        size=10,
        grains=["day", "week"],
        is_active=True,
        is_published=False,
        metric_ids=["metric1", "metric2"],
        story_groups=["group1", "group2"],
    )

    # Assert
    assert result == expected_response
    mock_client.get.assert_called_once_with(
        "/notification/alerts",
        params={
            "page": 1,
            "size": 10,
            "grains": ["day", "week"],
            "is_active": "true",
            "is_published": "false",
            "metric_ids": ["metric1", "metric2"],
            "story_groups": ["group1", "group2"],
        },
    )


@pytest.mark.asyncio
async def test_create_notification_execution_success(mock_client):
    # Arrange
    execution_data = {
        "notification_type": "ALERT",
        "status": "SUCCESS",
        "delivery_meta": {
            "channels": ["email", "slack"],
            "recipients": ["user@example.com"],
            "delivery_status": "delivered",
        },
        "run_meta": {"run_id": "test-run-id", "start_time": "2024-03-20T10:00:00Z", "end_time": "2024-03-20T10:01:00Z"},
        "alert_meta": {"alert_id": "test-alert-id", "alert_name": "Test Alert", "metrics": ["metric1", "metric2"]},
    }
    expected_response = {"id": "test-execution-id", **execution_data}
    mock_client.post = AsyncMock(return_value=expected_response)

    # Act
    result = await mock_client.create_notification_execution(execution_data)

    # Assert
    assert result == expected_response
    mock_client.post.assert_called_once_with("/notification/executions", data=execution_data)


@pytest.mark.asyncio
async def test_create_notification_execution_report(mock_client):
    # Arrange
    execution_data = {
        "notification_type": "REPORT",
        "status": "SUCCESS",
        "delivery_meta": {"channels": ["email"], "recipients": ["user@example.com"], "delivery_status": "delivered"},
        "run_meta": {"run_id": "test-run-id", "start_time": "2024-03-20T10:00:00Z", "end_time": "2024-03-20T10:01:00Z"},
        "report_meta": {"report_id": "test-report-id", "report_name": "Weekly Report", "period": "2024-W12"},
    }
    expected_response = {"id": "test-execution-id", **execution_data}
    mock_client.post = AsyncMock(return_value=expected_response)

    # Act
    result = await mock_client.create_notification_execution(execution_data)

    # Assert
    assert result == expected_response
    mock_client.post.assert_called_once_with("/notification/executions", data=execution_data)


@pytest.mark.asyncio
async def test_create_notification_execution_failure(mock_client):
    # Arrange
    execution_data = {
        "notification_type": "ALERT",
        "status": "FAILURE",
        "delivery_meta": {"channels": ["email"], "delivery_status": "failed"},
        "error_info": {
            "error_type": "DELIVERY_ERROR",
            "error_message": "Failed to send email",
            "stack_trace": "Error stack trace",
        },
        "alert_meta": {"alert_id": "test-alert-id"},
    }
    expected_response = {"id": "test-execution-id", **execution_data}
    mock_client.post = AsyncMock(return_value=expected_response)

    # Act
    result = await mock_client.create_notification_execution(execution_data)

    # Assert
    assert result == expected_response
    assert result["status"] == "FAILURE"
    assert "error_info" in result
    mock_client.post.assert_called_once_with("/notification/executions", data=execution_data)


@pytest.mark.asyncio
async def test_create_notification_execution_http_error(mock_client):
    # Arrange
    execution_data = {"notification_type": "ALERT", "status": "SUCCESS", "alert_meta": {"alert_id": "test-alert-id"}}
    mock_client.post = AsyncMock(side_effect=HttpClientError("Internal server error", status_code=500))

    # Act & Assert
    with pytest.raises(HttpClientError) as exc_info:
        await mock_client.create_notification_execution(execution_data)

    assert exc_info.value.status_code == 500
    mock_client.post.assert_called_once_with("/notification/executions", data=execution_data)


# New test methods for functionality added in the diff


@pytest.mark.asyncio
async def test_get_tenant_details_success(mock_client):
    # Arrange
    expected_response = {"id": 1, "name": "Test Tenant", "identifier": "test_tenant", "domains": ["test.com"]}
    mock_client.get = AsyncMock(return_value=expected_response)

    # Act
    result = await mock_client.get_tenant_details()

    # Assert
    assert result == expected_response
    assert result["id"] == 1
    assert result["name"] == "Test Tenant"
    assert result["identifier"] == "test_tenant"
    assert result["domains"] == ["test.com"]
    mock_client.get.assert_called_once_with("/tenant/details")


@pytest.mark.asyncio
async def test_get_tenant_details_failure(mock_client):
    # Arrange
    mock_client.get = AsyncMock(side_effect=HttpClientError("API Error", status_code=500))

    # Act & Assert
    with pytest.raises(HttpClientError) as exc_info:
        await mock_client.get_tenant_details()

    assert exc_info.value.status_code == 500
    mock_client.get.assert_called_once_with("/tenant/details")


@pytest.mark.asyncio
async def test_get_tenant_details_empty_response(mock_client):
    # Arrange
    expected_response = {}
    mock_client.get = AsyncMock(return_value=expected_response)

    # Act
    result = await mock_client.get_tenant_details()

    # Assert
    assert result == {}
    mock_client.get.assert_called_once_with("/tenant/details")


@pytest.mark.asyncio
async def test_get_snowflake_config_success(mock_client):
    # Arrange
    expected_response = {
        "account_identifier": "test-account.us-east-1",
        "username": "test_user",
        "warehouse": "TEST_WH",
        "role": "TEST_ROLE",
        "database": "TEST_DB",
        "db_schema": "TEST_SCHEMA",
        "auth_method": "PASSWORD",
    }
    mock_client.get = AsyncMock(return_value=expected_response)

    # Act
    result = await mock_client.get_snowflake_config()

    # Assert
    assert result == expected_response
    assert result["account_identifier"] == "test-account.us-east-1"
    assert result["username"] == "test_user"
    assert result["warehouse"] == "TEST_WH"
    assert result["role"] == "TEST_ROLE"
    assert result["database"] == "TEST_DB"
    assert result["db_schema"] == "TEST_SCHEMA"
    assert result["auth_method"] == "PASSWORD"
    mock_client.get.assert_called_once_with("/tenant/snowflake-config/internal")


@pytest.mark.asyncio
async def test_get_snowflake_config_failure(mock_client):
    # Arrange
    mock_client.get = AsyncMock(side_effect=HttpClientError("Config not found", status_code=404))

    # Act & Assert
    with pytest.raises(InvalidTenantError) as exc_info:
        await mock_client.get_snowflake_config()

    assert "not found" in str(exc_info.value)
    mock_client.get.assert_called_once_with("/tenant/snowflake-config/internal")


@pytest.mark.asyncio
async def test_get_snowflake_config_with_private_key(mock_client):
    # Arrange
    expected_response = {
        "account_identifier": "test-account.us-east-1",
        "username": "test_user",
        "warehouse": "TEST_WH",
        "role": "TEST_ROLE",
        "database": "TEST_DB",
        "db_schema": "TEST_SCHEMA",
        "auth_method": "PRIVATE_KEY",
        "private_key": "-----BEGIN PRIVATE KEY-----\ntest_key\n-----END PRIVATE KEY-----",
        "private_key_passphrase": "test_passphrase",
    }
    mock_client.get = AsyncMock(return_value=expected_response)

    # Act
    result = await mock_client.get_snowflake_config()

    # Assert
    assert result == expected_response
    assert result["auth_method"] == "PRIVATE_KEY"
    assert result["private_key"] == "-----BEGIN PRIVATE KEY-----\ntest_key\n-----END PRIVATE KEY-----"
    assert result["private_key_passphrase"] == "test_passphrase"  # noqa
    mock_client.get.assert_called_once_with("/tenant/snowflake-config/internal")


@pytest.mark.asyncio
async def test_get_snowflake_config_empty_response(mock_client):
    # Arrange
    expected_response = {}
    mock_client.get = AsyncMock(return_value=expected_response)

    # Act
    result = await mock_client.get_snowflake_config()

    # Assert
    assert result == {}
    mock_client.get.assert_called_once_with("/tenant/snowflake-config/internal")
