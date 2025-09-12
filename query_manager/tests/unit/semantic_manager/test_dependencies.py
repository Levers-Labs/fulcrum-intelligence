"""Tests for semantic_manager dependencies."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from commons.clients.snowflake import SnowflakeClient
from query_manager.semantic_manager.dependencies import get_cache_manager, get_semantic_manager, get_snowflake_client


@pytest.mark.asyncio
async def test_get_semantic_manager(db_session):
    """Test getting semantic manager dependency."""
    manager = await get_semantic_manager(db_session)

    assert manager is not None
    assert hasattr(manager, "session")
    assert manager.session == db_session


@pytest.mark.asyncio
async def test_get_snowflake_client_success():
    """Test successful Snowflake client creation."""
    mock_insight_client = AsyncMock()
    mock_insight_client.get_snowflake_config.return_value = {
        "account_identifier": "test_account",
        "username": "test_user",
        "password": "test_password",
        "database": "test_db",
        "db_schema": "test_schema",
        "auth_method": "username_password",
        "warehouse": "test_warehouse",
        "role": "test_role",
    }

    client = await get_snowflake_client(mock_insight_client)

    assert isinstance(client, SnowflakeClient)
    mock_insight_client.get_snowflake_config.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_snowflake_client_with_private_key():
    """Test Snowflake client creation with private key auth."""
    mock_insight_client = AsyncMock()
    mock_insight_client.get_snowflake_config.return_value = {
        "account_identifier": "test_account",
        "username": "test_user",
        "private_key": "-----BEGIN PRIVATE KEY-----\ntest_key\n-----END PRIVATE KEY-----",
        "private_key_passphrase": "test_passphrase",
        "database": "test_db",
        "db_schema": "test_schema",
        "auth_method": "private_key",
    }

    client = await get_snowflake_client(mock_insight_client)

    assert isinstance(client, SnowflakeClient)
    mock_insight_client.get_snowflake_config.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_snowflake_client_minimal_config():
    """Test Snowflake client creation with minimal required config."""
    mock_insight_client = AsyncMock()
    mock_insight_client.get_snowflake_config.return_value = {
        "account_identifier": "test_account",
        "username": "test_user",
        "database": "test_db",
        "db_schema": "test_schema",
        "auth_method": "username_password",
    }

    client = await get_snowflake_client(mock_insight_client)

    assert isinstance(client, SnowflakeClient)


@pytest.mark.asyncio
async def test_get_cache_manager_success(db_session):
    """Test successful cache manager creation."""
    mock_snowflake_client = MagicMock(spec=SnowflakeClient)
    mock_insight_client = AsyncMock()
    mock_insight_client.get_tenant_details.return_value = {"identifier": "test_tenant", "name": "Test Tenant"}

    cache_manager = await get_cache_manager(db_session, mock_snowflake_client, mock_insight_client)

    assert cache_manager is not None
    assert cache_manager.session == db_session
    assert cache_manager.snowflake_client == mock_snowflake_client
    assert cache_manager.tenant_identifier == "test_tenant"
    mock_insight_client.get_tenant_details.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_cache_manager_with_none_snowflake_client(db_session):
    """Test cache manager creation with None Snowflake client."""
    mock_insight_client = AsyncMock()
    mock_insight_client.get_tenant_details.return_value = {"identifier": "test_tenant"}

    cache_manager = await get_cache_manager(db_session, None, mock_insight_client)

    assert cache_manager is not None
    assert cache_manager.snowflake_client is None
    assert cache_manager.tenant_identifier == "test_tenant"


@pytest.mark.asyncio
async def test_get_cache_manager_missing_tenant_identifier(db_session):
    """Test cache manager creation when tenant details don't have identifier."""
    mock_snowflake_client = MagicMock(spec=SnowflakeClient)
    mock_insight_client = AsyncMock()
    mock_insight_client.get_tenant_details.return_value = {"name": "Test Tenant"}  # Missing identifier

    cache_manager = await get_cache_manager(db_session, mock_snowflake_client, mock_insight_client)

    assert cache_manager is not None
    assert cache_manager.tenant_identifier is None
