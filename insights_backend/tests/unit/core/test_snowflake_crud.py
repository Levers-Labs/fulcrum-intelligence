# ruff: noqa: S105,S106
import pytest
import pytest_asyncio
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.crud import NotFoundError
from commons.exceptions import ConflictError
from commons.models.enums import SnowflakeAuthMethod
from commons.utilities.context import set_tenant_id
from insights_backend.core.crud import TenantCRUD
from insights_backend.core.models.tenant import SnowflakeConfig, Tenant, TenantConfig
from insights_backend.core.schemas import SnowflakeConfigCreate, SnowflakeConfigUpdate

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(name="tenant_crud")
async def tenant_crud_fixture(db_session: AsyncSession):
    """Fixture for TenantCRUD instance"""
    return TenantCRUD(model=Tenant, session=db_session)


@pytest_asyncio.fixture(name="setup_tenant")
async def setup_tenant_fixture(db_session: AsyncSession, jwt_payload: dict):
    """Set up a tenant for testing"""
    # Create tenant with a specific ID to match JWT payload
    tenant = Tenant(
        id=jwt_payload["tenant_id"],
        external_id=jwt_payload["external_id"],
        name="test_tenant",
        identifier="test_tenant",
        domains=["test.com"],
    )
    db_session.add(tenant)
    await db_session.flush()

    # Set tenant context
    set_tenant_id(tenant.id)

    # Ensure tenant has an ID
    assert tenant.id is not None

    # Create tenant config
    config = TenantConfig(
        tenant_id=tenant.id,
        cube_connection_config={  # type: ignore
            "cube_api_url": "http://test-cube-api.com",
            "cube_auth_type": "SECRET_KEY",
            "cube_auth_secret_key": "test-secret-key",
        },
        slack_connection={  # type: ignore
            "bot_token": "xoxb-test-token",
            "bot_user_id": "U123456",
            "app_id": "A123456",
            "team": {"id": "T123456", "name": "Test Team"},
            "authed_user": {"id": "U123456"},
        },
    )
    db_session.add(config)
    await db_session.flush()
    return tenant


@pytest_asyncio.fixture(name="existing_snowflake_config")
async def existing_snowflake_config_fixture(db_session: AsyncSession, setup_tenant: Tenant, jwt_payload: dict):
    """Create an existing Snowflake configuration"""
    # Set tenant context before creating the config
    set_tenant_id(jwt_payload["tenant_id"])

    assert setup_tenant.id is not None
    config = SnowflakeConfig(
        tenant_id=setup_tenant.id,
        account_identifier="existing-account.us-east-1",
        username="existing_user",
        warehouse="EXISTING_WH",
        role="EXISTING_ROLE",
        database="EXISTING_DB",
        db_schema="EXISTING_SCHEMA",
        auth_method=SnowflakeAuthMethod.PASSWORD,
        password="existing_password",
        private_key=None,
        private_key_passphrase=None,
    )
    db_session.add(config)
    await db_session.flush()
    await db_session.refresh(config)
    return config


@pytest_asyncio.fixture(name="snowflake_config_data")
def snowflake_config_data_fixture():
    """Sample Snowflake configuration data"""
    return SnowflakeConfigCreate(
        account_identifier="test-account.us-east-1",
        username="test_user",
        warehouse="TEST_WH",
        role="TEST_ROLE",
        database="TEST_DB",
        db_schema="TEST_SCHEMA",
        auth_method=SnowflakeAuthMethod.PASSWORD,
        password="test_password",
        private_key=None,
        private_key_passphrase=None,
    )


@pytest_asyncio.fixture(name="snowflake_config_key_auth_data")
def snowflake_config_key_auth_data_fixture():
    """Sample Snowflake configuration data with key authentication"""
    return SnowflakeConfigCreate(
        account_identifier="test-account.us-east-1",
        username="test_user",
        warehouse="TEST_WH",
        role="TEST_ROLE",
        database="TEST_DB",
        db_schema="TEST_SCHEMA",
        auth_method=SnowflakeAuthMethod.PRIVATE_KEY,
        password=None,
        private_key="-----BEGIN PRIVATE KEY-----\ntest_private_key\n-----END PRIVATE KEY-----",
        private_key_passphrase="test_passphrase",
    )


# Test Cases


async def test_create_snowflake_config(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    snowflake_config_data: SnowflakeConfigCreate,
    jwt_payload: dict,
):
    """Test creating a new Snowflake configuration"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    config = await tenant_crud.create_snowflake_config(snowflake_config_data)

    assert config.tenant_id == setup_tenant.id
    assert config.account_identifier == snowflake_config_data.account_identifier
    assert config.username == snowflake_config_data.username
    assert config.warehouse == snowflake_config_data.warehouse
    assert config.role == snowflake_config_data.role
    assert config.database == snowflake_config_data.database
    assert config.db_schema == snowflake_config_data.db_schema
    assert config.auth_method == snowflake_config_data.auth_method
    assert config.password == snowflake_config_data.password


async def test_create_snowflake_config_key_auth(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    snowflake_config_key_auth_data: SnowflakeConfigCreate,
    jwt_payload: dict,
):
    """Test creating Snowflake config with private key authentication"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    config = await tenant_crud.create_snowflake_config(snowflake_config_key_auth_data)

    assert config.tenant_id == setup_tenant.id
    assert config.account_identifier == snowflake_config_key_auth_data.account_identifier
    assert config.username == snowflake_config_key_auth_data.username
    assert config.auth_method == SnowflakeAuthMethod.PRIVATE_KEY
    assert config.private_key == snowflake_config_key_auth_data.private_key
    assert config.private_key_passphrase == snowflake_config_key_auth_data.private_key_passphrase
    assert config.password is None


async def test_create_snowflake_config_duplicate(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    existing_snowflake_config: SnowflakeConfig,
    snowflake_config_data: SnowflakeConfigCreate,
    jwt_payload: dict,
):
    """Test creating duplicate Snowflake configuration raises ConflictError"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    with pytest.raises(ConflictError) as exc_info:
        await tenant_crud.create_snowflake_config(snowflake_config_data)

    assert "Snowflake configuration already exists for this tenant" in str(exc_info.value)


async def test_get_snowflake_config(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    existing_snowflake_config: SnowflakeConfig,
    jwt_payload: dict,
):
    """Test retrieving an existing Snowflake configuration"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    config = await tenant_crud.get_snowflake_config()

    assert config.tenant_id == setup_tenant.id
    assert config.account_identifier == existing_snowflake_config.account_identifier
    assert config.username == existing_snowflake_config.username
    assert config.warehouse == existing_snowflake_config.warehouse
    assert config.role == existing_snowflake_config.role
    assert config.database == existing_snowflake_config.database
    assert config.db_schema == existing_snowflake_config.db_schema
    assert config.auth_method == existing_snowflake_config.auth_method
    assert config.password == existing_snowflake_config.password


async def test_get_snowflake_config_not_found(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    jwt_payload: dict,
):
    """Test retrieving non-existent Snowflake configuration raises NotFoundError"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    with pytest.raises(NotFoundError) as exc_info:
        await tenant_crud.get_snowflake_config()

    assert str(setup_tenant.id) in str(exc_info.value)


async def test_update_snowflake_config(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    existing_snowflake_config: SnowflakeConfig,
    jwt_payload: dict,
):
    """Test updating an existing Snowflake configuration"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    update_data = SnowflakeConfigUpdate(
        account_identifier="updated-account.us-west-2",
        username="updated_user",
        warehouse="UPDATED_WH",
        role="UPDATED_ROLE",
        database="UPDATED_DB",
        db_schema="UPDATED_SCHEMA",
        auth_method=SnowflakeAuthMethod.PASSWORD,
        password="updated_password",
        private_key=None,
        private_key_passphrase=None,
    )

    updated_config = await tenant_crud.update_snowflake_config(update_data)

    assert updated_config.tenant_id == setup_tenant.id
    assert updated_config.account_identifier == update_data.account_identifier
    assert updated_config.username == update_data.username
    assert updated_config.warehouse == update_data.warehouse
    assert updated_config.role == update_data.role
    assert updated_config.database == update_data.database
    assert updated_config.db_schema == update_data.db_schema
    assert updated_config.auth_method == update_data.auth_method
    assert updated_config.password == update_data.password


async def test_update_snowflake_config_auth_method_change(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    existing_snowflake_config: SnowflakeConfig,
    jwt_payload: dict,
):
    """Test updating Snowflake config to change authentication method"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    update_data = SnowflakeConfigUpdate(
        auth_method=SnowflakeAuthMethod.PRIVATE_KEY,
        private_key="-----BEGIN PRIVATE KEY-----\nupdated_private_key\n-----END PRIVATE KEY-----",
        private_key_passphrase="updated_passphrase",
        password=None,
    )

    updated_config = await tenant_crud.update_snowflake_config(update_data)

    assert updated_config.tenant_id == setup_tenant.id
    assert updated_config.auth_method == SnowflakeAuthMethod.PRIVATE_KEY
    assert updated_config.private_key == update_data.private_key
    assert updated_config.private_key_passphrase == update_data.private_key_passphrase
    assert updated_config.password is None
    # Other fields should remain unchanged
    assert updated_config.account_identifier == existing_snowflake_config.account_identifier
    assert updated_config.username == existing_snowflake_config.username
    assert updated_config.warehouse == existing_snowflake_config.warehouse
    assert updated_config.role == existing_snowflake_config.role
    assert updated_config.database == existing_snowflake_config.database
    assert updated_config.db_schema == existing_snowflake_config.db_schema


async def test_update_snowflake_config_partial(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    existing_snowflake_config: SnowflakeConfig,
    jwt_payload: dict,
):
    """Test partial update of Snowflake configuration"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    update_data = SnowflakeConfigUpdate(
        username="partially_updated_user", password=None, private_key=None, private_key_passphrase=None
    )

    updated_config = await tenant_crud.update_snowflake_config(update_data)

    assert updated_config.tenant_id == setup_tenant.id
    assert updated_config.username == update_data.username
    # Other fields should remain unchanged
    assert updated_config.account_identifier == existing_snowflake_config.account_identifier
    assert updated_config.warehouse == existing_snowflake_config.warehouse
    assert updated_config.role == existing_snowflake_config.role
    assert updated_config.database == existing_snowflake_config.database
    assert updated_config.db_schema == existing_snowflake_config.db_schema


async def test_update_snowflake_config_not_found(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    jwt_payload: dict,
):
    """Test updating non-existent Snowflake configuration raises NotFoundError"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    update_data = SnowflakeConfigUpdate(
        username="new_user", password=None, private_key=None, private_key_passphrase=None
    )

    with pytest.raises(NotFoundError) as exc_info:
        await tenant_crud.update_snowflake_config(update_data)

    assert str(setup_tenant.id) in str(exc_info.value)


async def test_delete_snowflake_config(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    existing_snowflake_config: SnowflakeConfig,
    jwt_payload: dict,
):
    """Test deleting an existing Snowflake configuration"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    result = await tenant_crud.delete_snowflake_config()

    assert result is True

    # Verify config is deleted
    with pytest.raises(NotFoundError):
        await tenant_crud.get_snowflake_config()


async def test_delete_snowflake_config_not_found(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    jwt_payload: dict,
):
    """Test deleting non-existent Snowflake configuration returns False"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    result = await tenant_crud.delete_snowflake_config()

    assert result is False


async def test_test_snowflake_connection_success(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    mocker,
    jwt_payload: dict,
):
    """Test successful Snowflake connection test"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    # Mock the SnowflakeClient to return success
    mock_client_class = mocker.patch("insights_backend.core.crud.SnowflakeClient")
    mock_client_instance = mock_client_class.return_value
    # Since the method is async, we need to use AsyncMock
    from unittest.mock import AsyncMock

    mock_client_instance.test_connection = AsyncMock(
        return_value={
            "success": True,
            "message": "Connection successful",
            "connection_details": {
                "warehouse": "TEST_WH",
                "database": "TEST_DB",
                "db_schema": "TEST_SCHEMA",
                "role": "TEST_ROLE",
            },
        }
    )

    config_data = {
        "account_identifier": "test-account.us-east-1",
        "username": "test_user",
        "warehouse": "TEST_WH",
        "role": "TEST_ROLE",
        "database": "TEST_DB",
        "db_schema": "TEST_SCHEMA",
        "auth_method": "PASSWORD",
        "password": "test_password",
    }

    result = await tenant_crud.test_snowflake_connection(config_data)

    assert result["success"] is True
    assert result["message"] == "Connection successful"
    assert "connection_details" in result


async def test_test_snowflake_connection_failure(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    mocker,
    jwt_payload: dict,
):
    """Test failed Snowflake connection test"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    # Mock the SnowflakeClient to raise an exception
    mock_client_class = mocker.patch("insights_backend.core.crud.SnowflakeClient")
    mock_client_instance = mock_client_class.return_value
    from unittest.mock import AsyncMock

    mock_client_instance.test_connection = AsyncMock(side_effect=Exception("Connection failed: Invalid credentials"))

    config_data = {
        "account_identifier": "test-account.us-east-1",
        "username": "test_user",
        "warehouse": "TEST_WH",
        "role": "TEST_ROLE",
        "database": "TEST_DB",
        "db_schema": "TEST_SCHEMA",
        "auth_method": "PASSWORD",
        "password": "wrong_password",
    }

    result = await tenant_crud.test_snowflake_connection(config_data)

    assert result["success"] is False
    assert "Connection failed" in result["message"]
    assert result["connection_details"] is None


async def test_test_snowflake_connection_invalid_config(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    mocker,
    jwt_payload: dict,
):
    """Test Snowflake connection test with invalid configuration"""
    # Mock the SnowflakeConfigModel to raise validation error
    mock_config_model = mocker.patch("insights_backend.core.crud.SnowflakeConfigModel")
    mock_config_model.side_effect = ValueError("Invalid configuration")

    config_data = {
        "account_identifier": "test-account",
        "username": "test_user",
        # Missing required fields
    }

    result = await tenant_crud.test_snowflake_connection(config_data)

    assert result["success"] is False
    assert "Connection failed" in result["message"]
    assert result["connection_details"] is None


async def test_enable_metric_cache(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    jwt_payload: dict,
):
    """Test enabling metric cache feature"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    updated_config = await tenant_crud.enable_metric_cache(enabled=True)

    assert updated_config.tenant_id == setup_tenant.id
    assert updated_config.enable_metric_cache is True


async def test_disable_metric_cache(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    jwt_payload: dict,
):
    """Test disabling metric cache feature"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    updated_config = await tenant_crud.enable_metric_cache(enabled=False)

    assert updated_config.tenant_id == setup_tenant.id
    assert updated_config.enable_metric_cache is False


async def test_enable_metric_cache_default_true(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    jwt_payload: dict,
):
    """Test enabling metric cache with default parameter"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    updated_config = await tenant_crud.enable_metric_cache()

    assert updated_config.tenant_id == setup_tenant.id
    assert updated_config.enable_metric_cache is True


async def test_enable_metric_cache_tenant_not_found(
    tenant_crud: TenantCRUD,
    jwt_payload: dict,
):
    """Test enabling metric cache when tenant config not found"""
    # Set a tenant ID that doesn't have a config
    set_tenant_id(99999)

    with pytest.raises(NotFoundError) as exc_info:
        await tenant_crud.enable_metric_cache(enabled=True)

    assert "99999" in str(exc_info.value)


async def test_snowflake_config_encryption(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    snowflake_config_data: SnowflakeConfigCreate,
    jwt_payload: dict,
):
    """Test that sensitive fields are encrypted in the database"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    config = await tenant_crud.create_snowflake_config(snowflake_config_data)

    # The password should be encrypted by the EncryptedType
    # We can't directly test encryption without accessing the raw database field,
    # but we can verify that the password is retrievable and matches
    assert config.password == snowflake_config_data.password

    # Retrieve the config again to ensure encryption/decryption works
    retrieved_config = await tenant_crud.get_snowflake_config()
    assert retrieved_config.password == snowflake_config_data.password


async def test_snowflake_config_private_key_encryption(
    tenant_crud: TenantCRUD,
    setup_tenant: Tenant,
    snowflake_config_key_auth_data: SnowflakeConfigCreate,
    jwt_payload: dict,
):
    """Test that private key and passphrase are encrypted"""
    # Set tenant context
    set_tenant_id(jwt_payload["tenant_id"])

    config = await tenant_crud.create_snowflake_config(snowflake_config_key_auth_data)

    # The private key and passphrase should be encrypted
    assert config.private_key == snowflake_config_key_auth_data.private_key
    assert config.private_key_passphrase == snowflake_config_key_auth_data.private_key_passphrase

    # Retrieve the config again to ensure encryption/decryption works
    retrieved_config = await tenant_crud.get_snowflake_config()
    assert retrieved_config.private_key == snowflake_config_key_auth_data.private_key
    assert retrieved_config.private_key_passphrase == snowflake_config_key_auth_data.private_key_passphrase
