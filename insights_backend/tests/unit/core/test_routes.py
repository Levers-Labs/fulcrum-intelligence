# ruff: noqa: S105,S106
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from fastapi import HTTPException
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from commons.db.crud import NotFoundError
from commons.models.enums import SnowflakeAuthMethod
from commons.models.tenant import CubeConnectionConfig, SlackConnectionConfig
from commons.utilities.context import set_tenant_id
from insights_backend.core.models.tenant import SnowflakeConfig, Tenant, TenantConfig

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(name="insert_tenant")
async def insert_tenant_fixture(db_session: AsyncSession, jwt_payload: dict):
    set_tenant_id(jwt_payload["tenant_id"])
    tenant = await db_session.execute(select(Tenant).filter_by(external_id=jwt_payload["external_id"]))
    tenant = tenant.scalar_one_or_none()  # type: ignore
    if not tenant:
        tenant = Tenant(  # type: ignore
            external_id=jwt_payload["external_id"], name="test_tenant", identifier="test_tenant", domains=["test.com"]
        )
        db_session.add(tenant)
        await db_session.flush()  # Flush to get the tenant ID

        # Create minimal TenantConfig with default dict values for JSONB fields
        config = TenantConfig(
            tenant_id=tenant.id,  # type: ignore
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


@pytest_asyncio.fixture(name="snowflake_config_create_data")
def snowflake_config_create_data_fixture():
    """Sample data for creating Snowflake config"""
    return {
        "account_identifier": "test-account.us-east-1",
        "username": "test_user",
        "warehouse": "TEST_WH",
        "role": "TEST_ROLE",
        "database": "TEST_DB",
        "db_schema": "TEST_SCHEMA",
        "auth_method": "PASSWORD",
        "password": "test_password",
    }


@pytest_asyncio.fixture(name="snowflake_config_key_auth_data")
def snowflake_config_key_auth_data_fixture():
    """Sample data for creating Snowflake config with key authentication"""
    return {
        "account_identifier": "test-account.us-east-1",
        "username": "test_user",
        "warehouse": "TEST_WH",
        "role": "TEST_ROLE",
        "database": "TEST_DB",
        "db_schema": "TEST_SCHEMA",
        "auth_method": "PRIVATE_KEY",
        "private_key": "-----BEGIN PRIVATE KEY-----\ntest_private_key\n-----END PRIVATE KEY-----",
        "private_key_passphrase": "test_passphrase",
    }


@pytest_asyncio.fixture(name="insert_snowflake_config")
async def insert_snowflake_config_fixture(db_session: AsyncSession, jwt_payload: dict):
    """Insert a test Snowflake config"""
    set_tenant_id(jwt_payload["tenant_id"])
    config = SnowflakeConfig(
        tenant_id=jwt_payload["tenant_id"],
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


async def test_create_user(insert_tenant, db_user_json: dict, async_client: AsyncClient, db_session: AsyncSession):
    # Act
    response = await async_client.post("/v1/users/", json=db_user_json)
    # Assert
    assert response.status_code == 200
    db_user = response.json()
    assert "id" in db_user


async def test_list_users(insert_tenant, db_user_json, async_client: AsyncClient, db_session: AsyncSession):
    # Create User first
    response = await async_client.post("/v1/users/", json=db_user_json)
    user_id = response.json()["id"]
    # Act
    response = await async_client.get("/v1/users/")
    data = response.json()
    # Assert
    assert response.status_code == status.HTTP_200_OK
    assert len(data["results"]) == 1
    assert data["results"][0]["id"] == user_id


async def test_get_user(insert_tenant, db_user_json, async_client: AsyncClient, db_session: AsyncSession):
    # Create User first
    response = await async_client.post("/v1/users/", json=db_user_json)
    user_id = response.json()["id"]
    # Act
    response = await async_client.get(f"/v1/users/{user_id}")
    data = response.json()
    # Assert
    assert response.status_code == status.HTTP_200_OK
    assert data["id"] == user_id


async def test_get_user_by_email(insert_tenant, db_user_json, async_client: AsyncClient, db_session: AsyncSession):
    # Create User first
    response = await async_client.post("/v1/users/", json=db_user_json)
    user_id = response.json()["id"]

    # Act
    response = await async_client.get(f"/v1/users/user-by-email/{db_user_json['email']}")
    data = response.json()
    # Assert
    assert response.status_code == status.HTTP_200_OK
    assert data["id"] == user_id


async def test_update_user(insert_tenant, db_user_json, async_client: AsyncClient, db_session: AsyncSession):
    # Create User first
    response = await async_client.post("/v1/users/", json=db_user_json)
    user_id = response.json()["id"]

    # Update user
    user_json = {
        "name": "test_name",
        "email": db_user_json["email"],
        "provider": "email",
        "external_user_id": "auth0|001123",
        "profile_picture": "http://test-some-url.com",
    }
    # Act
    response = await async_client.put(f"/v1/users/{user_id}", json=user_json)
    # Assert
    assert response.status_code == status.HTTP_200_OK
    response_data = response.json()
    assert response_data["profile_picture"] == "http://test-some-url.com"

    # Act
    response = await async_client.put("/v1/users/-4", json=user_json)
    # Assert
    assert response.status_code == status.HTTP_404_NOT_FOUND


async def test_delete_user(insert_tenant, db_user_json, async_client: AsyncClient, db_session: AsyncSession):
    # Create User first
    response = await async_client.post("/v1/users/", json=db_user_json)
    user_id = response.json()["id"]

    # Act
    response = await async_client.delete(f"/v1/users/{user_id}")
    # Assert
    assert response.status_code == status.HTTP_200_OK


async def test_list_tenants(insert_tenant, async_client: AsyncClient, db_session: AsyncSession):
    # Act
    response = await async_client.get("/v1/tenants/all")
    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert "results" in data
    assert "count" in data
    assert len(data["results"]) == 1
    assert data["results"][0]["id"] == insert_tenant.id


async def test_get_tenant_config(mocker, insert_tenant, async_client: AsyncClient, db_session: AsyncSession):
    # Act
    response = await async_client.get("/v1/tenant/config")
    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert "cube_connection_config" in data

    # Assert
    mocker.patch(
        "insights_backend.core.crud.TenantCRUD.get_tenant_config", side_effect=NotFoundError("Tenant not found")
    )

    # Act
    response = await async_client.get("/v1/tenant/config")
    # Assert
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["detail"] == "Tenant not found"


async def test_get_tenant_config_internal(insert_tenant, async_client: AsyncClient, db_session: AsyncSession):
    # Act
    response = await async_client.get("/v1/tenant/config/internal")
    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert "cube_connection_config" in data
    assert "cube_auth_secret_key" in data["cube_connection_config"]


async def test_get_tenant_config_internal_not_found(mocker, async_client: AsyncClient, db_session: AsyncSession):
    # Prepare
    # Mock tenant config to raise NotFoundError
    mocker.patch(
        "insights_backend.core.crud.TenantCRUD.get_tenant_config", side_effect=NotFoundError("Tenant not found")
    )
    # Act
    response = await async_client.get("/v1/tenant/config/internal")
    # Assert
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["detail"] == "Tenant not found"


async def test_update_tenant_config(insert_tenant, async_client: AsyncClient, db_session: AsyncSession):
    # Prepare the new configuration data
    new_config_data = {
        "cube_connection_config": {
            "cube_api_url": "http://new-cube-api.com",
            "cube_auth_type": "SECRET_KEY",
            "cube_auth_secret_key": "new-secret-key",
        },
        "enable_story_generation": True,
    }

    # Act: Update the tenant configuration with tenant_id in the headers
    response = await async_client.put(
        "/v1/tenant/config", json=new_config_data, headers={"X-Tenant-Id": str(insert_tenant.id)}
    )

    # Assert: Check the response status code
    assert response.status_code == status.HTTP_200_OK

    # Assert: Check the response data
    updated_config = response.json()
    cube_config = updated_config["cube_connection_config"]
    assert "cube_api_url" in cube_config
    assert cube_config["cube_api_url"] == new_config_data["cube_connection_config"]["cube_api_url"]  # type: ignore


async def test_generate_authorize_url(async_client: AsyncClient):
    # Act
    response = await async_client.get("/v1/slack/oauth/authorize")

    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert "authorization_url" in data
    assert data["authorization_url"].startswith("https://slack.com/oauth/v2/authorize")


async def test_oauth_callback_success(async_client: AsyncClient, mocker, insert_tenant):
    # Mock the SlackOAuthService.authorize_oauth_code method
    mock_slack_config = SlackConnectionConfig(
        bot_token="xoxb-test-token",  # noqa
        bot_user_id="U123456",
        app_id="A123456",
        team={"id": "T123456", "name": "Test Team"},
        authed_user={"id": "U123456"},
    )
    mocker.patch(
        "insights_backend.services.slack_oauth.SlackOAuthService.authorize_oauth_code", return_value=mock_slack_config
    )

    # Act
    response = await async_client.post("/v1/slack/oauth/callback", params={"code": "test_code"})

    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["slack_connection"]["bot_user_id"] == "U123456"
    assert data["slack_connection"]["team"]["name"] == "Test Team"


async def test_oauth_callback_invalid_code(async_client: AsyncClient, mocker):
    # Mock the service to raise an HTTPException for invalid code
    mocker.patch(
        "insights_backend.services.slack_oauth.SlackOAuthService.authorize_oauth_code",
        side_effect=HTTPException(status_code=400, detail="Slack OAuth error: invalid_code"),
    )

    # Act
    response = await async_client.post("/v1/slack/oauth/callback", params={"code": "invalid_code"})

    # Assert
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "invalid_code" in response.json()["detail"]


async def test_disconnect_slack_success(async_client: AsyncClient, mocker, insert_tenant):
    # Mock the necessary service methods
    mock_tenant_config = TenantConfig(
        tenant_id=1,
        cube_connection_config=CubeConnectionConfig(
            cube_api_url="http://test-cube-api.com",
            cube_auth_type="SECRET_KEY",
            cube_auth_secret_key="test-secret-key",
        ),
        slack_connection=SlackConnectionConfig(
            bot_token="xoxb-test-token",  # noqa
            bot_user_id="U123456",
            app_id="A123456",
            team={"id": "T123456", "name": "Test Team"},
            authed_user={"id": "U123456"},
        ),
    )
    mocker.patch("insights_backend.core.crud.TenantCRUD.get_tenant_config", return_value=mock_tenant_config)
    mocker.patch("insights_backend.services.slack_oauth.SlackOAuthService.revoke_oauth_token")
    mocker.patch(
        "insights_backend.core.crud.TenantCRUD.revoke_slack_connection",
        return_value=TenantConfig(
            tenant_id=1,
            cube_connection_config=CubeConnectionConfig(
                cube_api_url="http://test-cube-api.com",
                cube_auth_type="SECRET_KEY",
                cube_auth_secret_key="test-secret-key",
            ),
            slack_connection=None,
        ),
    )

    # Act
    response = await async_client.post("/v1/slack/disconnect")

    # Assert
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["slack_connection"] is None


async def test_disconnect_slack_no_connection(async_client: AsyncClient, mocker):
    # Mock tenant config with no Slack connection
    mocker.patch(
        "insights_backend.core.crud.TenantCRUD.get_tenant_config",
        return_value=TenantConfig(tenant_id=1, slack_connection=None),
    )

    # Act
    response = await async_client.post("/v1/slack/disconnect")

    # Assert
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert "Slack connection details not found" in response.json()["detail"]


async def test_list_channels(async_client: AsyncClient, mocker, insert_tenant):
    # Mock the SlackClient.list_channels method
    mock_channels = {
        "results": [
            {"name": "general", "id": "HADEDA", "is_channel": True},
            {"name": "random", "id": "HADEDS", "is_channel": True},
        ],
        "next_cursor": None,
    }
    mocker.patch("commons.clients.slack.SlackClient.list_channels", return_value=mock_channels)

    # Act
    response = await async_client.get("/v1/slack/channels")

    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data["results"]) == 2
    assert data["results"][0]["name"] == "general"
    assert data["next_cursor"] is None


async def test_list_channels_with_filters(async_client: AsyncClient, mocker, insert_tenant):
    # Mock the SlackClient.list_channels method
    mock_channels = {
        "results": [{"name": "test-channel", "id": "dasd", "is_channel": True}],
        "next_cursor": None,
    }
    mocker.patch("commons.clients.slack.SlackClient.list_channels", return_value=mock_channels)

    # Act
    response = await async_client.get("/v1/slack/channels", params={"name": "test", "limit": 50})

    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert len(data["results"]) == 1
    assert data["results"][0]["name"] == "test-channel"


async def test_get_channel_info(async_client: AsyncClient, mocker, insert_tenant):
    mock_channel = {"id": "HADEDA", "name": "general"}

    mocker.patch("commons.clients.slack.SlackClient.get_channel_info", return_value=mock_channel)

    # Act
    response = await async_client.get("/v1/slack/channels/HADEDA")

    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["id"] == "HADEDA"
    assert data["name"] == "general"


async def test_get_invalid_channel_info(async_client: AsyncClient, mocker, insert_tenant):
    channel_id = "NOCHANNEL"

    # Act
    response = await async_client.get(f"/v1/slack/channels/{channel_id}")

    # Assert
    assert response.status_code == status.HTTP_404_NOT_FOUND


async def test_list_tenants_with_stories_enabled_filter(mocker, insert_tenant, async_client: AsyncClient):
    # Mock the paginate method to return a predefined result
    mock_tenant = Tenant(
        id=2, external_id="test-external-id-2", name="test_tenant_2", identifier="test_tenant_2", domains=["test2.com"]
    )

    # Mock the paginate method in TenantCRUD
    mocker.patch("insights_backend.core.crud.TenantCRUD.paginate", return_value=([mock_tenant], 1))

    # Test filtering for stories enabled
    response = await async_client.get("/v1/tenants/all?enable_story_generation=True")

    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["count"] == 1
    assert len(data["results"]) == 1
    assert data["results"][0]["identifier"] == "test_tenant_2"


async def test_update_user_success(insert_tenant, db_user_json, async_client: AsyncClient):
    # Create User first
    response = await async_client.post("/v1/users/", json=db_user_json)
    user_id = response.json()["id"]

    # Update data
    update_data = {
        "name": "Updated Name",
        "email": "updated.email@example.com",
        "external_user_id": "new_ext_id",
        "profile_picture": "https://new-picture.com/pic.jpg",
    }

    # Act
    response = await async_client.put(f"/v1/users/{user_id}", json=update_data)

    # Assert
    assert response.status_code == 200
    updated_user = response.json()
    assert updated_user["name"] == update_data["name"]
    assert updated_user["email"] == update_data["email"]
    assert updated_user["external_user_id"] == update_data["external_user_id"]
    assert updated_user["profile_picture"] == update_data["profile_picture"]


async def test_update_user_email_exists(insert_tenant, db_user_json, async_client: AsyncClient):
    # Create first user
    response = await async_client.post("/v1/users/", json=db_user_json)

    # Create second user with different email
    second_user = db_user_json.copy()
    second_user["email"] = "second.user@example.com"
    response = await async_client.post("/v1/users/", json=second_user)
    second_user_id = response.json()["id"]

    # Try to update second user with first user's email
    update_data = {
        "name": "Updated Name",
        "email": db_user_json["email"],  # Using first user's email
        "external_user_id": "new_ext_id",
        "profile_picture": "https://new-picture.com/pic.jpg",
    }

    # Act
    response = await async_client.put(f"/v1/users/{second_user_id}", json=update_data)

    # Assert
    assert response.status_code == 400
    assert "Email already exists" in response.json()["detail"]


async def test_update_user_partial(insert_tenant, db_user_json, async_client: AsyncClient):
    # Create User first
    response = await async_client.post("/v1/users/", json=db_user_json)
    user_id = response.json()["id"]
    original_user = response.json()

    # Update only name
    update_data = {"name": "Updated Name", "email": original_user["email"]}  # Keep original email

    # Act
    response = await async_client.put(f"/v1/users/{user_id}", json=update_data)

    # Assert
    assert response.status_code == 200
    updated_user = response.json()
    assert updated_user["name"] == update_data["name"]
    assert updated_user["email"] == original_user["email"]
    assert updated_user["external_user_id"] == original_user["external_user_id"]
    assert updated_user["profile_picture"] == original_user["profile_picture"]


async def test_update_user_same_email(insert_tenant, db_user_json, async_client: AsyncClient):
    # Create User first
    response = await async_client.post("/v1/users/", json=db_user_json)
    user_id = response.json()["id"]

    # Update with same email but different name
    update_data = {"name": "Updated Name", "email": db_user_json["email"]}  # Same email

    # Act
    response = await async_client.put(f"/v1/users/{user_id}", json=update_data)

    # Assert
    assert response.status_code == 200
    updated_user = response.json()
    assert updated_user["name"] == update_data["name"]
    assert updated_user["email"] == db_user_json["email"]


# Snowflake Configuration Tests
async def test_create_snowflake_config(insert_tenant, snowflake_config_create_data, async_client: AsyncClient):
    """Test creating a new Snowflake configuration"""
    response = await async_client.post("/v1/tenant/snowflake-config", json=snowflake_config_create_data)

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["account_identifier"] == snowflake_config_create_data["account_identifier"]
    assert data["username"] == snowflake_config_create_data["username"]
    assert data["warehouse"] == snowflake_config_create_data["warehouse"]
    assert data["role"] == snowflake_config_create_data["role"]
    assert data["database"] == snowflake_config_create_data["database"]
    assert data["db_schema"] == snowflake_config_create_data["db_schema"]
    assert data["auth_method"] == snowflake_config_create_data["auth_method"]
    assert "password" not in data  # Sensitive data should not be returned
    assert "tenant_id" in data
    assert "id" in data


async def test_create_snowflake_config_key_auth(
    insert_tenant, snowflake_config_key_auth_data, async_client: AsyncClient
):
    """Test creating Snowflake config with private key authentication"""
    response = await async_client.post("/v1/tenant/snowflake-config", json=snowflake_config_key_auth_data)

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["auth_method"] == "PRIVATE_KEY"
    assert "private_key" not in data  # Sensitive data should not be returned
    assert "private_key_passphrase" not in data


async def test_create_snowflake_config_validation_error(insert_tenant, async_client: AsyncClient):
    """Test creating Snowflake config with validation errors"""
    # Missing password for password auth
    invalid_data = {
        "account_identifier": "test-account",
        "username": "test_user",
        "database": "TEST_DB",
        "db_schema": "TEST_SCHEMA",
        "auth_method": "PASSWORD",
        # password is missing
    }

    response = await async_client.post("/v1/tenant/snowflake-config", json=invalid_data)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


async def test_create_snowflake_config_missing_private_key(insert_tenant, async_client: AsyncClient):
    """Test creating Snowflake config with missing private key"""
    invalid_data = {
        "account_identifier": "test-account",
        "username": "test_user",
        "database": "TEST_DB",
        "db_schema": "TEST_SCHEMA",
        "auth_method": "PRIVATE_KEY",
        # private_key is missing
    }

    response = await async_client.post("/v1/tenant/snowflake-config", json=invalid_data)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


async def test_get_snowflake_config(insert_tenant, insert_snowflake_config, async_client: AsyncClient):
    """Test retrieving Snowflake configuration"""
    response = await async_client.get("/v1/tenant/snowflake-config")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["account_identifier"] == insert_snowflake_config.account_identifier
    assert data["username"] == insert_snowflake_config.username
    assert data["warehouse"] == insert_snowflake_config.warehouse
    assert data["role"] == insert_snowflake_config.role
    assert data["database"] == insert_snowflake_config.database
    assert data["db_schema"] == insert_snowflake_config.db_schema
    assert data["auth_method"] == insert_snowflake_config.auth_method.value
    assert "password" not in data  # Sensitive data should not be returned
    assert data["id"] == insert_snowflake_config.id
    assert data["tenant_id"] == insert_snowflake_config.tenant_id


async def test_get_snowflake_config_not_found(insert_tenant, async_client: AsyncClient):
    """Test retrieving non-existent Snowflake configuration"""
    response = await async_client.get("/v1/tenant/snowflake-config")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert "Snowflake configuration not found" in response.json()["detail"]


async def test_get_snowflake_config_internal(insert_tenant, insert_snowflake_config, async_client: AsyncClient):
    """Test retrieving internal Snowflake configuration with sensitive data"""
    response = await async_client.get("/v1/tenant/snowflake-config/internal")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["account_identifier"] == insert_snowflake_config.account_identifier
    assert data["password"] == insert_snowflake_config.password  # Should include sensitive data


async def test_update_snowflake_config(insert_tenant, insert_snowflake_config, async_client: AsyncClient):
    """Test updating Snowflake configuration"""
    update_data = {
        "account_identifier": "updated-account.us-west-2",
        "username": "updated_user",
        "warehouse": "UPDATED_WH",
        "database": "UPDATED_DB",
    }

    response = await async_client.put("/v1/tenant/snowflake-config", json=update_data)

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["account_identifier"] == update_data["account_identifier"]
    assert data["username"] == update_data["username"]
    assert data["warehouse"] == update_data["warehouse"]
    assert data["database"] == update_data["database"]
    # Fields not in update should remain unchanged
    assert data["role"] == insert_snowflake_config.role
    assert data["db_schema"] == insert_snowflake_config.db_schema


async def test_update_snowflake_config_not_found(insert_tenant, async_client: AsyncClient):
    """Test updating non-existent Snowflake configuration"""
    update_data = {"username": "new_user"}

    response = await async_client.put("/v1/tenant/snowflake-config", json=update_data)

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert "Tenant or Snowflake configuration not found" in response.json()["detail"]


async def test_update_snowflake_config_partial(insert_tenant, insert_snowflake_config, async_client: AsyncClient):
    """Test partial update of Snowflake configuration"""
    update_data = {"username": "partially_updated_user"}

    response = await async_client.put("/v1/tenant/snowflake-config", json=update_data)

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["username"] == update_data["username"]
    # Other fields should remain unchanged
    assert data["account_identifier"] == insert_snowflake_config.account_identifier
    assert data["warehouse"] == insert_snowflake_config.warehouse


async def test_delete_snowflake_config(insert_tenant, insert_snowflake_config, async_client: AsyncClient):
    """Test deleting Snowflake configuration"""
    response = await async_client.delete("/v1/tenant/snowflake-config")

    assert response.status_code == status.HTTP_200_OK
    assert "Snowflake configuration deleted successfully" in response.json()["detail"]

    # Verify config is actually deleted
    get_response = await async_client.get("/v1/tenant/snowflake-config")
    assert get_response.status_code == status.HTTP_404_NOT_FOUND


async def test_delete_snowflake_config_not_found(insert_tenant, async_client: AsyncClient):
    """Test deleting non-existent Snowflake configuration"""
    response = await async_client.delete("/v1/tenant/snowflake-config")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert "Snowflake configuration not found" in response.json()["detail"]


async def test_test_snowflake_connection_success(
    insert_tenant, snowflake_config_create_data, async_client: AsyncClient, mocker
):
    """Test successful Snowflake connection test"""
    # Mock the connection test to return success
    mock_test_result = {
        "success": True,
        "message": "Connection successful",
        "connection_details": {
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
            "db_schema": "TEST_SCHEMA",
            "role": "TEST_ROLE",
        },
    }
    mocker.patch("insights_backend.core.crud.TenantCRUD.test_snowflake_connection", return_value=mock_test_result)

    response = await async_client.post("/v1/tenant/snowflake-config/test", json=snowflake_config_create_data)

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["success"] is True
    assert data["message"] == "Connection successful"
    assert "connection_details" in data


async def test_test_snowflake_connection_failure(
    insert_tenant, snowflake_config_create_data, async_client: AsyncClient, mocker
):
    """Test failed Snowflake connection test"""
    # Mock the connection test to return failure
    mock_test_result = {
        "success": False,
        "message": "Connection failed: Invalid credentials",
        "connection_details": None,
    }
    mocker.patch("insights_backend.core.crud.TenantCRUD.test_snowflake_connection", return_value=mock_test_result)

    response = await async_client.post("/v1/tenant/snowflake-config/test", json=snowflake_config_create_data)

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["success"] is False
    assert "Connection failed" in data["message"]
    assert data["connection_details"] is None


async def test_test_snowflake_connection_invalid_data(insert_tenant, async_client: AsyncClient):
    """Test Snowflake connection test with invalid data"""
    invalid_data = {
        "account_identifier": "test-account",
        "username": "test_user",
        "auth_method": "PASSWORD",
        # Missing required fields like database, db_schema
    }

    response = await async_client.post("/v1/tenant/snowflake-config/test", json=invalid_data)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


async def test_enable_metric_cache(insert_tenant, async_client: AsyncClient):
    """Test enabling metric cache"""
    response = await async_client.put("/v1/tenant/metric-cache/enable?enabled=true")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["enable_metric_cache"] is True


async def test_disable_metric_cache(insert_tenant, async_client: AsyncClient):
    """Test disabling metric cache"""
    response = await async_client.put("/v1/tenant/metric-cache/enable?enabled=false")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["enable_metric_cache"] is False


async def test_enable_metric_cache_default_true(insert_tenant, async_client: AsyncClient):
    """Test enabling metric cache with default parameter"""
    response = await async_client.put("/v1/tenant/metric-cache/enable")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["enable_metric_cache"] is True


async def test_enable_metric_cache_tenant_not_found(mocker, async_client: AsyncClient):
    """Test enabling metric cache when tenant config not found"""
    mocker.patch(
        "insights_backend.core.crud.TenantCRUD.enable_metric_cache", side_effect=NotFoundError("Tenant not found")
    )

    response = await async_client.put("/v1/tenant/metric-cache/enable?enabled=true")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert "Tenant or Snowflake configuration not found" in response.json()["detail"]


async def test_create_snowflake_config_duplicate(
    insert_tenant, insert_snowflake_config, snowflake_config_create_data, async_client: AsyncClient
):
    """Test creating Snowflake config when one already exists"""
    # Try to create another config when one already exists
    response = await async_client.post("/v1/tenant/snowflake-config", json=snowflake_config_create_data)

    # Should fail due to unique constraint on tenant_id
    assert response.status_code == status.HTTP_409_CONFLICT
    assert "Snowflake configuration already exists for this tenant" in response.json()["detail"]


async def test_snowflake_config_auth_method_validation(insert_tenant, async_client: AsyncClient):
    """Test Snowflake config creation with invalid auth method"""
    invalid_data = {
        "account_identifier": "test-account",
        "username": "test_user",
        "database": "TEST_DB",
        "db_schema": "TEST_SCHEMA",
        "auth_method": "INVALID_METHOD",
        "password": "test_password",
    }

    response = await async_client.post("/v1/tenant/snowflake-config", json=invalid_data)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


async def test_snowflake_config_required_fields_validation(insert_tenant, async_client: AsyncClient):
    """Test Snowflake config creation with missing required fields"""
    incomplete_data = {
        "account_identifier": "test-account",
        "username": "test_user",
        # Missing database, db_schema, auth_method
    }

    response = await async_client.post("/v1/tenant/snowflake-config", json=incomplete_data)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


async def test_update_snowflake_config_auth_method_change(
    insert_tenant, insert_snowflake_config, async_client: AsyncClient
):
    """Test updating Snowflake config to change authentication method"""
    update_data = {
        "auth_method": "PRIVATE_KEY",
        "private_key": "-----BEGIN PRIVATE KEY-----\nnew_private_key\n-----END PRIVATE KEY-----",
        "password": None,  # Clear password when switching to private key
    }

    response = await async_client.put("/v1/tenant/snowflake-config", json=update_data)

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["auth_method"] == "PRIVATE_KEY"


# Add test for new tenant details route added in the diff


async def test_get_tenant_details_success(async_client: AsyncClient, insert_tenant, db_session: AsyncSession):
    """Test successful retrieval of tenant details."""
    # Act
    response = await async_client.get("/v1/tenant/details")

    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert "id" in data
    assert "name" in data
    assert "identifier" in data
    assert "domains" in data
    assert data["id"] == insert_tenant.id
    assert data["name"] == insert_tenant.name
    assert data["identifier"] == insert_tenant.identifier
    assert data["domains"] == insert_tenant.domains


async def test_get_tenant_details_not_found(mocker, async_client: AsyncClient, db_session: AsyncSession):
    """Test tenant details retrieval when tenant not found."""
    # Mock tenant CRUD to raise NotFoundError (using correct method name)
    mocker.patch("insights_backend.core.crud.TenantCRUD.get", side_effect=NotFoundError("Tenant not found"))

    # Act
    response = await async_client.get("/v1/tenant/details")

    # Assert
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert "Tenant not found" in response.json()["detail"]


async def test_get_tenant_details_with_minimal_data(
    async_client: AsyncClient, jwt_payload: dict, db_session: AsyncSession
):
    """Test tenant details with minimal required data."""
    # Create tenant with minimal data
    from commons.utilities.context import set_tenant_id
    from insights_backend.core.models.tenant import Tenant

    set_tenant_id(jwt_payload["tenant_id"])

    # Create minimal tenant
    minimal_tenant = Tenant(
        id=jwt_payload["tenant_id"],
        external_id=jwt_payload["external_id"],
        name="Minimal Tenant",
        identifier="minimal_tenant",
        domains=[],  # Empty domains
    )
    db_session.add(minimal_tenant)
    await db_session.flush()

    # Act
    response = await async_client.get("/v1/tenant/details")

    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["id"] == jwt_payload["tenant_id"]
    assert data["name"] == "Minimal Tenant"
    assert data["identifier"] == "minimal_tenant"
    assert data["domains"] == []


async def test_get_tenant_details_context_missing(mocker, async_client: AsyncClient):
    """Test tenant details when tenant context is missing."""
    # Mock get_tenant_id to return None
    mocker.patch("commons.utilities.context.get_tenant_id", return_value=None)

    # Act
    response = await async_client.get("/v1/tenant/details")

    # Assert - Should return 500 or appropriate error for missing context
    assert response.status_code >= 400


# Add test for new enable_metric_cache functionality that was enhanced in the diff


async def test_enable_metric_cache_success(insert_tenant, async_client: AsyncClient):
    """Test successfully enabling metric cache."""
    response = await async_client.put("/v1/tenant/metric-cache/enable?enabled=true")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["enable_metric_cache"] is True


async def test_disable_metric_cache_success(insert_tenant, async_client: AsyncClient):
    """Test successfully disabling metric cache."""
    response = await async_client.put("/v1/tenant/metric-cache/enable?enabled=false")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["enable_metric_cache"] is False


async def test_enable_metric_cache_invalid_parameter(insert_tenant, async_client: AsyncClient):
    """Test enabling metric cache with invalid parameter."""
    response = await async_client.put("/v1/tenant/metric-cache/enable?enabled=invalid")

    # Should return validation error
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


# Test the enhanced snowflake config functionality


async def test_create_snowflake_config_duplicate_enhanced(
    insert_tenant, insert_snowflake_config, snowflake_config_create_data, async_client: AsyncClient
):
    """Test creating Snowflake config when one already exists (enhanced error handling)."""
    # Try to create another config when one already exists
    response = await async_client.post("/v1/tenant/snowflake-config", json=snowflake_config_create_data)

    # Should fail due to unique constraint on tenant_id
    assert response.status_code == status.HTTP_409_CONFLICT
    assert "Snowflake configuration already exists for this tenant" in response.json()["detail"]


async def test_snowflake_config_private_key_validation(insert_tenant, async_client: AsyncClient):
    """Test Snowflake config validation for private key authentication."""
    invalid_data = {
        "account_identifier": "test-account",
        "username": "test_user",
        "database": "TEST_DB",
        "db_schema": "TEST_SCHEMA",
        "auth_method": "PRIVATE_KEY",
        # Missing private_key field
    }

    response = await async_client.post("/v1/tenant/snowflake-config", json=invalid_data)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


async def test_snowflake_config_password_validation(insert_tenant, async_client: AsyncClient):
    """Test Snowflake config validation for password authentication."""
    invalid_data = {
        "account_identifier": "test-account",
        "username": "test_user",
        "database": "TEST_DB",
        "db_schema": "TEST_SCHEMA",
        "auth_method": "PASSWORD",
        # Missing password field
    }

    response = await async_client.post("/v1/tenant/snowflake-config", json=invalid_data)
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


async def test_test_snowflake_connection_comprehensive(
    insert_tenant, snowflake_config_create_data, async_client: AsyncClient, mocker
):
    """Test comprehensive Snowflake connection testing."""
    # Mock successful connection test (matching actual SnowflakeClient response format)
    mock_test_result = {
        "success": True,
        "message": "Connection successful",
        "connection_details": {
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
            "schema": "TEST_SCHEMA",
            "role": "TEST_ROLE",
            "account": "test-account.us-east-1",
        },
        "version": "SNOWFLAKE 7.0.0",
    }
    # Mock the async method properly
    mock_test_connection = AsyncMock(return_value=mock_test_result)
    mocker.patch("insights_backend.core.crud.TenantCRUD.test_snowflake_connection", mock_test_connection)

    response = await async_client.post("/v1/tenant/snowflake-config/test", json=snowflake_config_create_data)

    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["success"] is True
    assert data["message"] == "Connection successful"
    assert "connection_details" in data
    # Version field should be present if mock is working correctly
    if "version" in data:
        assert data["version"] == "SNOWFLAKE 7.0.0"
