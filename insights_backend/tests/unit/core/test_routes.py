import pytest
import pytest_asyncio
from fastapi import HTTPException
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from commons.db.crud import NotFoundError
from commons.models.tenant import SlackConnectionConfig
from commons.utilities.context import set_tenant_id
from insights_backend.core.models.tenant import Tenant, TenantConfig

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
        config = TenantConfig(
            tenant_id=tenant.id,  # type: ignore
            cube_connection_config={
                "cube_api_url": "http://test-cube-api.com",
                "cube_auth_type": "SECRET_KEY",
                "cube_auth_secret_key": "test-secret-key",
            },
            slack_connection=dict(
                bot_token="xoxb-test-token",  # noqa
                bot_user_id="U123456",
                app_id="A123456",
                team={"id": "T123456", "name": "Test Team"},
                authed_user={"id": "U123456"},
            ),
        )
        db_session.add(tenant)
        db_session.add(config)
        await db_session.flush()
    return tenant


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
        cube_connection_config={
            "cube_api_url": "http://test-cube-api.com",
            "cube_auth_type": "SECRET_KEY",
            "cube_auth_secret_key": "test-secret-key",
        },
        slack_connection=dict(
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
            cube_connection_config={
                "cube_api_url": "http://test-cube-api.com",
                "cube_auth_type": "SECRET_KEY",
                "cube_auth_secret_key": "test-secret-key",
            },
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
        "insights_backend.core.crud.TenantCRUD.get_tenant_config", return_value=TenantConfig(slack_connection=None)
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
