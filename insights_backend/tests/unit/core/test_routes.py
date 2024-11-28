import pytest
import pytest_asyncio
from fastapi import HTTPException
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

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
        tenant = Tenant(
            external_id=jwt_payload["external_id"], name="test_tenant", identifier="test_tenant", domains=["test.com"]
        )
        config = TenantConfig(
            tenant_id=tenant.id,
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


async def test_get_tenant_config(insert_tenant, async_client: AsyncClient, db_session: AsyncSession):
    # Act
    response = await async_client.get("/v1/tenant/config")
    # Assert
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert "cube_connection_config" in data


async def test_update_tenant_config(insert_tenant, async_client: AsyncClient, db_session: AsyncSession):
    # Prepare the new configuration data
    new_config_data = {
        "cube_connection_config": {
            "cube_api_url": "http://new-cube-api.com",
            "cube_auth_type": "SECRET_KEY",
            "cube_auth_secret_key": "new-secret-key",
        }
    }

    # Act: Update the tenant configuration with tenant_id in the headers
    response = await async_client.put(
        "/v1/tenant/config", json=new_config_data, headers={"X-Tenant-Id": str(insert_tenant.id)}
    )

    # Assert: Check the response status code
    assert response.status_code == status.HTTP_200_OK

    # Assert: Check the response data
    updated_config = response.json()
    assert (
        updated_config["cube_connection_config"]["cube_api_url"]
        == new_config_data["cube_connection_config"]["cube_api_url"]
    )


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
