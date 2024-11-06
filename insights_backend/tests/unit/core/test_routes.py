import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status

from commons.utilities.context import set_tenant_id
from insights_backend.core.models.tenant import Tenant, TenantConfig

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(name="insert_tenant")
async def insert_tenant_fixture(db_session: AsyncSession, jwt_payload: dict):
    set_tenant_id(jwt_payload["tenant_id"])
    tenant = await db_session.execute(select(Tenant).filter_by(external_id=jwt_payload["external_id"]))
    tenant = tenant.scalar_one_or_none()
    if not tenant:
        tenant = Tenant(
            external_id=jwt_payload["external_id"], name="test_tenant", identifier="test_tenant", domains=["test.com"]
        )
        config = TenantConfig(
            tenant_id=tenant.id,
            cube_connection_config={
                "cube_connection_config": {
                    "cube_api_url": "http://test-cube-api.com",
                    "cube_auth_type": "SECRET_KEY",
                    "cube_auth_secret_key": "test-secret-key",
                }
            },
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
