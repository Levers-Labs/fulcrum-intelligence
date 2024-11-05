from unittest import mock

import pytest
from starlette import status

from commons.utilities.context import set_tenant_id


@pytest.fixture
def db_user_json():
    return {
        "name": "test_name",
        "email": "test_email@test.com",
        "provider": "google",
        "tenant_org_id": "test_org_id",
        "external_user_id": "auth0|001123",
        "profile_picture": "http://test.com",
    }


@pytest.mark.asyncio
@mock.patch("insights_backend.core.routes.handle_tenant_context_from_org_id")
async def test_create_user(mock_handle_tenant_context, client, db_user_json):
    mock_handle_tenant_context.return_value = None
    # Setting up tenant id in context
    set_tenant_id(1)

    response = client.post("/v1/users/", json=db_user_json)
    assert response.status_code == 200
    db_user = response.json()
    assert "id" in db_user
    mock_handle_tenant_context.assert_called_once_with(db_user_json["tenant_org_id"], mock.ANY)


@pytest.mark.asyncio
@mock.patch("insights_backend.core.routes.handle_tenant_context_from_org_id")
async def test_get_user(mock_handle_tenant_context, client, db_user_json):
    mock_handle_tenant_context.return_value = None
    # Setting up tenant id in context
    set_tenant_id(1)
    # creating a user
    response = client.post("/v1/users/", json=db_user_json)
    assert response.status_code == status.HTTP_200_OK
    db_user = response.json()
    assert "id" in db_user

    response = client.get(f"/v1/users/{db_user['id']}")
    user = response.json()
    assert response.status_code == status.HTTP_200_OK
    assert user["id"] == db_user["id"]

    response = client.get("/v1/users/-1")
    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
@mock.patch("insights_backend.core.routes.handle_tenant_context_from_org_id")
async def test_get_user_by_email(mock_handle_tenant_context, client, db_user_json):
    mock_handle_tenant_context.return_value = None
    # Setting up tenant id in context
    set_tenant_id(1)
    # creating a user
    response = client.post("/v1/users/", json=db_user_json)
    assert response.status_code == status.HTTP_200_OK
    db_user = response.json()

    response = client.get(f"/v1/users/user-by-email/{db_user_json['email']}")
    assert response.status_code == status.HTTP_200_OK

    response = client.get("/v1/users/user-by-email/test_email12@test.com")
    assert response.status_code == status.HTTP_404_NOT_FOUND

    client.delete(f"/v1/users/{db_user['id']}")


@pytest.mark.asyncio
@mock.patch("insights_backend.core.routes.handle_tenant_context_from_org_id")
async def test_update_user(mock_handle_tenant_context, client, db_user_json, mocker):
    mock_handle_tenant_context.return_value = None
    # Setting up tenant id in context
    set_tenant_id(1)

    response = client.post("/v1/users/", json=db_user_json)
    assert response.status_code == status.HTTP_200_OK
    db_user = response.json()
    assert "id" in db_user

    user_json = {
        "name": "test_name",
        "email": "test_email@test.com",
        "provider": "email",
        "external_user_id": "auth0|001123",
        "profile_picture": "http://test-some-url.com",
    }
    response = client.put(f"/v1/users/{db_user['id']}", json=user_json)
    assert response.status_code == status.HTTP_200_OK
    response = response.json()
    assert response["profile_picture"] == "http://test-some-url.com"

    response = client.put("/v1/users/-4", json=user_json)
    assert response.status_code == status.HTTP_404_NOT_FOUND

    client.delete(f"/v1/users/{db_user['id']}")
