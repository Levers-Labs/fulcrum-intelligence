from unittest import mock

import pytest
from starlette import status

from commons.utilities.context import set_tenant_id


class MockSecurity:
    def __init__(self, *args, **kwargs):
        self.dependency = lambda: True
        self.use_cache = False


mock.patch("fastapi.Security", MockSecurity).start()
set_tenant_id("1")


@pytest.mark.asyncio
async def test_create_user(client, db_user_json):
    response = client.post("/v1/users/", json=db_user_json)
    db_user = response.json()
    assert response.status_code == status.HTTP_200_OK

    response = client.post("/v1/users/", json=db_user_json)
    assert response.status_code == status.HTTP_400_BAD_REQUEST

    client.delete(f"/v1/users/{db_user['id']}")


def test_get_user(db_user_json, client):
    # creating a user
    response = client.post("/v1/users/", json=db_user_json)
    db_user = response.json()

    response = client.get(f"/v1/users/{db_user['id']}")
    user = response.json()
    assert response.status_code == status.HTTP_200_OK
    assert user == db_user

    response = client.get("/v1/users/-1")
    assert response.status_code == status.HTTP_404_NOT_FOUND

    client.delete(f"/v1/users/{db_user['id']}")


def test_get_user_by_email(db_user_json, client):
    # creating a user
    response = client.post("/v1/users/", json=db_user_json)
    db_user = response.json()

    response = client.get("/v1/users/user-by-email/test_email@test.com")
    assert response.status_code == status.HTTP_200_OK

    response = client.get("/v1/users/user-by-email/test_email12@test.com")
    assert response.status_code == status.HTTP_404_NOT_FOUND

    client.delete(f"/v1/users/{db_user['id']}")


def test_update_user(db_user_json, client):
    response = client.post("/v1/users/", json=db_user_json)
    db_user = response.json()

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
    assert response["provider"] == "email"
    assert response["profile_picture"] == "http://test-some-url.com"

    response = client.put("/v1/users/-4", json=user_json)
    assert response.status_code == status.HTTP_404_NOT_FOUND

    client.delete(f"/v1/users/{db_user['id']}")


def test_list_user(client):
    response = client.get("/v1/users/")
    assert response.status_code == status.HTTP_200_OK


def test_get_tenant(client):
    response = client.get("/v1/tenants/me")
    assert response.status_code == status.HTTP_404_NOT_FOUND


def test_get_tenant_config(client):
    response = client.get("/v1/tenants/me/config")
    assert response.status_code == status.HTTP_404_NOT_FOUND
