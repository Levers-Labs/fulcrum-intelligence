import pytest
from starlette import status


@pytest.mark.asyncio
async def test_create_user(db_session, client):
    user_json = {
        "name": "test_name",
        "email": "test_email@test.com",
        "provider": "google",
        "external_user_id": "auth0|001123",
        "profile_picture": "http://test.com",
    }
    response = client.post("/v1/users/", json=user_json)
    assert response.status_code == status.HTTP_200_OK


def test_get_user(db_session, client, db_user):
    response = client.get("/v1/users/1")
    user = response.json()
    assert response.status_code == status.HTTP_200_OK
    del user["created_at"]
    del user["updated_at"]
    assert user == db_user


def test_get_user_by_email(db_session, client, db_user):
    response = client.get("/v1/users/user-by-email/test_email@test.com")
    assert response.status_code == status.HTTP_200_OK


def test_update_user(db_session, client):
    user_json = {
        "name": "test_name",
        "email": "test_email@test.com",
        "provider": "email",
        "external_user_id": "auth0|001123",
        "profile_picture": "http://test-some-url.com",
    }
    response = client.put("/v1/users/1", json=user_json)
    assert response.status_code == status.HTTP_200_OK
    response = response.json()
    assert response["provider"] == "email"
    assert response["profile_picture"] == "http://test-some-url.com"


def test_list_user(client, db_user):
    response = client.get("/v1/users/")
    users = response.json()

    assert response.status_code == status.HTTP_200_OK
    assert users["count"] == 1
