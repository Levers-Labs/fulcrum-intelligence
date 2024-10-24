from datetime import datetime, timedelta
from unittest import mock

import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette import status

from commons.auth.auth import OAuth2User, UserType
from insights_backend.core.routes import user_router


class TestUserRoutes:
    @pytest.fixture(autouse=True)
    def setup(self):
        app = FastAPI()
        app.include_router(user_router, prefix="/v1")
        self.client = TestClient(app)

    @pytest.fixture
    def mock_token(self):
        # Create a mock JWT token
        payload = {
            "sub": "PN0CtJASlMDm9TEivb3izsDnIf5dcFYA@clients",
            "permissions": ["user:write", "user:read"],
            "iat": datetime.utcnow(),
            "exp": datetime.utcnow() + timedelta(hours=1),
            "tenant_id": 1,  # Include tenant_id if needed
        }
        token = jwt.encode(payload, "secret", algorithm="HS256")  # Use a secret key
        return token

    @pytest.fixture
    def auth_headers(self, mock_token):
        return {"Authorization": f"Bearer {mock_token}"}

    @pytest.fixture
    def db_user_json(self):
        return {
            "name": "test_name",
            "email": "test_email@test.com",
            "provider": "google",
            "tenant_org_id": "test_org_id",
            "external_user_id": "auth0|001123",
            "profile_picture": "http://test.com",
        }

    @pytest.mark.asyncio
    async def test_create_user(self, db_user_json, auth_headers):
        response = self.client.post("/v1/users/", json=db_user_json, headers=auth_headers)

        assert response.status_code == 200  # Use 200 instead of status.HTTP_200_OK
        db_user = response.json()
        assert "id" in db_user

    def test_get_user(self, db_user_json, auth_headers):
        # creating a user
        response = self.client.post("/v1/users/", json=db_user_json, headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        db_user = response.json()
        assert "id" in db_user

        response = self.client.get(f"/v1/users/{db_user['id']}", headers=auth_headers)
        user = response.json()
        assert response.status_code == status.HTTP_200_OK
        assert user["id"] == db_user["id"]

        response = self.client.get("/v1/users/-1", headers=auth_headers)
        assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_get_user_by_email(self, db_user_json, auth_headers):
        # creating a user
        response = self.client.post("/v1/users/", json=db_user_json, headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        db_user = response.json()

        response = self.client.get(f"/v1/users/user-by-email/{db_user_json['email']}", headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK

        response = self.client.get("/v1/users/user-by-email/test_email12@test.com", headers=auth_headers)
        assert response.status_code == status.HTTP_404_NOT_FOUND

        self.client.delete(f"/v1/users/{db_user['id']}", headers=auth_headers)

    def test_update_user(self, db_user_json, auth_headers):
        with mock.patch("insights_backend.core.dependencies.oauth2_auth") as mock_auth:
            mock_verify = mock.AsyncMock()
            mock_verify.return_value = OAuth2User(
                external_id="PN0CtJASlMDm9TEivb3izsDnIf5dcFYA@clients",
                type=UserType.MACHINE,
                permissions=["user:write", "user:read", "story:*", "query:*", "analysis:*", "tenant:read"],
                app_user_id=None,
            )
            mock_auth.return_value.verify = mock_verify

            response = self.client.post("/v1/users/", json=db_user_json, headers=auth_headers)
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
            response = self.client.put(f"/v1/users/{db_user['id']}", json=user_json, headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            response = response.json()
            assert response["provider"] == "email"
            assert response["profile_picture"] == "http://test-some-url.com"

            response = self.client.put("/v1/users/-4", json=user_json, headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

            self.client.delete(f"/v1/users/{db_user['id']}", headers=auth_headers)

    def test_list_user(self, auth_headers):
        response = self.client.get("/v1/users/", headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
