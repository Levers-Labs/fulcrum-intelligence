from unittest.mock import AsyncMock, MagicMock, Mock

import jwt
import pytest
from fastapi.security import HTTPAuthorizationCredentials, SecurityScopes

from commons.auth.auth import (
    Oauth2Auth,
    OAuth2User,
    UnauthorizedException,
    UserType,
)


@pytest.fixture
def oauth2_auth():
    return Oauth2Auth(api_audience="audience", issuer="issuer")


@pytest.mark.asyncio
async def test_verify_with_valid_token_and_scopes(oauth2_auth, monkeypatch):
    token_credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="valid_token")
    security_scopes = SecurityScopes(scopes=["read"])

    # Mock the return values of verify_jwt and get_oauth_user
    def mock_verify_jwt(token):
        return {"sub": "userId", "permissions": ["read"]}

    async def mock_get_oauth_user(payload, token):
        return OAuth2User(
            external_id="user_id",
            type=UserType.APP,
            permissions=["read"],
            app_user={"id": "user_id", "name": "John Doe"},
        )

    # Replace the original methods with AsyncMock
    monkeypatch.setattr(oauth2_auth, "verify_jwt", Mock(side_effect=mock_verify_jwt))
    monkeypatch.setattr(oauth2_auth, "get_oauth_user", AsyncMock(side_effect=mock_get_oauth_user))

    # Call the method under test
    result = await oauth2_auth.verify(security_scopes, token_credentials)

    # Assert the returned OAuth2User object
    assert isinstance(result, OAuth2User)


def test_verify_jwt(oauth2_auth):
    token = "valid_token"  # noqa
    payload = {"sub": "user_id", "permissions": ["read"]}

    # Mocking jwks_client behavior
    oauth2_auth.jwks_client = MagicMock()
    oauth2_auth.jwks_client.get_signing_key_from_jwt = MagicMock(return_value=MagicMock(key="test_key"))

    jwt.decode = MagicMock(return_value=payload)

    result = oauth2_auth.verify_jwt(token)
    assert result == payload


def test_verify_token_claims_present_claim(oauth2_auth):
    payload = {"permissions": ["read", "write"], "user": "123"}
    claim_name = "permissions"
    expected_value = ["read"]

    oauth2_auth._verify_token_claims(payload, claim_name, expected_value)


def test_verify_token_claims_missing_claim(oauth2_auth):
    payload = {"user": "123"}
    claim_name = "permissions"
    expected_value = ["read"]

    with pytest.raises(UnauthorizedException):
        oauth2_auth._verify_token_claims(payload, claim_name, expected_value)


def test_verify_token_claims_missing_value(oauth2_auth):
    payload = {"permissions": ["write"], "user": "123"}
    claim_name = "permissions"
    expected_value = ["read"]

    with pytest.raises(UnauthorizedException):
        oauth2_auth._verify_token_claims(payload, claim_name, expected_value)
