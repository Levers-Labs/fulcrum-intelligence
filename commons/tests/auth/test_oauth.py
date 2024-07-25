# from unittest.mock import AsyncMock, MagicMock, Mock
#
# import httpx
# import jwt
# import pytest
# from fastapi.security import HTTPAuthorizationCredentials, SecurityScopes
# from httpx import Response
#
# from commons.auth.auth import (
#     Oauth2Auth,
#     OAuth2User,
#     UnauthorizedException,
#     UserType,
# )
#
#
# @pytest.fixture
# def oauth2_auth():
#     return Oauth2Auth(api_audience="audience", issuer="issuer", insights_backend_host="http://backend")
#
#
# @pytest.mark.asyncio
# async def test_verify_with_valid_token_and_scopes(oauth2_auth, monkeypatch):
#     token_credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="valid_token")
#     security_scopes = SecurityScopes(scopes=["read"])
#
#     # Mock the return values of verify_jwt and get_oauth_user
#     def mock_verify_jwt(token):
#         return {"sub": "user_id", "scope": ["read"]}
#
#     async def mock_get_oauth_user(payload, token):
#         return OAuth2User(
#             external_id="user_id",
#             type=UserType.APP,
#             permissions=["read"],
#             app_user={"id": "user_id", "name": "John Doe"},
#         )
#
#     # Replace the original methods with AsyncMock
#     monkeypatch.setattr(oauth2_auth, "verify_jwt", Mock(side_effect=mock_verify_jwt))
#     monkeypatch.setattr(oauth2_auth, "get_oauth_user", AsyncMock(side_effect=mock_get_oauth_user))
#
#     # Call the method under test
#     result = await oauth2_auth.verify(security_scopes, token_credentials)
#
#     # Assert the returned OAuth2User object
#     assert isinstance(result, OAuth2User)
#
#
# def test_verify_jwt(oauth2_auth):
#     token = "valid_token"  # noqa
#     payload = {"sub": "user_id", "scope": ["read"]}
#
#     # Mocking jwks_client behavior
#     oauth2_auth.jwks_client = MagicMock()
#     oauth2_auth.jwks_client.get_signing_key_from_jwt = MagicMock(return_value=MagicMock(key="test_key"))
#
#     jwt.decode = MagicMock(return_value=payload)
#
#     result = oauth2_auth.verify_jwt(token)
#     assert result == payload
#
#
# def test_verify_token_claims_present_claim(oauth2_auth):
#     payload = {"scope": ["read", "write"], "user_id": "123"}
#     claim_name = "scope"
#     expected_value = ["read"]
#
#     oauth2_auth._verify_token_claims(payload, claim_name, expected_value)
#
#
# def test_verify_token_claims_missing_claim(oauth2_auth):
#     payload = {"user_id": "123"}
#     claim_name = "scope"
#     expected_value = ["read"]
#
#     with pytest.raises(UnauthorizedException):
#         oauth2_auth._verify_token_claims(payload, claim_name, expected_value)
#
#
# def test_verify_token_claims_missing_value(oauth2_auth):
#     payload = {"scope": ["write"], "user_id": "123"}
#     claim_name = "scope"
#     expected_value = ["read"]
#
#     with pytest.raises(UnauthorizedException):
#         oauth2_auth._verify_token_claims(payload, claim_name, expected_value)
#
#
# @pytest.mark.asyncio
# async def test_get_oauth_user(oauth2_auth):
#     machine_user_payload = {"sub": "some_id@clients"}
#     result = await oauth2_auth.get_oauth_user(machine_user_payload, "some_token")
#     assert result == OAuth2User(
#         external_id=machine_user_payload["sub"],
#         type=UserType.MACHINE,
#         permissions=[],
#     )
#
#     app_user_payload = {"sub": "some_id", "user_id": 1}
#     oauth2_auth.get_app_user = AsyncMock(side_effect=lambda a, b: app_user_payload)
#
#     result = await oauth2_auth.get_oauth_user(app_user_payload, "some_token")
#     assert result == OAuth2User(
#         external_id=app_user_payload["sub"],
#         type=UserType.APP,
#         permissions=[],
#         app_user=app_user_payload,
#     )
#
#
# @pytest.mark.asyncio
# async def test_get_app_user_success(oauth2_auth, monkeypatch):
#     user_id = 123
#     token = "valid_token"  # noqa
#     expected_user_data = {"id": user_id, "name": "John Doe"}
#
#     # Mocking httpx.AsyncClient().get behavior
#     async def mock_get(*args, **kwargs):
#         return Response(status_code=200, json=expected_user_data)
#
#     monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)
#
#     # Mocking httpx.AsyncClient().get behavior
#     oauth2_auth.insights_backend_host = "http://backend"
#     result = await oauth2_auth.get_app_user(user_id, token)
#     assert result == expected_user_data
