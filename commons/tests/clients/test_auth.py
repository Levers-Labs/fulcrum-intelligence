import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import jwt
import pytest
from requests import Request

from commons.clients.auth import ClientCredsAuth, JWTAuth, JWTSecretKeyAuth


def test_jwt_auth_init():
    token = "test_token"  # noqa
    auth = JWTAuth(token)
    assert auth.token == token


def test_jwt_auth_auth_flow():
    token = "test_token"  # noqa
    auth = JWTAuth(token)
    request = MagicMock()
    next(auth.auth_flow(request))
    request.headers.__setitem__.assert_called_once_with("Authorization", f"Bearer {token}")


@pytest.fixture
def jwt_secret_key_auth():
    secret_key = "test_secret_key"  # noqa
    token_expiration = timedelta(days=30)
    return JWTSecretKeyAuth(secret_key, token_expiration)


def test_jwt_secret_key_auth_init(jwt_secret_key_auth):
    assert jwt_secret_key_auth.secret_key == "test_secret_key"  # noqa
    assert jwt_secret_key_auth.token_expiration == timedelta(days=30)
    assert jwt_secret_key_auth.token is not None
    assert jwt_secret_key_auth.token_expiration_time is not None


def test_jwt_secret_key_auth_generate_token(jwt_secret_key_auth):
    old_token = jwt_secret_key_auth.token
    old_expiration_time = jwt_secret_key_auth.token_expiration_time
    # add 1-second sleep to make sure the token is different
    time.sleep(1)
    new_token = jwt_secret_key_auth._generate_token()
    assert new_token != old_token
    assert jwt_secret_key_auth.token == new_token
    assert jwt_secret_key_auth.token_expiration_time != old_expiration_time


def test_jwt_secret_key_auth_auth_flow_valid_token(jwt_secret_key_auth):
    request = MagicMock()
    next(jwt_secret_key_auth.auth_flow(request))
    request.headers.__setitem__.assert_called_once_with("Authorization", f"Bearer {jwt_secret_key_auth.token}")


def test_jwt_secret_key_auth_auth_flow_expired_token(jwt_secret_key_auth):
    request = MagicMock()
    jwt_secret_key_auth._generate_token()
    jwt_secret_key_auth.token_expiration_time = datetime.utcnow() - timedelta(seconds=1)
    with patch.object(jwt_secret_key_auth, "_generate_token") as mock_generate_token:
        next(jwt_secret_key_auth.auth_flow(request))
        mock_generate_token.assert_called_once()


def test_get_auth0_access_token():
    response_json = {"access_token": "mocked_access_token"}

    with patch("requests.post") as mock_post:
        # Configure mock response
        mock_response = MagicMock()
        mock_response.json.return_value = response_json
        mock_response.raise_for_status = lambda: None  # To mock successful response
        mock_post.return_value = mock_response

        auth_instance = ClientCredsAuth(
            auth0_issuer="issuer", client_id="id", client_secret="secret", api_audience="audience"  # noqa
        )

        token = auth_instance.get_auth0_access_token()

        assert token == response_json["access_token"]
        mock_post.assert_called_once()


def test_is_token_expired():
    # Create instance of ClientCredsAuth
    auth_instance = ClientCredsAuth(
        auth0_issuer="issuer", client_id="id", client_secret="secret", api_audience="audience"  # noqa
    )

    # Test case 1: Token not expired
    auth_instance._token_expiry = time.time() + 3600  # Setting expiry 1 hour from now
    assert not auth_instance.is_token_expired()

    # Test case 2: Token expired
    auth_instance._token_expiry = time.time() - 3600  # Setting expiry 1 hour ago
    assert auth_instance.is_token_expired()


@pytest.fixture
def mocked_access_token():
    return "mocked_access_token"


def test_auth_flow(mocked_access_token):
    auth_instance = ClientCredsAuth(
        auth0_issuer="issuer", client_id="id", client_secret="secret", api_audience="audience"  # noqa
    )

    request = Request("GET", "https://example.com")
    request.headers = {}

    auth_instance.get_auth0_access_token = lambda: mocked_access_token

    mock_decoded_token = {"exp": time.time() + 3600}  # Example expiration time in the future
    jwt.decode = MagicMock(return_value=mock_decoded_token)

    generator = auth_instance.auth_flow(request)
    modified_request = next(generator)

    assert "Authorization" in modified_request.headers
    assert modified_request.headers["Authorization"] == f"Bearer {mocked_access_token}"

    # Check that jwt.decode was called with the correct parameters
    jwt.decode.assert_called_once_with(mocked_access_token, options={"verify_signature": False})
