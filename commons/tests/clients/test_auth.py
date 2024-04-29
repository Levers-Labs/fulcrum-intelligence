import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from commons.clients.auth import JWTAuth, JWTSecretKeyAuth


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
