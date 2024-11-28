from unittest.mock import AsyncMock, patch
from urllib.parse import urlencode

import pytest
from fastapi import HTTPException
from httpx import HTTPError

from insights_backend.services.slack_oauth import SlackOAuthService


@pytest.fixture
def oauth_service():
    return SlackOAuthService(
        client_id="test_client_id",
        client_secret="test_client_secret",  # noqa
        redirect_uri="http://test.com/callback",
        scopes=["channels:read", "chat:write"],
    )


def test_generate_oauth_url(oauth_service):
    url = oauth_service.generate_oauth_url()

    assert "client_id=test_client_id" in url
    assert urlencode({"scope": "channels:read"}) in url
    assert urlencode({"redirect_uri": "http://test.com/callback"}) in url
    assert url.startswith(SlackOAuthService.SLACK_AUTHORIZE_URL)


@pytest.mark.asyncio
async def test_authorize_oauth_code_success(oauth_service):
    mock_response = {
        "ok": True,
        "access_token": "xoxb-test-token",
        "bot_user_id": "U0123456",
        "app_id": "A0123456",
        "team": {"id": "T0123456", "name": "Test Team"},
        "authed_user": {"id": "U0123456"},
    }

    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.post = AsyncMock(
            return_value=AsyncMock(json=lambda: mock_response, raise_for_status=lambda: None)
        )

        result = await oauth_service.authorize_oauth_code("test_code")

        assert result.bot_token == "xoxb-test-token"  # noqa
        assert result.bot_user_id == "U0123456"
        assert result.app_id == "A0123456"
        assert result.team == {"id": "T0123456", "name": "Test Team"}
        assert result.authed_user == {"id": "U0123456"}


@pytest.mark.asyncio
async def test_authorize_oauth_code_slack_error(oauth_service):
    mock_response = {"ok": False, "error": "invalid_code"}

    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.post = AsyncMock(
            return_value=AsyncMock(json=lambda: mock_response, raise_for_status=lambda: None)
        )

        with pytest.raises(HTTPException) as exc_info:
            await oauth_service.authorize_oauth_code("invalid_code")

        assert exc_info.value.status_code == 400
        assert "Slack OAuth error: invalid_code" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_authorize_oauth_code_http_error(oauth_service):
    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.post = AsyncMock(side_effect=HTTPError("HTTP Error"))

        with pytest.raises(HTTPException) as exc_info:
            await oauth_service.authorize_oauth_code("test_code")

        assert exc_info.value.status_code == 500
        assert "Failed to authorize oauth code" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_revoke_oauth_token(oauth_service):
    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.post = AsyncMock()

        await oauth_service.revoke_oauth_token("test_token")

        mock_client.return_value.__aenter__.return_value.post.assert_called_once_with(
            SlackOAuthService.SLACK_REVOKE_URL, data={"token": "test_token"}
        )


@pytest.mark.asyncio
async def test_revoke_oauth_token_error(oauth_service):
    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__.return_value.post = AsyncMock(side_effect=HTTPError("HTTP Error"))

        await oauth_service.revoke_oauth_token("test_token")  # Should log error but not raise
