import logging
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import HTTPException

from commons.models.tenant import SlackConnectionConfig

logger = logging.getLogger(__name__)


class SlackOAuthService:
    SLACK_AUTHORIZE_URL: str = "https://slack.com/oauth/v2/authorize"
    SLACK_ACCESS_TOKEN_URL: str = "https://slack.com/api/oauth.v2.access"
    SLACK_REVOKE_URL: str = "https://slack.com/api/auth.revoke"

    def __init__(self, client_id: str, client_secret: str, redirect_uri: str, scopes: list[str]):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.scopes = scopes

    def generate_oauth_url(self) -> str:
        """Generate Slack OAuth URL with required parameters."""
        params = {
            "client_id": self.client_id,
            "scope": ",".join(self.scopes),
            "redirect_uri": self.redirect_uri,
        }
        return f"{self.SLACK_AUTHORIZE_URL}?{urlencode(params)}"

    async def authorize_oauth_code(self, code: str) -> SlackConnectionConfig:
        """Exchange authorization code for access token."""
        try:
            async with httpx.AsyncClient() as client:
                # Send the request to Slack
                response = await client.post(
                    SlackOAuthService.SLACK_ACCESS_TOKEN_URL,
                    data={
                        "code": code,
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                        "redirect_uri": self.redirect_uri,
                    },
                )
                response.raise_for_status()
                # Parse the response
                data: dict[str, Any] = response.json()

                # Check if the response is ok
                if not data.get("ok"):
                    raise HTTPException(status_code=400, detail=f"Slack OAuth error: {data.get('error')}")

                # Convert to SlackConnectionConfig
                return SlackConnectionConfig(
                    bot_token=data["access_token"],
                    bot_user_id=data["bot_user_id"],
                    app_id=data["app_id"],
                    team=data["team"],
                    authed_user=data["authed_user"],
                )

        except httpx.HTTPError as e:
            logger.error("Slack OAuth token authorization failed: %s", str(e))
            raise HTTPException(status_code=500, detail="Failed to authorize oauth code") from e

    async def revoke_oauth_token(self, token: str) -> None:
        """Revoke OAuth token."""
        try:
            async with httpx.AsyncClient() as client:
                await client.post(SlackOAuthService.SLACK_REVOKE_URL, data={"token": token})
        except httpx.HTTPError as e:
            logger.error("Slack OAuth token revocation failed: %s", str(e))
