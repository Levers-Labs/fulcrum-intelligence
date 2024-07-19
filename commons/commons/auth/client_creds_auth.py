import logging
import time
from collections.abc import Generator

import jwt
import requests
from httpx import Auth, Request, Response

logger = logging.getLogger(__name__)


class ClientCredsAuth(Auth):
    def __init__(self, auth0_issuer: str, client_id: str, client_secret: str, api_audience: str):
        """
        Machine to Machine Comm. Auth class, It can be used to get the token for inter service communication.
        Ex: Analysis Manager wants to communicate with query manager, in that case we will pass this class object
        while creating the client.

        Args:
            auth0_issuer (str): The Auth0 issuer URL.
            client_id (str): The client ID for the Auth0 application.
            client_secret (str): The client secret for the Auth0 application.
            api_audience (str): The API audience for the Auth0 token.
        """
        self.auth0_issuer: str = auth0_issuer
        self.client_id: str = client_id
        self.client_secret: str = client_secret
        self.api_audience: str = api_audience
        self._token: str | None = None
        self._token_expiry: float = 0

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """
        Add Authorization header to request.

        Args:
            request (Request): A Request object.

        Yields:
            Request: The modified request with the Authorization header.
        """
        if not self._token or self.is_token_expired():
            self._token = self.get_auth0_access_token()
            decoded_token = jwt.decode(self._token, options={"verify_signature": False})
            self._token_expiry = decoded_token["exp"]

        request.headers["Authorization"] = f"Bearer {self._token}"
        logger.debug("Auth flow headers: %s", request.headers)
        yield request

    def get_auth0_access_token(self) -> str:
        """
        Fetch the token from Auth0 server.
        Client ID and client Secret are of specific apps in Auth0,
        based on client ID and secret we will get the scopes in token for authorization.

        Returns:
            str: The access token.
        """
        token_url = f"{self.auth0_issuer}/oauth/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "audience": self.api_audience,
        }

        headers = {"Content-Type": "application/json"}

        response = requests.post(token_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()["access_token"]

    def is_token_expired(self) -> bool:
        """
        Check if the current token is expired.

        Returns:
            bool: True if the token is expired, False otherwise.
        """
        return time.time() >= self._token_expiry
