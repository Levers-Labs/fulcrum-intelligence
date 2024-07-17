import logging
from collections.abc import Generator

import requests
from httpx import Auth, Request, Response

logger = logging.getLogger(__name__)


class M2MAuth(Auth):
    def __init__(self, config):
        """
        Machine to Machine Comm. Auth class, It can be used to get the token for inter service communication.
        Ex: Analysis Manager wants to communicate with query manager, in that case we will pass this class object
        while creating the client.
        :param config:
        """
        self.config = config

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """
        Add Authorization header to request
        :param request: a Request object
        :return: Response object
        """
        request.headers["Authorization"] = f"Bearer {self.get_auth0_access_token()}"
        logger.info("headers", request.headers)
        yield request

    def regenerate_expired_token(self):
        """
        If the token is expired, regenerate it.
        """
        return self.get_auth0_access_token()

    def get_auth0_access_token(self):
        """
        Fetching the token from Auth0 server,
        Client ID and client Secret are of specific apps in Auth0,
        based on client ID and secret we will get the scopes in token for authorization
        """
        token_url = f"https://{self.config.AUTH0_DOMAIN}/oauth/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.config.SERVICE_CLIENT_ID,
            "client_secret": self.config.SERVICE_CLIENT_SECRET,
            "audience": self.config.AUTH0_API_AUDIENCE,
        }

        headers = {"Content-Type": "application/json"}

        response = requests.post(token_url, json=payload, headers=headers, timeout=5)
        response.raise_for_status()
        return response.json()["access_token"]
