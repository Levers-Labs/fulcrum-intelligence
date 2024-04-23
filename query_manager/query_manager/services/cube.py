from enum import Enum

from httpx import Auth

from commons.clients.auth import JWTAuth, JWTSecretKeyAuth
from commons.clients.base import AsyncHttpClient


class CubeJWTAuthType(str, Enum):
    """
    Enum class for the supported Cube JWT authentication types.
    """

    SECRET_KEY = "SECRET_KEY"  # noqa
    TOKEN = "TOKEN"  # noqa


class CubeClient(AsyncHttpClient):
    def __init__(
        self,
        base_url: str,
        auth_type: CubeJWTAuthType | None = None,
        auth_options: dict[str, str] | None = None,
    ):
        """
        Base class for interacting with the Cube API.
        :param base_url: Base url of the Cube API e.g., https://analytics.cube.dev/cubejs-api/v1
        :param auth_type: The type of authentication to use, defaults to None if no authentication is required.
        :param auth_options: Dictionary containing the authentication options. Required if auth_type is not None.
        For SECRET_KEY auth_type, the dictionary should contain the key "secret_key".
        For TOKEN auth_type, the dictionary should contain the key "token" which is the JWT token.
        """
        self.auth_type = auth_type
        self.auth_options = auth_options
        self._validate_auth_options()
        auth = self._create_auth(auth_type, auth_options)
        super().__init__(base_url, auth=auth)

    def _create_auth(self, auth_type: CubeJWTAuthType | None, auth_options: dict[str, str] | None) -> Auth | None:
        """
        First validates the auth_options &
        Creates an Auth object based on the auth_type and auth_options.

        :param auth_type: The type of authentication to use.
        :param auth_options: Dictionary containing the authentication options.
        :return: An Auth object for the specified authentication type.
        """
        self._validate_auth_options()

        if auth_type is None or auth_options is None:
            return None
        if auth_type == CubeJWTAuthType.SECRET_KEY:
            return JWTSecretKeyAuth(auth_options["secret_key"])
        elif auth_type == CubeJWTAuthType.TOKEN:
            return JWTAuth(auth_options["token"])

    def _validate_auth_options(self):
        if self.auth_type is None:
            return
        if self.auth_type == CubeJWTAuthType.SECRET_KEY:
            if "secret_key" not in self.auth_options:
                raise ValueError("Secret key is required for SECRET_KEY authentication.")
        elif self.auth_type == CubeJWTAuthType.TOKEN:
            if "token" not in self.auth_options:
                raise ValueError("Token is required for TOKEN authentication.")
        else:
            raise ValueError(f"Unsupported authentication type: {self.auth_type}")

    async def load_query_data(self, query: dict) -> dict:
        """
        Loads the data for a given query from the Cube API.
        :param query: The query to execute.
        :return: The response data from the Cube API.
        """
        response = await self.post("load", data={"query": query})
        return response["data"]
