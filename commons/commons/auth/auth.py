import logging
import urllib
from collections.abc import Callable
from typing import Any, TypeVar

import httpx
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, SecurityScopes
from pydantic import AnyHttpUrl

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


class UnauthorizedException(HTTPException):
    def __init__(self, detail: str, **kwargs):
        """Returns HTTP 403"""
        super().__init__(status.HTTP_403_FORBIDDEN, detail=detail)


class UnauthenticatedException(HTTPException):
    def __init__(self, detail: str, **kwargs):
        """Returns HTTP 401"""
        super().__init__(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)


class Auth:
    """
    Class to handle the authentication and authorization related checks
    - verify a jwt token
    - verify the claims
    - verify the authenticated user from DB
    """

    def __init__(
        self,
        auth0_api_audience: str,
        auth0_issuer: str,
        insights_backend_host: str | AnyHttpUrl,
    ) -> None:
        """
        Initialize the Auth class.

        Args:
            auth0_api_audience (str): The API audience for Auth0.
            auth0_issuer (str): The issuer URL for Auth0.
            insights_backend_host (str | AnyHttpUrl): The host URL for the insights backend.
        """
        self.auth0_api_audience = auth0_api_audience
        self.auth0_issuer = auth0_issuer
        self.insights_backend_host = insights_backend_host
        self.jwks_client = self.get_jwk_client()
        self.auth0_algorithms = ["RS256"]  # Add this line to define the algorithms

    def get_jwk_client(self) -> jwt.PyJWKClient:
        """
        Method to fetch the public keys for JWT, from specified auth0_domain.

        Returns:
            jwt.PyJWKClient: The JWT client for fetching public keys.
        """
        jwks_url = f"{self.auth0_issuer}/.well-known/jwks.json"
        return jwt.PyJWKClient(jwks_url, cache_jwk_set=True, cache_keys=True)

    async def verify(
        self,
        security_scopes: SecurityScopes,
        token: HTTPAuthorizationCredentials | None = Depends(HTTPBearer()),  # noqa: B008
    ) -> Any:
        """
        This method verifies the token, in total there are 3 checks
        - we fetch the public keys from the origin of jwt to verify the provided token
        - once the token is verified, we verify the claims
        - if the claims are verified, then we verify the user whose id is present in token with our DB.

        Args:
            security_scopes (SecurityScopes): Scopes required for a route to execute.
            token (HTTPAuthorizationCredentials | None): JWT token, present in Authorization header.

        Returns:
            Any: The authenticated user or machine-to-machine client ID.

        Raises:
            UnauthenticatedException: If the token is invalid or authentication fails.
            UnauthorizedException: If the user is not authorized to perform the operation.
        """
        if token is None:
            raise UnauthenticatedException(detail="Authentication token is not provided.")

        # This gets the 'kid' from the passed token
        try:
            signing_key = self.jwks_client.get_signing_key_from_jwt(token.credentials).key
        except jwt.exceptions.PyJWKClientError as error:
            raise UnauthenticatedException(str(error)) from error
        except jwt.exceptions.DecodeError as error:
            raise UnauthenticatedException(str(error)) from error

        try:
            payload = jwt.decode(
                token.credentials,
                signing_key,
                algorithms=self.auth0_algorithms,
                audience=self.auth0_api_audience,
                options={"verify_issuer": False},
            )
        except Exception as error:
            raise UnauthenticatedException(str(error)) from error

        if security_scopes.scopes:
            self._check_token_claims(payload, "scope", security_scopes.scopes)

        # verify user, skipping the user check in DB if the request is a machine to machine comm.
        if payload["sub"].endswith("@clients"):
            return payload["sub"]

        user = await self.get_auth_user(payload["user_id"], token.credentials)
        return user

    def _check_token_claims(self, payload: dict[str, Any], claim_name: str, expected_value: list[str]) -> None:
        """
        Verify the claims which are present in the payload with the ones required for the route.

        Args:
            payload (dict[str, Any]): JWT payload.
            claim_name (str): Key which contains the claim in payload.
            expected_value (list[str]): Scopes required for the route to execute.

        Raises:
            UnauthorizedException: If the required claims are not present or the user is not authorized.
        """
        if claim_name not in payload:
            raise UnauthorizedException(detail=f'No claim "{claim_name}" found in token')

        payload_claim = payload[claim_name]

        for value in expected_value:
            if value not in payload_claim:
                raise UnauthorizedException(detail="User isn't authorised to perform this operation")

    async def get_auth_user(self, user_id: int, token: str) -> dict[str, Any]:
        """
        Method to fetch the user from DB, whose ID is present in the JWT.

        Args:
            user_id (int): ID of the user.
            token (str): JWT token.

        Returns:
            dict[str, Any]: The authenticated user data.

        Raises:
            UnauthenticatedException: If the user is invalid or not found.
        """
        async with httpx.AsyncClient() as client:
            headers = {"Authorization": f"Bearer {token}"}
            response = await client.get(
                urllib.parse.urljoin(  # type: ignore
                    str(self.insights_backend_host),
                    str(user_id),  # Convert user_id to string
                ),
                headers=headers,
            )

            user = response.json()
            if user is None:
                raise UnauthenticatedException(detail="Invalid User")
            return user
