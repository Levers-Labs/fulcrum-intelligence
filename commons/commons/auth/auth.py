import logging
import urllib
from collections.abc import Callable
from enum import Enum
from typing import Annotated, Any, TypeVar

import httpx
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, SecurityScopes
from pydantic import AnyHttpUrl

from commons.models import BaseModel

F = TypeVar("F", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


class UserType(str, Enum):
    APP = "app"
    MACHINE = "machine"


class OAuth2User(BaseModel):
    external_id: str
    type: UserType
    permissions: list[str]
    # for app user
    app_user: dict | None = None


class UnauthorizedException(HTTPException):
    def __init__(self, detail: str, **kwargs):
        """Returns HTTP 403"""
        super().__init__(status.HTTP_403_FORBIDDEN, detail=detail)


class UnauthenticatedException(HTTPException):
    def __init__(self, detail: str, **kwargs):
        """Returns HTTP 401"""
        super().__init__(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)


class Oauth2Auth:
    """
    Class to handle OAuth2 authentication
    verify the token, scopes, and user
    """

    def __init__(
        self,
        api_audience: str,
        issuer: str,
        insights_backend_host: str | AnyHttpUrl | None = None,
        algorithms: list[str] | None = None,
    ) -> None:
        """
        Initialize the Auth class.

        Args:
            api_audience (str): The API audience for OAuth2.
            issuer (str): The issuer URL for OAuth2.
            insights_backend_host (str | AnyHttpUrl): The host URL for the insights backend.
        """
        self.api_audience = api_audience
        self.issuer = issuer
        self.insights_backend_host = insights_backend_host
        self.jwks_client = self.get_jwk_client()
        self.algorithms = algorithms or ["RS256"]

    def get_jwk_client(self) -> jwt.PyJWKClient:
        """
        Method to fetch the public keys for JWT, from specified issuer.

        Returns:
            jwt.PyJWKClient: The JWT client for fetching public keys.
        """
        jwks_url = f"{self.issuer}.well-known/jwks.json"
        return jwt.PyJWKClient(jwks_url, cache_jwk_set=True, cache_keys=True)

    def verify_jwt(self, token: str) -> dict[str, Any]:
        """
        Verify the JWT token and return its payload.

        Args:
            token (str): The JWT token to verify.

        Returns:
            dict[str, Any]: The decoded JWT payload.

        Raises:
            UnauthenticatedException: If the token is invalid or cannot be decoded.
        """
        try:
            signing_key = self.jwks_client.get_signing_key_from_jwt(token).key
        except (jwt.exceptions.PyJWKClientError, jwt.exceptions.DecodeError) as error:
            raise UnauthenticatedException(str(error)) from error

        try:
            payload = jwt.decode(
                token,
                signing_key,
                algorithms=self.algorithms,
                audience=self.api_audience,
                issuer=f"{self.issuer.rstrip('/')}/",
            )
            return payload
        except Exception as error:
            raise UnauthenticatedException(str(error)) from error

    async def verify(
        self,
        security_scopes: SecurityScopes,
        token: Annotated[HTTPAuthorizationCredentials | None, Depends(HTTPBearer(auto_error=False))] = None,
    ) -> OAuth2User:
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

        # verify the token
        payload = self.verify_jwt(token.credentials)
        # Verify the scopes if route requires it
        if security_scopes.scopes:
            # in case of M2M app token permissions are present in scope key
            # else in case of normal app user it is present in permissions key
            claims_key = "scope" if payload["sub"].endswith("@clients") else "permissions"

            self._verify_token_claims(payload, claims_key, security_scopes.scopes)

        user = await self.get_oauth_user(payload, token.credentials)
        return user

    def _verify_token_claims(self, payload: dict[str, Any], claim_name: str, expected_value: list[str]) -> None:
        """
        Verify the claims which are present in the payload with the ones required for the route.

        Args:
            payload (dict[str, Any]): JWT payload.
            claim_name (str): Key which contains the claim in payload.
            expected_value (list[str]): Scopes required for the route to execute.

        Raises:
            UnauthorizedException: If the required claims are not present or the user is not authorized.
        """
        # Check if the claim is present in the payload
        if claim_name not in payload:
            raise UnauthorizedException(detail=f'No claim "{claim_name}" found in token')

        payload_claim = payload[claim_name]

        # Check if the claim contains all the required values
        for value in expected_value:
            if value not in payload_claim:
                raise UnauthorizedException(detail="User isn't authorised to perform this operation")

    async def get_oauth_user(self, payload: dict[str, Any], token: str) -> OAuth2User:
        """
        Create an OAuth2User object based on the JWT payload.

        Args:
            payload (dict[str, Any]): The decoded JWT payload.
            token (str): JWT token.

        Returns:
            OAuth2User: An OAuth2User object containing user information.
        """
        is_machine_user = payload["sub"].endswith("@clients")

        if is_machine_user:
            return OAuth2User(
                external_id=payload["sub"],
                type=UserType.MACHINE,
                permissions=payload.get("scope", "").split(),
            )
        else:
            # Fetch user details from insights backend
            user_data = await self.get_app_user(payload["userId"], token)

            return OAuth2User(
                external_id=payload["sub"],
                type=UserType.APP,
                permissions=payload.get("scope", "").split(),
                app_user=user_data,
            )

    async def get_app_user(self, user_id: int, token: str) -> dict[str, Any]:
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
        if self.insights_backend_host is None:
            raise UnauthenticatedException(detail="Insights backend host is not provided")

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
