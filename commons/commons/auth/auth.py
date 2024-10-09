import logging
from collections.abc import Callable
from enum import Enum
from typing import Annotated, Any, TypeVar

import jwt
from fastapi import (
    Depends,
    HTTPException,
    Request,
    status,
)
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, SecurityScopes

from commons.auth.constants import TENANT_VERIFICATION_BYPASS_ENDPOINTS
from commons.models import BaseModel
from commons.utilities.context import get_tenant_id

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
    app_user_id: int | None = None


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
        algorithms: list[str] | None = None,
    ) -> None:
        """
        initialize the Auth class.

        Args:
            api_audience (str): The API audience for OAuth2.
            issuer (str): The issuer URL for OAuth2.
        """
        self.api_audience = api_audience
        self.issuer = issuer.rstrip("/")
        self.jwks_client = self.get_jwk_client()
        self.algorithms = algorithms or ["RS256"]

    def get_jwk_client(self) -> jwt.PyJWKClient:
        """
        Method to fetch the public keys for JWT, from specified issuer.

        Returns:
            jwt.PyJWKClient: The JWT client for fetching public keys.
        """
        jwks_url = f"{self.issuer.rstrip('/')}/.well-known/jwks.json"
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
                issuer=f"{self.issuer}/",
            )
            return payload
        except Exception as error:
            raise UnauthenticatedException(str(error)) from error

    async def verify(
        self,
        security_scopes: SecurityScopes,
        request: Request,
        token: Annotated[HTTPAuthorizationCredentials | None, Depends(HTTPBearer(auto_error=False))] = None,
    ) -> OAuth2User:
        """
        This method verifies the token, in total there are 3 checks
        - we fetch the public keys from the origin of jwt to verify the provided token
        - once the token is verified, we verify the claims
        - if the claims are verified, then we verify the user whose id is present in token with our DB.

        Args:
            security_scopes (SecurityScopes): Scopes required for a route to execute.
            request (Request): The request object.
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

        # verify tenant: JWT tenant is same as the tenant_id in request header
        await self.verify_tenant(payload, request)

        # Verify the scopes if route requires it
        if security_scopes.scopes:
            # in case of M2M app token permissions are present in scope key
            # else in case of normal app user it is present in permissions key
            claims_key = "scope" if payload["sub"].endswith("@clients") else "permissions"

            self._verify_token_claims(payload, claims_key, security_scopes.scopes)

        user = await self.get_oauth_user(payload, token.credentials)
        return user

    async def verify_tenant(self, payload: dict[str, Any], request: Request):
        """
        Verify the tenant passed in request header
        payload: token payload
        request: request object
        """
        current_route = request.url.path.removeprefix(request.base_url.path)
        if current_route.strip("/") in TENANT_VERIFICATION_BYPASS_ENDPOINTS:
            logger.info(f"Skipping tenant verification for route: {current_route}")
            return
        # Verify tenant id is present in context
        tenant_id = get_tenant_id()
        if not tenant_id:
            raise UnauthorizedException(detail="Tenant id not found in request header")
        # TODO: remove this after auth0 feture is enabled
        # https://community.auth0.com/t/why-is-there-an-organization-property-in-the-python-skds-client-credential-flow/134158  # noqa
        if self.is_machine_user(payload):
            logger.info("Machine user, tenant id check skipped")
            return
        if "tenant_id" not in payload or payload["tenant_id"] != tenant_id:
            raise UnauthorizedException(detail="Tenant id in token doesn't match tenant id in request header")

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

    def is_machine_user(self, payload: dict[str, Any]) -> bool:
        """
        Check if the user is a machine user.
        """
        return payload["sub"].endswith("@clients")

    async def get_oauth_user(self, payload: dict[str, Any], token: str) -> OAuth2User:
        """
        Create an OAuth2User object based on the JWT payload.

        Args:
            payload (dict[str, Any]): The decoded JWT payload.
            token (str): JWT token.

        Returns:
            OAuth2User: An OAuth2User object containing user information.
        """
        is_machine_user = self.is_machine_user(payload)

        if is_machine_user:
            return OAuth2User(
                external_id=payload["sub"],
                type=UserType.MACHINE,
                permissions=payload.get("scope", "").split(),
            )
        else:
            # Fetch user details from insights backend
            user_id = payload.get("user_id", "userId")
            # convert user_id to int if present
            user_id = int(user_id) if user_id else None
            # raise error if user_id is not present in payload
            if user_id is None:
                raise UnauthenticatedException(detail="No user id found in token")

            return OAuth2User(
                external_id=payload["sub"],
                type=UserType.APP,
                permissions=payload.get("scope", "").split(),
                app_user_id=user_id,
            )
