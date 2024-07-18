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
        auth0_domain: str,
        auth0_algorithms: list[str],
        auth0_api_audience: str,
        auth0_issuer: str,
        insights_backend_host: str | AnyHttpUrl,
    ) -> None:
        self.auth0_domain = auth0_domain
        self.auth0_algorithms = auth0_algorithms
        self.auth0_api_audience = auth0_api_audience
        self.auth0_issuer = auth0_issuer
        self.insights_backend_host = insights_backend_host
        self.jwks_client = self.get_jwk_client()

    def get_jwk_client(self):
        """
        Method to fetch the public keys for JWT, from specified auth0_domain
        """
        jwks_url = f"https://{self.auth0_domain}/.well-known/jwks.json"
        return jwt.PyJWKClient(jwks_url, cache_jwk_set=True, cache_keys=True)

    async def verify(
        self,
        security_scopes: SecurityScopes,
        token: HTTPAuthorizationCredentials | None = Depends(HTTPBearer()),  # noqa: B008
    ):
        """
        This method verifies the token, in total there are 3 checks
        - we fetch the public keys from the origin of jwt to verify the provided token
        - once the token is verified, we verify the claims
        - if the claims are verified, then we verify the user whose id is present in token with our DB.

        Input:
        - security_scopes: scopes required for a route to execute
        - token: jwt token, present in Authorization header

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
                issuer=self.auth0_issuer,
            )
        except Exception as error:
            raise UnauthenticatedException(str(error)) from error

        # to check the claims
        if len(security_scopes.scopes) > 0:
            self._check_token_claims(payload, "scope", security_scopes.scopes)

        # verify user, skipping the user check in DB if the request is a machine to machine comm.
        if payload["sub"].endswith("@clients"):
            return payload["sub"]

        user = await self.get_auth_user(payload["user_id"], token.credentials)
        return user

    def _check_token_claims(self, payload: dict[str, Any], claim_name: str, expected_value: list[str]):
        """
        In this method we are verifying the claims which are present in the payload with the ones required for the
        route
        Input:
        - payload: jwt payload
        - claim_name: key which contains the claim in payload
        - expected_value: scopes required for the route to execute
        """
        if claim_name not in payload:
            raise UnauthorizedException(detail=f'No claim "{claim_name}" found in token')

        payload_claim = payload[claim_name]

        for value in expected_value:
            if value not in payload_claim:
                raise UnauthorizedException(detail="User isn't authorised to perform this operation")

    async def get_auth_user(self, user_id: int, token: str):
        """
        Method to fetch the user from DB, whose ID is present in the JWT.
        Input:
        - user_id: id of the user
        - token : jwt token
        """
        async with httpx.AsyncClient() as client:
            headers = {"Authorization": f"Bearer {token}"}
            response = await client.get(
                urllib.parse.urljoin(  # type: ignore
                    str(self.insights_backend_host),
                    user_id,
                ),
                headers=headers,
            )

            user = response.json()
            if user is None:
                raise UnauthenticatedException(detail="Invalid User")
            return user
