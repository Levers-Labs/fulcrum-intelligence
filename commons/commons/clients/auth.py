"""
Auth classes for httpx client
"""

from collections.abc import Generator
from datetime import datetime, timedelta

import jwt
from httpx import Auth, Request, Response


class JWTAuth(Auth):
    def __init__(self, token: str):
        """
        JWT Auth class
        :param token:
        """
        self.token = token

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """
        Add Authorization header to request
        :param request: a Request object
        :return: Response object
        """
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request


class JWTSecretKeyAuth(JWTAuth):
    """
    JWT Secret Key Auth class
    """

    def __init__(self, secret_key: str, token_expiration: timedelta = timedelta(days=30)):
        self.secret_key = secret_key
        self.token_expiration = token_expiration
        self.token_expiration_time: datetime | None = None
        super().__init__(self._generate_token())

    def _generate_token(self) -> str:
        """
        Generate a JWT token
        Also updates the token and token expiration time
        :return: JWT token
        """
        expires_at = datetime.utcnow() + self.token_expiration
        token = jwt.encode({"exp": expires_at}, self.secret_key, algorithm="HS256")
        self.token = token
        self.token_expiration_time = expires_at
        return token

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """
        Add Authorization header to request
        :param request: a Request object
        :return: Response object
        """
        if not self.token or (self.token_expiration_time and self.token_expiration_time <= datetime.utcnow()):
            self._generate_token()
        return super().auth_flow(request)
