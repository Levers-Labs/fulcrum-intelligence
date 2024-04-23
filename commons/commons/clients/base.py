from http import HTTPStatus
from json import JSONDecodeError
from typing import Any

from httpx import (
    AsyncClient,
    Auth,
    HTTPStatusError,
    InvalidURL,
    Response,
    TimeoutException,
)
from pydantic import AnyHttpUrl


class HttpClientError(Exception):
    def __init__(
        self, message: str, status_code: int | None = None, content: dict | None = None, url: str | None = None
    ):
        self.message = message
        self.status_code = status_code
        self.content = content
        self.url = url
        super().__init__(self.message)


class AsyncHttpClient:
    def __init__(self, base_url: str | AnyHttpUrl, auth: Auth | None = None):
        self.base_url = base_url
        self.auth = auth

    @staticmethod
    def _get_url(base_url: str | AnyHttpUrl, endpoint: str) -> str:
        """
        Join base_url and endpoint
        """
        path = str(base_url).rstrip("/") + "/" + endpoint.lstrip("/")
        return path

    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Response:
        """
        make an asynchronous HTTP request to the specified endpoint.

        Args:
            method (str): The HTTP method for the request (e.g., 'GET', 'POST').
            endpoint (str): The endpoint URL relative to the base URL.
            **kwargs: Additional keyword arguments to pass to the underlying HTTP client.

        Returns:
            Response: The Response object returned by the HTTP client.

        Raises:
            HttpClientError: If an error occurs during the request or response handling.
                - If the response status code is 401 (Unauthorized):
                    - If the error description indicates an expired token, raises an HttpClientError
                      with the message "Token is expired".
                    - Otherwise, raises an HttpClientError with the message "Authentication failed".
                - If the response status code is 404 (Not Found), raises an HttpClientError with the
                  message "Resource not found".
                - if the response status code is 422 (Unprocessable Entity), raises an HttpClientError
                  with the message "Invalid input".
                - for any other HTTP error, raise an HttpClientError with the error message.
            HttpClientError: If an invalid URL is provided or if the request times out.

        Example:
            response = await _make_request('GET', '/users', params={'page': 1})
            print(response.json())
        """
        async with AsyncClient(auth=self.auth) as client:
            url = self._get_url(self.base_url, endpoint)
            try:
                response = await client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except HTTPStatusError as e:
                content = None
                if e.response is not None:
                    try:
                        content = e.response.json()
                    except (JSONDecodeError, UnicodeDecodeError):
                        content = None

                status_code = e.response.status_code if e.response else None
                if status_code == HTTPStatus.UNAUTHORIZED:
                    if content and "Token is expired" in content.get("description", ""):
                        raise HttpClientError(
                            "Token is expired", status_code=status_code, content=content, url=url
                        ) from e
                    raise HttpClientError(
                        "Authentication failed", status_code=status_code, content=content, url=url
                    ) from e
                elif status_code == HTTPStatus.NOT_FOUND:
                    raise HttpClientError(
                        "Resource not found", status_code=status_code, content=content, url=url
                    ) from e
                elif status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
                    raise HttpClientError("Invalid input", status_code=status_code, content=content, url=url) from e
                else:
                    raise HttpClientError(
                        f"HTTP error occurred: {e}", status_code=status_code, content=content, url=url
                    ) from e
            except InvalidURL as e:
                raise HttpClientError(f"Invalid URL: {e}", url=url) from e
            except TimeoutException as e:
                raise HttpClientError(f"Request timed out: {e}", url=url) from e

    async def get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Makes a async http get request.
        endpoint: absolute or relative url
        """
        response = await self._make_request("GET", endpoint, params=params)
        return response.json()

    async def post(self, endpoint: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Makes a async http post request.
        endpoint: absolute or relative url
        data: request body
        """
        response = await self._make_request("POST", endpoint, json=data)
        return response.json()
