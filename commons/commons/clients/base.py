import re
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

from commons.auth.auth import UnauthorizedException
from commons.auth.constants import TENANT_ID_HEADER, TENANT_VERIFICATION_BYPASS_ENDPOINTS
from commons.utilities.context import get_tenant_id
from commons.utilities.json_utils import serialize_json


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
    def __init__(self, base_url: str | AnyHttpUrl, api_version: str = "v1", auth: Auth | None = None):
        self.base_url = self.get_base_url_with_version(base_url, api_version)
        self.auth = auth

    @staticmethod
    def get_base_url_with_version(base_url: str | AnyHttpUrl, api_version: str = "v1") -> str:
        """
        Get the base URL with the API version.
        If the base URL ends with a slash, it is removed.
        If the base URL does not end with the API version, it is appended.

        Args:
            base_url (str | AnyHttpUrl): The base URL to be modified.
            api_version (str, optional): The API version to be appended to the base URL. Defaults to "v1".

        Returns:
            str: The base URL with the API version.
        """
        base_url = str(base_url)
        if base_url.endswith("/"):
            base_url = base_url[:-1]
        if not re.search(rf"/{api_version}$", base_url):
            base_url = f"{base_url}/{api_version}"
        return base_url

    @staticmethod
    def _get_url(base_url: str | AnyHttpUrl, endpoint: str) -> str:
        """
        Join base_url and endpoint
        """
        path = str(base_url).rstrip("/") + "/" + endpoint.lstrip("/")
        return path

    def _add_tenant_id_to_headers(self, headers: dict[str, Any], endpoint: str) -> dict[str, Any]:
        """
        Fetch the tenant ID and add it to the headers, unless the endpoint is in the bypass list.

        Args:
            headers (dict[str, Any]): The original headers.
            endpoint (str): The API endpoint being called.

        Returns:
            dict[str, Any]: The headers with the tenant ID added if required.

        Raises:
            UnauthorizedException: If the tenant ID is not found for non-bypassed endpoints.
        """
        # Check if endpoint is in bypass list
        if any(endpoint in bypass_endpoint for bypass_endpoint in TENANT_VERIFICATION_BYPASS_ENDPOINTS):
            return headers

        tenant_id = get_tenant_id()
        if not tenant_id:
            raise UnauthorizedException(detail="Tenant ID not found in context")

        headers[TENANT_ID_HEADER] = str(tenant_id)
        return headers

    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Response:
        """
        Make an asynchronous HTTP request to the specified endpoint.
        """
        # Set 1 min timeout if not provided
        kwargs.setdefault("timeout", 60)

        # Add tenant ID to headers
        headers = kwargs.get("headers", {})
        kwargs["headers"] = self._add_tenant_id_to_headers(headers, endpoint)

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
                raise HttpClientError("Request timed out", url=url) from e

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
        # Use custom JSON encoder to handle datetime objects
        response = await self._make_request("POST", endpoint, json=serialize_json(data))
        return response.json()
