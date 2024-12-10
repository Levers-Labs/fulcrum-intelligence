from typing import Any

from commons.clients.base import AsyncHttpClient, HttpClientError
from commons.exceptions import InvalidTenantError
from commons.utilities.context import get_tenant_id


class InsightBackendClient(AsyncHttpClient):
    async def get_user(self, user_id: int, token: str):
        # headers = {"Authorization": token}
        return await self.get(f"/users/{user_id}")  # , {"headers": headers})

    async def get_tenant_config(self) -> dict:
        """
        Get tenant configuration for tenant from context.
        Raises an InvalidTenant exception if the tenant is not found.
        :return: dict
        """
        try:
            config = await self.get("/tenant/config/internal")
            return config
        except HttpClientError as e:
            if e.status_code == 404:
                tenant_id = get_tenant_id()
                raise InvalidTenantError(tenant_id) from e
            raise

    async def validate_tenant(self):
        """
        Validates the existence of a tenant by attempting to retrieve its configuration.
        This function asynchronously retrieves the Insights Backend Client instance using the provided settings.
        It then attempts to fetch the tenant configuration using the client. If the attempt fails, it logs a warning
        indicating that the tenant ID does not exist and raises an InvalidTenantError.
        :param tenant_id: The ID of the tenant to validate.
        :raises InvalidTenantError: If the tenant ID does not exist.
        """
        # Attempt to fetch the tenant configuration
        try:
            _ = await self.get("/tenant/config")
        except HttpClientError as e:
            if e.status_code == 404:
                tenant_id = get_tenant_id()
                raise InvalidTenantError(tenant_id) from e
            raise

    async def get_slack_channel_details(self, channel_id: str) -> Any:
        """
        Get tenant configuration for tenant from context.
        Raises an InvalidTenant exception if the tenant is not found.
        :return: dict
        """
        response = await self.get(f"slack/channels/{channel_id}")
        return response

    async def get_slack_config(self) -> dict:
        """
        Get tenant configuration for tenant from context.
        Raises an InvalidTenant exception if the tenant is not found.
        :return: dict
        """
        try:
            config = await self.get("/tenant/config/internal")
            slack_config = config["slack_connection"]
            return slack_config
        except HttpClientError as e:
            if e.status_code == 404:
                tenant_id = get_tenant_id()
                raise InvalidTenantError(tenant_id) from e
            raise
