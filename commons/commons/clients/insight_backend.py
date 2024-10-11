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
            config = await self.get("/tenant/config")
            return config
        except HttpClientError as e:
            if e.status_code == 404:
                tenant_id = get_tenant_id()
                raise InvalidTenantError(tenant_id) from e
            raise
