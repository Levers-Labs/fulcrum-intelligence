from commons.clients.base import AsyncHttpClient


class InsightBackendClient(AsyncHttpClient):
    async def get_user(self, user_id: int, token: str):
        # headers = {"Authorization": token}
        return await self.get(f"/users/{user_id}")  # , {"headers": headers})

    async def get_tenant_config(self) -> dict:
        """
        Get tenant configuration for tenant from context.
        Raises an exception if the tenant is not found.
        :return:
        """
        return await self.get("/tenant/config")
