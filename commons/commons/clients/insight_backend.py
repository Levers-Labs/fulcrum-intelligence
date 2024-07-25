from commons.clients.base import AsyncHttpClient


class InsightBackendClient(AsyncHttpClient):
    async def get_user(self, user_id: int, token: str):
        # headers = {"Authorization": token}
        return await self.get(f"/users/{user_id}")  # , {"headers": headers})
