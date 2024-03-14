import httpx


class AsyncHttpClient:
    def __init__(self, base_url):
        self.base_url = base_url

    async def get(self, endpoint, params=None):
        async with httpx.AsyncClient() as client:
            response = await client.get(self.base_url + endpoint, params=params)
            return response.json()

    async def post(self, endpoint, data=None):
        async with httpx.AsyncClient() as client:
            response = await client.post(self.base_url + endpoint, json=data)
            return response.json()
