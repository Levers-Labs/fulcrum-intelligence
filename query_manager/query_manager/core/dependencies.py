from typing import Annotated

from fastapi import Depends

from query_manager.services.query_client import QueryClient


async def get_query_client() -> QueryClient:
    return QueryClient()


QueryClientDep = Annotated[QueryClient, Depends(get_query_client)]
