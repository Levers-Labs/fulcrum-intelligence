import pytest

from query_manager.core.dependencies import get_query_client
from query_manager.services.query_client import QueryClient


@pytest.mark.asyncio
async def test_get_query_client():
    query_client = await get_query_client()
    assert isinstance(query_client, QueryClient)
