import pytest

from query_manager.config import Storage, get_settings
from query_manager.core.dependencies import (
    get_cube_client,
    get_parquet_service,
    get_query_client,
    get_s3_client,
    get_supabase_client,
)
from query_manager.services.cube import CubeClient
from query_manager.services.parquet import ParquetService
from query_manager.services.query_client import QueryClient
from query_manager.services.s3 import S3Client
from query_manager.services.supabase_client import SupabaseClient


@pytest.mark.asyncio
async def test_get_s3_client():
    s3_client = await get_s3_client()
    assert isinstance(s3_client, S3Client)


@pytest.mark.asyncio
async def test_get_supabase_client():
    settings = get_settings()

    if settings.STORAGE == Storage.SUPABASE:
        supabase_client = await get_supabase_client()
        assert isinstance(supabase_client, SupabaseClient)


@pytest.mark.asyncio
async def test_get_cube_client():
    cube_client = await get_cube_client()
    assert isinstance(cube_client, CubeClient)


@pytest.mark.asyncio
async def test_get_parquet_service():
    parquet_service = await get_parquet_service()
    assert isinstance(parquet_service, ParquetService)


@pytest.mark.asyncio
async def test_get_query_client():
    cube_client = await get_cube_client()
    query_client = await get_query_client(cube_client=cube_client)
    assert isinstance(query_client, QueryClient)
