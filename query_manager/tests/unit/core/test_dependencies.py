import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from query_manager.core.dependencies import (
    get_cube_client,
    get_parquet_service,
    get_query_client,
    get_s3_client,
)
from query_manager.services.cube import CubeClient
from query_manager.services.parquet import ParquetService
from query_manager.services.query_client import QueryClient
from query_manager.services.s3 import S3Client


@pytest.mark.asyncio
async def test_get_s3_client():
    s3_client = await get_s3_client()
    assert isinstance(s3_client, S3Client)


@pytest.mark.asyncio
async def test_get_cube_client():
    cube_client = await get_cube_client()
    assert isinstance(cube_client, CubeClient)


@pytest.mark.asyncio
async def test_get_parquet_service():
    s3_client = await get_s3_client()
    parquet_service = await get_parquet_service(s3_client=s3_client)
    assert isinstance(parquet_service, ParquetService)


@pytest.mark.asyncio
async def test_get_query_client():
    cube_client = await get_cube_client()
    query_client = await get_query_client(cube_client=cube_client, session=AsyncSession)
    assert isinstance(query_client, QueryClient)
