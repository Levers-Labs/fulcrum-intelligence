import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from query_manager.core.crud import CRUDDimensions, CRUDMetric
from query_manager.core.dependencies import (
    get_cube_client,
    get_dimensions_crud,
    get_metric_crud,
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
async def test_get_dimensions_crud():
    dimensions_crud = await get_dimensions_crud(session=AsyncSession)
    assert isinstance(dimensions_crud, CRUDDimensions)


@pytest.mark.asyncio
async def test_get_metrics_crud():
    metrics_crud = await get_metric_crud(session=AsyncSession)
    assert isinstance(metrics_crud, CRUDMetric)


@pytest.mark.asyncio
async def test_get_query_client():
    dimensions_crud = await get_dimensions_crud(session=AsyncSession)
    metric_crud = await get_metric_crud(session=AsyncSession)
    cube_client = await get_cube_client()
    query_client = await get_query_client(
        cube_client=cube_client, dimensions_crud=dimensions_crud, metric_crud=metric_crud
    )
    assert isinstance(query_client, QueryClient)
