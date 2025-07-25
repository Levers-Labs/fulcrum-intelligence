from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from query_manager.core.crud import (
    CRUDDimensions,
    CRUDMetric,
    CRUDMetricCacheConfig,
    CRUDMetricCacheGrainConfig,
)
from query_manager.core.dependencies import (
    get_cube_client,
    get_dimensions_crud,
    get_metric_cache_config_crud,
    get_metric_cache_grain_config_crud,
    get_metric_crud,
    get_parquet_service,
    get_query_client,
    get_s3_client,
)
from query_manager.services.cube import CubeClient, CubeJWTAuthType
from query_manager.services.parquet import ParquetService
from query_manager.services.query_client import QueryClient
from query_manager.services.s3 import S3Client


@pytest.fixture
def mock_insights_backend_client():
    client = AsyncMock()
    client.get_tenant_config.return_value = {
        "cube_connection_config": {
            "cube_api_url": "http://test-cube-api.com",
            "cube_auth_type": "SECRET_KEY",
            "cube_auth_secret_key": "test-secret-key",
        }
    }
    return client


@pytest.fixture
def mock_async_session():
    # Create a mock session that will pass type checking
    session = MagicMock(spec=AsyncSession)
    return session


@pytest.mark.asyncio
async def test_get_s3_client():
    s3_client = await get_s3_client()
    assert isinstance(s3_client, S3Client)


@pytest.mark.asyncio
async def test_get_cube_client(mock_insights_backend_client):
    cube_client = await get_cube_client(mock_insights_backend_client)
    assert isinstance(cube_client, CubeClient)
    # Additional assertions to verify the client configuration
    assert cube_client.base_url == "http://test-cube-api.com/v1"
    assert cube_client.auth_type == CubeJWTAuthType.SECRET_KEY
    assert cube_client.auth_options == {"secret_key": "test-secret-key"}


@pytest.mark.asyncio
async def test_get_parquet_service():
    s3_client = await get_s3_client()
    parquet_service = await get_parquet_service(s3_client=s3_client)
    assert isinstance(parquet_service, ParquetService)


@pytest.mark.asyncio
async def test_get_dimensions_crud(mock_async_session):
    dimensions_crud = await get_dimensions_crud(session=mock_async_session)
    assert isinstance(dimensions_crud, CRUDDimensions)


@pytest.mark.asyncio
async def test_get_metrics_crud(mock_async_session):
    metrics_crud = await get_metric_crud(session=mock_async_session)
    assert isinstance(metrics_crud, CRUDMetric)


@pytest.mark.asyncio
async def test_get_metric_cache_grain_config_crud(mock_async_session):
    metric_cache_grain_config_crud = await get_metric_cache_grain_config_crud(session=mock_async_session)
    assert isinstance(metric_cache_grain_config_crud, CRUDMetricCacheGrainConfig)


@pytest.mark.asyncio
async def test_get_metric_cache_config_crud(mock_async_session):
    metric_cache_config_crud = await get_metric_cache_config_crud(session=mock_async_session)
    assert isinstance(metric_cache_config_crud, CRUDMetricCacheConfig)


@pytest.mark.asyncio
async def test_get_query_client(mock_insights_backend_client, mock_async_session):
    dimensions_crud = await get_dimensions_crud(session=mock_async_session)
    metric_crud = await get_metric_crud(session=mock_async_session)
    cube_client = await get_cube_client(mock_insights_backend_client)
    query_client = await get_query_client(
        cube_client=cube_client, dimensions_crud=dimensions_crud, metric_crud=metric_crud
    )
    assert isinstance(query_client, QueryClient)
    # Additional assertion to verify the cube_client in query_client
    assert query_client.cube_client == cube_client


# Test the cache CRUD instantiation


@pytest.mark.asyncio
async def test_cache_crud_creation(mock_async_session):
    """Test cache CRUD instances are created correctly."""
    cache_crud = await get_metric_cache_config_crud(session=mock_async_session)
    assert isinstance(cache_crud, CRUDMetricCacheConfig)
    assert cache_crud.session == mock_async_session


@pytest.mark.asyncio
async def test_cache_grain_crud_creation(mock_async_session):
    """Test cache grain CRUD instances are created correctly."""
    grain_crud = await get_metric_cache_grain_config_crud(session=mock_async_session)
    assert isinstance(grain_crud, CRUDMetricCacheGrainConfig)
    assert grain_crud.session == mock_async_session
