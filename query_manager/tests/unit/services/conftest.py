from unittest.mock import AsyncMock

import pytest

from query_manager.services.parquet import ParquetService
from query_manager.services.query_client import QueryClient


@pytest.fixture
async def mock_s3_client():
    mock_client = AsyncMock()
    mock_client.upload_to_cloud_storage = AsyncMock()
    mock_client.generate_presigned_url = AsyncMock(return_value="http://mocked-presigned-url.com")
    return mock_client


@pytest.fixture
async def mock_supabase_client():
    mock_client = AsyncMock()
    mock_client.upload_to_cloud_storage = AsyncMock()
    mock_client.generate_presigned_url = AsyncMock(return_value="http://mocked-presigned-url.com")
    return mock_client


@pytest.fixture
async def parquet_service(mock_s3_client, mock_supabase_client):
    s3_client_mock = await mock_s3_client
    return ParquetService(s3_client_mock)


@pytest.fixture
async def query_client(mock_s3_client):
    mock_s3_client = await mock_s3_client
    return QueryClient(mock_s3_client)
