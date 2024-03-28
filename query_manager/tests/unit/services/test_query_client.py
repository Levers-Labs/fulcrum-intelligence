from unittest.mock import AsyncMock, patch

import pytest

from query_manager.services.query_client import QueryClient


@pytest.mark.asyncio
async def test_load_data():
    query_client = QueryClient()
    result = await query_client.load_data(query_client.metric_file_path)
    assert len(result) > 0


@pytest.mark.asyncio
async def test_load_data_exception():
    query_client = QueryClient()
    with patch("query_manager.services.query_client.aiofiles.open") as mock_open:
        mock_open.side_effect = FileNotFoundError
        with pytest.raises(FileNotFoundError):
            await query_client.load_data(query_client.metric_file_path)


@pytest.mark.asyncio
async def test_list_metrics(mocker, metric):
    query_client = QueryClient()
    mock_load_data = AsyncMock(return_value=[metric])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    result = await query_client.list_metrics()
    assert len(result) == 1
    assert result[0] == metric


@pytest.mark.asyncio
async def test_get_metric_details(mocker, metric):
    query_client = QueryClient()
    mock_load_data = AsyncMock(return_value=[metric])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    result = await query_client.get_metric_details(metric["id"])
    assert result == metric


@pytest.mark.asyncio
async def test_list_dimensions(mocker, dimension):
    query_client = QueryClient()
    mock_load_data = AsyncMock(return_value=[dimension])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    result = await query_client.list_dimensions()
    assert len(result) == 1
    assert result[0] == dimension


@pytest.mark.asyncio
async def test_get_dimension_details(mocker, dimension):
    query_client = QueryClient()
    mock_load_data = AsyncMock(return_value=[dimension])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    result = await query_client.get_dimension_details(dimension["id"])
    assert result == dimension


@pytest.mark.asyncio
async def test_get_dimension_members(mocker, dimension):
    query_client = QueryClient()
    mock_load_data = AsyncMock(return_value=[dimension])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    result = await query_client.get_dimension_members(dimension["id"])
    assert result == dimension["members"]
