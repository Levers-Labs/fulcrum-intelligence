from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from query_manager.core.dependencies import get_query_client, get_s3_client


@pytest.fixture
async def query_client():
    s3_client = await get_s3_client()
    client = await get_query_client(s3_client)
    return client


@pytest.mark.asyncio
async def test_load_data(query_client):
    query_client = await query_client
    result = await query_client.load_data(query_client.metric_file_path)
    assert len(result) > 0


@pytest.mark.asyncio
async def test_load_data_exception(query_client):
    query_client = await query_client
    with patch("query_manager.services.query_client.aiofiles.open") as mock_open:
        mock_open.side_effect = FileNotFoundError
        with pytest.raises(FileNotFoundError):
            await query_client.load_data(query_client.metric_file_path)


@pytest.mark.asyncio
async def test_list_metrics(mocker, metric, query_client):
    query_client = await query_client
    mock_load_data = AsyncMock(return_value=[metric])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    result = await query_client.list_metrics()
    assert len(result) == 1
    assert result[0] == metric


@pytest.mark.asyncio
async def test_get_metric_details(mocker, metric, query_client):
    query_client = await query_client
    mock_load_data = AsyncMock(return_value=[metric])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    result = await query_client.get_metric_details(metric["id"])
    assert result == metric


@pytest.mark.asyncio
async def test_list_dimensions(mocker, dimension, query_client):
    query_client = await query_client
    mock_load_data = AsyncMock(return_value=[dimension])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    result = await query_client.list_dimensions()
    assert len(result) == 1
    assert result[0] == dimension


@pytest.mark.asyncio
async def test_get_dimension_details(mocker, dimension, query_client):
    query_client = await query_client
    mock_load_data = AsyncMock(return_value=[dimension])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    result = await query_client.get_dimension_details(dimension["id"])
    assert result == dimension


@pytest.mark.asyncio
async def test_get_dimension_members(mocker, dimension, query_client):
    query_client = await query_client
    mock_load_data = AsyncMock(return_value=[dimension])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    result = await query_client.get_dimension_members(dimension["id"])
    assert result == dimension["members"]


@pytest.mark.asyncio
async def test_get_metric_targets_with_time_range(mocker, query_client):
    query_client = await query_client
    mock_query_s3_json = AsyncMock(return_value=[{"target": "value1"}, {"target": "value2"}])
    mocker.patch.object(query_client.s3_client, "query_s3_json", mock_query_s3_json)

    result = await query_client.get_metric_targets("metric_id", "2022-01-01", "2022-01-31")

    # Assert that the results match the mock return value
    assert result == [{"target": "value1"}, {"target": "value2"}]
    mock_query_s3_json.assert_awaited_with("mock_data/metric/metric_id/target.json", "SELECT * FROM s3object")


@pytest.mark.asyncio
async def test_get_metric_targets_without_time_range(mocker, query_client):
    query_client = await query_client
    mock_query_s3_json = AsyncMock(return_value=[{"target": "value3"}])
    mocker.patch.object(query_client.s3_client, "query_s3_json", mock_query_s3_json)

    result = await query_client.get_metric_targets("metric_id")

    # Assert that the results match the mock return value
    assert result == [{"target": "value3"}]
    mock_query_s3_json.assert_awaited_with("mock_data/metric/metric_id/target.json", "SELECT * FROM s3object")


@pytest.mark.asyncio
async def test_get_metric_values_without_dimensions(mocker, query_client):
    query_client = await query_client
    # Mock response from S3 for values without dimensions
    mock_query_s3_json = AsyncMock(return_value=[{"date": "2022-01-01", "value": 200}])
    mocker.patch.object(query_client.s3_client, "query_s3_json", mock_query_s3_json)

    result = await query_client.get_metric_values("metric_id", date(2022, 1, 1), date(2022, 1, 31), None)

    assert len(result) == 1
    assert result[0] == {"date": date(2022, 1, 1), "value": 200}
    mock_query_s3_json.assert_awaited_with("mock_data/metric/metric_id/values.json", "SELECT * FROM s3object")


@pytest.mark.asyncio
async def test_get_metric_values_with_dimensions(mocker, query_client):
    query_client = await query_client
    # Mock response from S3 for values with dimensions
    mock_query_s3_json = AsyncMock(
        return_value=[{"date": "2022-01-01", "value": 100, "dimensions": {"name": "X", "member": "Y"}}]
    )
    mocker.patch.object(query_client.s3_client, "query_s3_json", mock_query_s3_json)

    result = await query_client.get_metric_values(
        "metric_id", date(2022, 1, 1), date(2022, 1, 31), ["dimension1", "dimension2"]
    )

    assert len(result) == 1
    assert result[0] == {"date": date(2022, 1, 1), "value": 100, "dimensions": {"name": "X", "member": "Y"}}
    mock_query_s3_json.assert_awaited_with(
        "mock_data/metric/metric_id/values_with_dimensions.json", "SELECT * FROM s3object"
    )
