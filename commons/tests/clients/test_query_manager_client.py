from datetime import date
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from commons.clients.query_manager import QueryManagerClient
from commons.models.enums import Granularity


@pytest.mark.asyncio
async def test_get_metric_value(mocker):
    # Arrange
    client = QueryManagerClient(base_url="https://example.com")
    get_metric_values_mock = AsyncMock(return_value=[{"value": 100}])
    mocker.patch.object(client, "get_metric_values", get_metric_values_mock)
    metric_id = "test_metric"

    # Act
    result = await client.get_metric_value(metric_id)

    # Assert
    assert result == {"value": 100}
    get_metric_values_mock.assert_called_once_with(metric_id=metric_id)

    # Act
    # with date range
    result = await client.get_metric_value(metric_id, start_date=date(2023, 1, 1), end_date=date(2023, 1, 31))

    # Assert
    assert result == {"value": 100}
    get_metric_values_mock.assert_called_with(
        metric_id=metric_id, start_date=date(2023, 1, 1), end_date=date(2023, 1, 31)
    )


@pytest.mark.asyncio
async def test_get_metric_values(mocker):
    # Arrange
    client = QueryManagerClient(base_url="https://example.com")
    mock_response = {"data": [{"value": 100}, {"value": 200}]}
    post_mock = AsyncMock(return_value=mock_response)
    mocker.patch.object(client, "post", post_mock)
    metric_id = "test_metric"
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 31)
    dimensions = ["dimension1", "dimension2"]

    # Act
    results = await client.get_metric_values(metric_id, start_date, end_date, dimensions)

    # Assert
    assert results == [{"value": 100}, {"value": 200}]
    post_mock.assert_called_once_with(
        endpoint=f"metrics/{metric_id}/values",
        data={
            "start_date": "2023-01-01",
            "end_date": "2023-01-31",
            "dimensions": dimensions,
        },
    )


@pytest.mark.asyncio
async def test_get_metrics_values(mocker):
    # Arrange
    client = QueryManagerClient(base_url="https://example.com")
    get_metric_values_mock = AsyncMock(
        side_effect=[
            [{"value": 100}, {"value": 200}],
            [{"value": 300}, {"value": 400}],
        ]
    )
    mocker.patch.object(client, "get_metric_values", get_metric_values_mock)
    metric_ids = ["metric1", "metric2"]
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 31)
    dimensions = ["dimension1", "dimension2"]

    # Act
    results = await client.get_metrics_values(metric_ids, start_date, end_date, dimensions)

    # Assert
    assert results == [
        {"value": 100},
        {"value": 200},
        {"value": 300},
        {"value": 400},
    ]
    assert get_metric_values_mock.call_count == 2


@pytest.mark.asyncio
async def test_get_metric_time_series(mocker):
    # Arrange
    client = QueryManagerClient(base_url="https://example.com")
    mock_response = {"data": [{"date": "2023-01-01", "value": 100}, {"date": "2023-01-02", "value": 200}]}
    post_mock = AsyncMock(return_value=mock_response)
    mocker.patch.object(client, "post", post_mock)
    metric_id = "test_metric"
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 31)
    grain = Granularity.DAY
    dimensions = ["dimension1", "dimension2"]

    # Act
    results = await client.get_metric_time_series(metric_id, start_date, end_date, grain, dimensions)

    # Assert
    assert results == [{"date": "2023-01-01", "value": 100}, {"date": "2023-01-02", "value": 200}]
    post_mock.assert_called_once_with(
        endpoint=f"metrics/{metric_id}/values",
        data={
            "start_date": "2023-01-01",
            "end_date": "2023-01-31",
            "dimensions": dimensions,
            "grain": grain.value,
        },
    )


@pytest.mark.asyncio
async def test_get_metric_time_series_df(mocker):
    # Arrange
    client = QueryManagerClient(base_url="https://example.com")
    mock_response = [{"date": "2023-01-01", "value": 100}, {"date": "2023-01-02", "value": 200}]
    get_metric_time_series_mock = AsyncMock(return_value=mock_response)
    mocker.patch.object(client, "get_metric_time_series", get_metric_time_series_mock)
    metric_id = "test_metric"
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 31)
    grain = Granularity.DAY
    dimensions = ["dimension1", "dimension2"]

    # Act
    result_df = await client.get_metric_time_series_df(metric_id, start_date, end_date, grain, dimensions)

    # Assert
    expected_df = pd.DataFrame(mock_response)
    pd.testing.assert_frame_equal(result_df, expected_df)
    get_metric_time_series_mock.assert_called_once_with(metric_id, start_date, end_date, grain, dimensions)


@pytest.mark.asyncio
async def test_get_metric(mocker):
    # Arrange
    client = QueryManagerClient(base_url="https://example.com")
    mock_response = {"id": "test_metric", "name": "Test Metric"}
    get_mock = AsyncMock(return_value=mock_response)
    mocker.patch.object(client, "get", get_mock)
    metric_id = "test_metric"

    # Act
    result = await client.get_metric(metric_id)

    # Assert
    assert result == {"id": "test_metric", "name": "Test Metric"}
    get_mock.assert_called_once_with(endpoint=f"metrics/{metric_id}")


@pytest.mark.asyncio
async def test_list_metrics(mocker):
    # Arrange
    client = QueryManagerClient(base_url="https://example.com")
    mock_response = {"results": [{"id": "metric1"}, {"id": "metric2"}]}
    get_mock = AsyncMock(return_value=mock_response)
    mocker.patch.object(client, "get", get_mock)
    metric_ids = ["metric1", "metric2"]

    # Act
    results = await client.list_metrics(metric_ids)

    # Assert
    assert results == [{"id": "metric1"}, {"id": "metric2"}]
    get_mock.assert_called_once_with(endpoint="metrics", params={"metric_ids": metric_ids})
