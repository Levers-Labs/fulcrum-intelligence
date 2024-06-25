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
async def test_get_metrics_value(mocker):
    """
    Test the get_metrics_value method of QueryManagerClient.
    """
    # Arrange
    client = QueryManagerClient(base_url="https://example.com")
    metric_ids = ["metric1", "metric2"]
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 31)
    expected_values = {
        "metric1": 100,
        "metric2": 200,
    }
    get_metric_value_mock = AsyncMock(
        side_effect=[
            {"value": 100},
            {"value": 200},
        ]
    )
    mocker.patch.object(client, "get_metric_value", get_metric_value_mock)

    # Act
    result = await client.get_metrics_value(metric_ids, start_date, end_date)

    # Assert
    assert result == expected_values
    get_metric_value_mock.assert_any_call("metric1", start_date, end_date)
    get_metric_value_mock.assert_any_call("metric2", start_date, end_date)


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
async def test_get_metrics_time_series(mocker):
    # Arrange
    client = QueryManagerClient(base_url="https://example.com")
    get_metric_time_series_mock = AsyncMock(
        side_effect=[
            [{"date": "2023-01-01", "value": 100}, {"date": "2023-01-02", "value": 200}],
            [{"date": "2023-01-01", "value": 300}, {"date": "2023-01-02", "value": 400}],
        ]
    )
    mocker.patch.object(client, "get_metric_time_series", get_metric_time_series_mock)
    metric_ids = ["metric1", "metric2"]
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 31)
    grain = Granularity.DAY
    dimensions = ["dimension1", "dimension2"]

    # Act
    results = await client.get_metrics_time_series(metric_ids, start_date, end_date, grain, dimensions)

    # Assert
    assert results == [
        {"date": "2023-01-01", "value": 100},
        {"date": "2023-01-02", "value": 200},
        {"date": "2023-01-01", "value": 300},
        {"date": "2023-01-02", "value": 400},
    ]
    assert get_metric_time_series_mock.call_count == 2


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
async def test_get_metrics_time_series_df(mocker):
    """
    Test the get_metrics_time_series_df method of QueryManagerClient.
    """
    # Arrange
    client = QueryManagerClient(base_url="https://example.com")
    metric_ids = ["metric1", "metric2"]
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 31)
    grain = Granularity.DAY

    mock_response_metric1 = [
        {"date": "2023-01-01", "value": 100},
        {"date": "2023-01-02", "value": 200},
    ]
    mock_response_metric2 = [
        {"date": "2023-01-01", "value": 300},
        {"date": "2023-01-02", "value": 400},
    ]

    get_metric_time_series_mock = AsyncMock(side_effect=[mock_response_metric1, mock_response_metric2])
    mocker.patch.object(client, "get_metric_time_series", get_metric_time_series_mock)

    # Act
    result_df = await client.get_metrics_time_series_df(metric_ids, start_date, end_date, grain)

    # Assert
    expected_df = pd.DataFrame(
        {
            "date": ["2023-01-01", "2023-01-02"],
            "metric1": [100, 200],
            "metric2": [300, 400],
        }
    )
    pd.testing.assert_frame_equal(result_df, expected_df)
    assert get_metric_time_series_mock.call_count == 2
    get_metric_time_series_mock.assert_any_call("metric1", start_date, end_date, grain)
    get_metric_time_series_mock.assert_any_call("metric2", start_date, end_date, grain)


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


@pytest.mark.asyncio
async def test_get_metric_targets(mocker):
    # Arrange
    client = QueryManagerClient(base_url="https://example.com")
    mock_response = [
        {
            "metric_id": "metric1",
            "grain": "week",
            "target_date": "2021-01-01",
            "aim": "maximize",
            "target_value": 100.0,
            "target_upper_bound": 115.0,
            "target_lower_bound": 85.0,
            "yellow_buffer": 1.5,
            "red_buffer": 3.0,
        }
    ]
    get_mock = AsyncMock(return_value={"results": mock_response})
    mocker.patch.object(client, "get", get_mock)
    metric_id = "test_metric"

    # Act
    results = await client.get_metric_targets(metric_id)

    # Assert
    assert results == mock_response
    get_mock.assert_called_once_with(endpoint=f"metrics/{metric_id}/targets", params={})

    # Act
    # with time range
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 31)
    grain = Granularity.WEEK
    results = await client.get_metric_targets(metric_id, grain=grain, start_date=start_date, end_date=end_date)

    # Assert
    assert results == mock_response
    get_mock.assert_called_with(
        endpoint=f"metrics/{metric_id}/targets",
        params={
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "grain": grain.value,
        },
    )


@pytest.mark.asyncio
async def test_get_metric_values_df(mocker):
    client = QueryManagerClient(base_url="https://example.com")
    get_metric_values_mock = AsyncMock(
        return_value=[
            {"date": "2023-01-01", "value": 100},
            {"date": "2023-01-02", "value": 200},
            {"date": "2023-01-03", "value": 300},
        ]
    )
    mocker.patch.object(client, "get_metric_values", get_metric_values_mock)
    metric_ids = "metric1"
    start_date = date(2023, 1, 1)
    end_date = date(2023, 1, 31)
    dimensions = ["dimension1", "dimension2"]

    # Act
    results = await client.get_metric_values_df(metric_ids, start_date, end_date, dimensions)
    # Assert
    assert results.equals(
        pd.DataFrame(
            [
                {"date": "2023-01-01", "value": 100},
                {"date": "2023-01-02", "value": 200},
                {"date": "2023-01-03", "value": 300},
            ]
        )
    )
