from unittest.mock import AsyncMock

import pytest

from query_manager.core.enums import TargetAim
from query_manager.core.schemas import (
    Dimension,
    DimensionDetail,
    MetricDetail,
    MetricList,
)
from query_manager.services.parquet import ParquetService
from query_manager.services.query_client import QueryClient
from query_manager.services.s3 import NoSuchKeyError


def test_health(client):
    response = client.get("/v1/health")
    res = response.json()
    assert response.status_code == 200
    assert res["graph_api_is_online"]
    assert res["cube_api_is_online"]


@pytest.mark.asyncio
async def test_list_metrics(client, mocker, metric):
    # Mock the QueryClient's list_metrics method
    mock_list_metrics = AsyncMock(return_value=[metric])
    mocker.patch.object(QueryClient, "list_metrics", mock_list_metrics)

    response = client.get("/v1/metrics")
    assert response.status_code == 200
    assert response.json() == {"results": [MetricList(**metric).model_dump(mode="json")]}


@pytest.mark.asyncio
async def test_get_metric(client, mocker, metric):
    # Mock the QueryClient's get_metric_details method
    mock_get_metric_details = AsyncMock(return_value=metric)
    mocker.patch.object(QueryClient, "get_metric_details", mock_get_metric_details)

    metric_id = metric["id"]
    response = client.get(f"/v1/metrics/{metric_id}")
    assert response.status_code == 200
    assert response.json() == MetricDetail(**metric).model_dump(mode="json")


@pytest.mark.asyncio
async def test_list_dimensions(client, mocker, dimension):
    # Mock the QueryClient's list_dimensions method
    mock_list_dimensions = AsyncMock(return_value=[dimension])
    mocker.patch.object(QueryClient, "list_dimensions", mock_list_dimensions)

    response = client.get("/v1/dimensions")
    assert response.status_code == 200
    assert response.json() == [Dimension(**dimension).model_dump(mode="json")]


@pytest.mark.asyncio
async def test_get_dimension(client, mocker, dimension):
    # Mock the QueryClient's get_dimension_details method
    mock_get_dimension_details = AsyncMock(return_value=dimension)
    mocker.patch.object(QueryClient, "get_dimension_details", mock_get_dimension_details)

    dimension_id = dimension["id"]
    response = client.get(f"/v1/dimensions/{dimension_id}")
    assert response.status_code == 200
    assert response.json() == DimensionDetail(**dimension).model_dump(mode="json")


@pytest.mark.asyncio
async def test_get_dimension_members(client, mocker, dimension):
    # Mock the QueryClient's get_dimension_members method
    mock_get_dimension_members = AsyncMock(return_value=dimension["members"])
    mocker.patch.object(QueryClient, "get_dimension_members", mock_get_dimension_members)

    dimension_id = dimension["id"]
    response = client.get(f"/v1/dimensions/{dimension_id}/members")
    assert response.status_code == 200
    assert response.json() == dimension["members"]


@pytest.mark.asyncio
async def test_get_metric_values(client, mocker):
    # Mock dependencies
    mock_get_metric_values = AsyncMock(return_value=[{"date": "2022-01-01", "value": 100, "metric_id": "CAC"}])
    mocker.patch.object(QueryClient, "get_metric_values", mock_get_metric_values)

    response = client.post(
        "/v1/metrics/test_metric/values", json={"start_date": "2022-01-01", "end_date": "2022-01-31"}
    )

    assert response.status_code == 200
    assert response.json() == {
        "data": [
            {
                "date": "2022-01-01",
                "value": 100,
                "metric_id": "CAC",
            },
        ],
        "url": None,
    }
    mock_get_metric_values.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_metric_values_parquet(client, mocker):
    # Mock dependencies
    mock_get_metric_values = AsyncMock(return_value=[{"date": "2022-01-01", "value": 100}])
    mocker.patch.object(QueryClient, "get_metric_values", mock_get_metric_values)

    mock_convert_and_upload = AsyncMock(return_value="http://file.parquet")
    mocker.patch.object(ParquetService, "convert_and_upload", mock_convert_and_upload)

    response = client.post(
        "/v1/metrics/test_metric/values",
        json={"start_date": "2022-01-01", "end_date": "2022-01-31", "output_format": "PARQUET"},
    )
    assert response.status_code == 200
    assert response.json() == {
        "data": None,
        "url": "http://file.parquet",
    }
    mock_get_metric_values.assert_awaited_once()
    mock_convert_and_upload.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_metric_values_404(client, mocker):
    # Mock the QueryClient's get_metric_values method
    mock_get_metric_values = AsyncMock(side_effect=NoSuchKeyError(key="test_metric"))
    mocker.patch.object(QueryClient, "get_metric_values", mock_get_metric_values)

    response = client.post(
        "/v1/metrics/test_metric/values", json={"start_date": "2022-01-01", "end_date": "2022-01-31"}
    )
    assert response.status_code == 404
    assert response.json()["error"] == "metric_not_found"
    mock_get_metric_values.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_metric_targets(mocker, client):
    # Mock dependencies
    mock_get_metric_targets = AsyncMock(
        return_value=[
            {"id": "test_metric", "aim": TargetAim.MAXIMIZE, "target_value": 123, "target_date": "2022-01-01"}
        ]
    )
    mocker.patch.object(QueryClient, "get_metric_targets", mock_get_metric_targets)

    response = client.get("/v1/metrics/test_metric/targets?start_date=2022-01-01&end_date=2022-01-31")

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": "test_metric",
            "aim": "Maximize",
            "target_value": 123,
            "target_date": "2022-01-01",
            "target_upper_bound": None,
            "target_lower_bound": None,
            "yellow_buffer": None,
            "red_buffer": None,
        },
    ]
    mock_get_metric_targets.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_metric_targets_404(client, mocker):
    # Mock the QueryClient's get_metric_targets method
    mock_get_metric_targets = AsyncMock(side_effect=NoSuchKeyError(key="test_metric"))
    mocker.patch.object(QueryClient, "get_metric_targets", mock_get_metric_targets)

    response = client.get("/v1/metrics/test_metric/targets?start_date=2022-01-01&end_date=2022-01-31")
    assert response.status_code == 404
    assert response.json()["error"] == "metric_not_found"
    mock_get_metric_targets.assert_awaited_once()
