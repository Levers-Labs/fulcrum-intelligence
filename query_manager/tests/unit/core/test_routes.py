from unittest import mock
from unittest.mock import AsyncMock

import pytest

from query_manager.core.enums import TargetAim
from query_manager.core.models import Metric
from query_manager.core.schemas import (
    DimensionCompact,
    DimensionDetail,
    MetricDetail,
    MetricList,
)
from query_manager.exceptions import MetricNotFoundError
from query_manager.services.parquet import ParquetService
from query_manager.services.query_client import QueryClient
from query_manager.services.s3 import NoSuchKeyError


class MockSecurity:
    def __init__(self, *args, **kwargs):
        self.dependency = lambda: True
        self.use_cache = False


mock.patch("fastapi.Security", MockSecurity).start()


def test_health(client):
    response = client.get("/v1/health")
    res = response.json()
    assert response.status_code == 200
    assert res["graph_api_is_online"]
    assert res["cube_api_is_online"]


@pytest.mark.asyncio
async def test_list_metrics(client, mocker, metric):
    # Mock the QueryClient's list_metrics method
    mock_list_metrics = AsyncMock(return_value=([Metric.parse_obj(metric)], 10))
    mocker.patch.object(QueryClient, "list_metrics", mock_list_metrics)

    response = client.get(f"/v1/metrics?metric_ids={mock_list_metrics}")
    assert response.status_code == 200
    assert response.json() == {
        "count": 10,
        "limit": 10,
        "offset": 0,
        "pages": 1,
        "results": [MetricList(**metric).model_dump(mode="json")],
    }


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
    mock_list_dimensions = AsyncMock(return_value=([dimension], 10))
    mocker.patch.object(QueryClient, "list_dimensions", mock_list_dimensions)

    response = client.get("/v1/dimensions")
    assert response.status_code == 200
    assert response.json() == {
        "count": 10,
        "limit": 10,
        "offset": 0,
        "pages": 1,
        "results": [DimensionCompact(**dimension).model_dump(mode="json")],
    }


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
    members_list = ["Enterprise", "Basic"]
    mock_get_dimension_members = AsyncMock(return_value=members_list)
    mocker.patch.object(QueryClient, "get_dimension_members", mock_get_dimension_members)

    dimension_id = dimension["id"]
    response = client.get(f"/v1/dimensions/{dimension_id}/members")
    assert response.status_code == 200
    assert response.json() == members_list


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
    target_values = [
        {
            "metric_id": "test_metric",
            "grain": "week",
            "aim": TargetAim.MAXIMIZE,
            "target_value": 123,
            "target_date": "2022-01-01",
            "target_upper_bound": None,
            "target_lower_bound": None,
            "yellow_buffer": None,
            "red_buffer": None,
        }
    ]
    mock_get_metric_targets = AsyncMock(return_value=target_values)
    mocker.patch.object(QueryClient, "get_metric_targets", mock_get_metric_targets)

    response = client.get("/v1/metrics/test_metric/targets?start_date=2022-01-01&end_date=2022-01-31&grain=week")

    assert response.status_code == 200
    assert response.json() == {"results": target_values, "url": None}
    mock_get_metric_targets.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_metric_targets_404(client, mocker):
    # Mock the QueryClient's get_metric_targets method
    mock_get_metric_targets = AsyncMock(side_effect=MetricNotFoundError("test_metric"))
    mocker.patch.object(QueryClient, "get_metric_targets", mock_get_metric_targets)

    response = client.get("/v1/metrics/test_metric/targets?start_date=2022-01-01&end_date=2022-01-31")
    assert response.status_code == 404
    assert response.json()["error"] == "metric_not_found"
    mock_get_metric_targets.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_metric_targets_parquet(client, mocker):
    # Mock dependencies
    result = {
        "results": None,
        "url": "https://fulcrum-metrics-pq.s3.amazonaws.com/targets/metrics/NewBizDeals/NewBizDeals_70fbe643-d0ad-4ff1-aaa4-2ecee3e56ddd.parquet?AWSAccessKeyId=ASIASSZID53AJMCQYVH6&Signature=5oTidSq2mLn0pLDTRB7y9bDFvM0%3D&x-amz-security-token=IQoJb3JpZ2luX2VjELr%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIQD6x9UyBJgOs8uI7ODczuOl3B2KHy4DYEqlr%2BdT05cs%2FQIgKQQ0tXX0nFsSjIUqQrNJzC%2FEbmiFFHg2Fx0xPluyqKIq%2BgIIQxAAGgwxNzc3ODg0MTU2ODAiDO83Xb4GSmi5Tq07HyrXAh2pc1lEdZC%2FqIDDqff8YGwFhE1jU%2Bs1uTCgMD5IpxoF8nEIvXVNSL44uQ6xrQd5EP%2FBWH%2F68wplUum2gkMy6g88XyNgCxQ3j5LiGiblqyK3GozPYLIdCxE1%2FQarcw1PBN4NjenEG8pPt7DdUVItr5lm5VAv%2BH1jMHrTHN1uO5OgbdTpCk%2BzIoya5UTCDYOXxuYIN3PFbIf4sZsxl4MWgoHEwI3N2dOT%2B0RESmyXfn%2FXq3IVwFQ7%2Bp6rFce0Iyvr7gq%2BC6atzZa%2B61e5uOrkpUiCU0CyFYu64Gf%2B9bcoj50lBAedFq1AckQLTGTPPYMNed9lTnYe8rF6AAsqi57BR04fMyiRI4OLNxAGgA4qeTo7es8dKDAxHK14LP6jvXVr4nt10Z1EjFS9pI5vTHN8cq2Ko%2Bn7pHvMb2OAYV7OMQT31ZwclgFt%2FJdOGW689Tcj%2FF12WQYsoSgwya3msgY6pwFoS9yUGltHK%2F6dqpqRdr5VdDWBFAvV98w5xsTnKVCk9S%2F19s5aWqj7jshYWetF8aYUovrRjOOFgClTCJMn6Zbf2murzt56GGHz65V1p6Tv15PPV8WmuHw3Wp6QiKBq5pFh5mfeoQWPFDQgFNWlR825Y3pDT8Vie%2FH%2FoKzPshY4ng6tw3J5PuzoRdrJXfYY42YdYcfkn9%2F0lNBtBgqwLCdaB2rNkdV6bQ%3D%3D&Expires=1717152017",
    }
    mock_get_metric_targets = AsyncMock(return_value=result)
    mocker.patch.object(QueryClient, "get_metric_targets", mock_get_metric_targets)

    mock_convert_and_upload = AsyncMock(return_value=result["url"])
    mocker.patch.object(ParquetService, "convert_and_upload", mock_convert_and_upload)

    response = client.get(
        "/v1/metrics/test_metric/targets?start_date=2022-01-01&end_date=2022-01-31&output_format=PARQUET",
    )
    assert response.status_code == 200
    assert response.json() == {
        "results": None,
        "url": result["url"],
    }
    mock_get_metric_targets.assert_awaited_once()
    mock_convert_and_upload.assert_awaited_once()
