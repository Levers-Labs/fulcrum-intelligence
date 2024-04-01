from unittest.mock import AsyncMock

import pytest

from query_manager.core.schemas import (
    Dimension,
    DimensionDetail,
    MetricDetail,
    MetricList,
)
from query_manager.services.query_client import QueryClient


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
    assert response.json() == [MetricList(**metric).model_dump(mode="json")]


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
