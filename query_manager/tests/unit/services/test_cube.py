from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from commons.clients.auth import JWTSecretKeyAuth
from commons.clients.base import HttpClientError
from commons.models.enums import Granularity
from query_manager.core.dependencies import get_cube_client
from query_manager.core.models import (
    Dimension,
    Metric,
    SemanticMetaDimension,
    SemanticMetaMetric,
)
from query_manager.core.schemas import DimensionDetail, MetricDetail
from query_manager.exceptions import MalformedMetricMetadataError, MetricValueNotFoundError, QueryManagerError
from query_manager.services.cube import CubeClient, CubeJWTAuthType


@pytest.fixture
def insights_backend_client():
    client = MagicMock()
    client.get_tenant_config = AsyncMock(
        return_value={
            "cube_connection_config": {
                "cube_api_url": "http://test-cube-api.com",
                "cube_auth_type": "SECRET_KEY",
                "cube_auth_secret_key": "test-secret-key",
            }
        }
    )
    return client


@pytest.fixture
async def cube_client(insights_backend_client):
    return await get_cube_client(insights_backend_client)


@pytest.mark.asyncio
async def test_cube_client_init():
    # no auth
    cube_client = CubeClient("https://analytics.cube.dev/cubejs-api/v1", auth_type=None, auth_options=None)
    assert cube_client.base_url == "https://analytics.cube.dev/cubejs-api/v1"
    assert cube_client.auth is None

    # secret key auth
    cube_client = CubeClient(
        "https://analytics.cube.dev/cubejs-api/v1",
        auth_type=CubeJWTAuthType.SECRET_KEY,
        auth_options={"secret_key": "secret_key"},
    )
    assert isinstance(cube_client.auth, JWTSecretKeyAuth)


def test_validate_auth_options():
    # no auth
    cube_client = CubeClient("https://analytics.cube.dev/cubejs-api/v1", auth_type=None, auth_options=None)
    cube_client._validate_auth_options()

    # secret key auth
    cube_client = CubeClient(
        "https://analytics.cube.dev/cubejs-api/v1",
        auth_type=CubeJWTAuthType.SECRET_KEY,
        auth_options={"secret_key": "secret_key"},
    )
    cube_client._validate_auth_options()

    # missing secret key
    with pytest.raises(ValueError):
        CubeClient("https://analytics.cube.dev/cubejs-api/v1", auth_type=CubeJWTAuthType.SECRET_KEY, auth_options={})

    # invalid auth type
    with pytest.raises(ValueError):
        CubeClient("https://analytics.cube.dev/cubejs-api/v1", auth_type="invalid", auth_options={})  # type: ignore


@pytest.mark.asyncio
async def test_load_query_data_success(mocker, cube_client):
    # Prepare
    cube_client = await cube_client
    query = {"measures": ["dim_opportunity.sqo_rate"], "dimensions": [], "timeDimensions": [], "filters": []}
    # mock the post method
    post_mock = AsyncMock(return_value={"data": [{"dim_opportunity.sqo_rate": 100.0}]})
    mocker.patch.object(cube_client, "post", post_mock)

    # Execute
    response = await cube_client.load_query_data(query)

    # Assert
    assert response == [{"dim_opportunity.sqo_rate": 100.0}]
    post_mock.assert_called_once_with("load", data={"query": query})


@pytest.mark.asyncio
async def test_load_query_data_continue_wait(mocker, cube_client):
    # Prepare
    cube_client = await cube_client
    query = {"measures": ["dim_opportunity.sqo_rate"], "dimensions": [], "timeDimensions": [], "filters": []}
    # mock the post method
    post_mock = AsyncMock(side_effect=[{"error": "Continue wait"}, {"data": [{"dim_opportunity.sqo_rate": 100.0}]}])
    mocker.patch.object(cube_client, "post", post_mock)

    # Execute
    response = await cube_client.load_query_data(query)

    # Assert
    assert response == [{"dim_opportunity.sqo_rate": 100.0}]
    post_mock.assert_called_with("load", data={"query": query})
    assert post_mock.call_count == 2


@pytest.mark.asyncio
async def test_load_query_data_continue_wait_max_retries(mocker, cube_client):
    # Prepare
    cube_client = await cube_client
    query = {"measures": ["dim_opportunity.sqo_rate"], "dimensions": [], "timeDimensions": [], "filters": []}
    # mock the post method
    post_mock = AsyncMock(return_value={"error": "Continue wait"})
    mocker.patch.object(cube_client, "post", post_mock)
    mocker.patch.object(cube_client, "CONTINUE_WAIT_MAX_RETRIES", 1)

    # Execute
    with pytest.raises(HttpClientError):
        await cube_client.load_query_data(query)

    # Assert
    post_mock.assert_called_with("load", data={"query": query})
    assert post_mock.call_count == 2


def test_convert_cube_response_to_metric_values(metric):
    # Prepare
    response = [
        {
            "dim_opportunity.account_name": "Splunk Inc.",
            "dim_opportunity.sqo_date.week": "2020-09-07T00:00:00.000",
            "dim_opportunity.sqo_date": "2020-09-09T24:00:00.000",
            "dim_opportunity.sqo_rate": "100.000000",
        }
    ]
    metric = MetricDetail.parse_obj(metric)
    metric.meta_data.semantic_meta = SemanticMetaMetric(
        cube="dim_opportunity",
        member="sqo_rate",
        member_type="measure",
        time_dimension={"cube": "dim_opportunity", "member": "sqo_date"},
    )
    metric.dimensions[0].dimension_id = "account_name"
    metric.dimensions[0].meta_data.semantic_meta = SemanticMetaDimension(
        cube="dim_opportunity", member="account_name", member_type="dimension"
    )

    # Execute
    metric_values = CubeClient.convert_cube_response_to_metric_values(response, metric, grain=Granularity.WEEK)

    # Assert
    assert metric_values == [
        {"metric_id": "metric1", "value": "100.000000", "date": "2020-09-07", "account_name": "Splunk Inc."}
    ]

    # Execute
    metric_values = CubeClient.convert_cube_response_to_metric_values(response, metric, grain=None)

    # Assert
    assert metric_values == [
        {"metric_id": "metric1", "value": "100.000000", "account_name": "Splunk Inc.", "date": "2020-09-09"}
    ]


def test_convert_cube_response_to_metric_values_no_time_dimension(metric):
    # Prepare
    response = [{"dim_opportunity.sqo_rate": "100.000000"}]
    metric = MetricDetail.parse_obj(metric)
    metric.meta_data.semantic_meta = SemanticMetaMetric(
        cube="dim_opportunity",
        member="sqo_rate",
        member_type="measure",
        time_dimension={"cube": "dim_opportunity", "member": "sqo_date"},
    )

    # Execute
    metric_values = CubeClient.convert_cube_response_to_metric_values(response, metric, grain=None)

    # Assert
    assert metric_values == [{"metric_id": "metric1", "value": "100.000000"}]


def test_generate_query_for_metric_with_grain(metric):
    # Prepare
    metric = MetricDetail.parse_obj(metric)
    metric.meta_data.semantic_meta = SemanticMetaMetric(
        cube="dim_opportunity",
        member="sqo_rate",
        member_type="measure",
        time_dimension={"cube": "dim_opportunity", "member": "sqo_date"},
    )

    # Execute
    query = CubeClient.generate_query_for_metric(
        metric, grain=Granularity.WEEK, start_date=date(2021, 1, 1), end_date=date(2021, 2, 1)
    )

    # Assert
    assert query == {
        "measures": ["dim_opportunity.sqo_rate"],
        "dimensions": [],
        "timeDimensions": [
            {
                "dimension": "dim_opportunity.sqo_date",
                "granularity": "week",
                "dateRange": [date(2021, 1, 1), date(2021, 2, 1)],
            }
        ],
        "filters": [],
    }


def test_generate_query_for_metric_without_grain(metric):
    # Prepare
    metric = MetricDetail.parse_obj(metric)
    metric.meta_data.semantic_meta = SemanticMetaMetric(
        cube="dim_opportunity",
        member="sqo_rate",
        member_type="measure",
        time_dimension={"cube": "dim_opportunity", "member": "sqo_date"},
    )

    # Execute
    query = CubeClient.generate_query_for_metric(metric, start_date=date(2021, 1, 1), end_date=date(2021, 2, 1))

    # Assert
    assert query == {
        "measures": ["dim_opportunity.sqo_rate"],
        "dimensions": [],
        "timeDimensions": [],
        "filters": [
            {"dimension": "dim_opportunity.sqo_date", "operator": "gte", "values": [date(2021, 1, 1)]},
            {"dimension": "dim_opportunity.sqo_date", "operator": "lt", "values": [date(2021, 2, 1)]},
        ],
    }


def test_generate_query_for_metric_dimensions(metric, dimension):
    # Prepare
    metric = Metric.parse_obj(metric)
    metric.meta_data.semantic_meta = SemanticMetaMetric(
        cube="dim_opportunity",
        member="sqo_rate",
        member_type="measure",
        time_dimension={"cube": "dim_opportunity", "member": "sqo_date"},
    )
    metric.dimensions = [Dimension.parse_obj(dimension)]
    metric.dimensions[0].dimension_id = "account_name"
    metric.dimensions[0].meta_data.semantic_meta = SemanticMetaDimension(
        cube="dim_opportunity", member="account_name", member_type="dimension"
    )

    # Execute
    query = CubeClient.generate_query_for_metric(metric, dimensions=["account_name", "other_dimension"])

    # Assert
    assert query == {
        "measures": ["dim_opportunity.sqo_rate"],
        "dimensions": ["dim_opportunity.account_name"],
        "timeDimensions": [],
        "filters": [],
    }


@pytest.mark.asyncio
async def test_load_metric_values_from_cube(mocker, metric, cube_client):
    # Prepare
    cube_client = await cube_client
    metric = MetricDetail.parse_obj(metric)
    metric.meta_data.semantic_meta = SemanticMetaMetric(
        cube="dim_opportunity",
        member="sqo_rate",
        member_type="measure",
        time_dimension={"cube": "dim_opportunity", "member": "sqo_date"},
    )
    query = {
        "measures": ["dim_opportunity.sqo_rate"],
        "dimensions": [],
        "timeDimensions": [],
        "filters": [],
    }
    # mock the load_query_data method
    load_query_data_mock = AsyncMock(return_value=[{"dim_opportunity.sqo_rate": 100.0}])
    mocker.patch.object(cube_client, "load_query_data", load_query_data_mock)

    # Execute
    metric_values = await cube_client.load_metric_values_from_cube(metric)

    # Assert
    assert metric_values == [{"value": 100.0, "metric_id": "metric1"}]
    load_query_data_mock.assert_called_once_with(query)


@pytest.mark.asyncio
async def test_load_metric_values_from_cube_error(mocker, metric, cube_client):
    # Prepare
    cube_client = await cube_client
    metric = MetricDetail.parse_obj(metric)
    metric.meta_data.semantic_meta = SemanticMetaMetric(
        cube="dim_opportunity",
        member="sqo_rate",
        member_type="measure",
        time_dimension={"cube": "dim_opportunity", "member": "sqo_date"},
    )
    # mock the load_query_data method
    load_query_data_mock = AsyncMock(side_effect=HttpClientError("Error"))
    mocker.patch.object(cube_client, "load_query_data", load_query_data_mock)

    # Execute
    with pytest.raises(MalformedMetricMetadataError):
        await cube_client.load_metric_values_from_cube(metric)
    load_query_data_mock.assert_called_once()

    # Prepare
    load_query_data_mock = AsyncMock(return_value=[])
    mocker.patch.object(cube_client, "load_query_data", load_query_data_mock)

    # Execute/Assert
    with pytest.raises(MetricValueNotFoundError):
        await cube_client.load_metric_values_from_cube(metric)


@pytest.mark.asyncio
async def test_load_metric_targets_from_cube(mocker, metric, cube_client):
    # Prepare
    cube_client = await cube_client  # Await the cube_client fixture
    metric = MetricDetail.parse_obj(metric)
    grain = Granularity.WEEK
    start_date = date(2021, 1, 1)
    end_date = date(2021, 2, 1)
    query = {
        "dimensions": [
            "metric_targets.metric_id",
            "metric_targets.grain",
            "metric_targets.target_date",
            "metric_targets.target_value",
            "metric_targets.aim",
            "metric_targets.yellow_buffer",
            "metric_targets.red_buffer",
        ],
        "filters": [
            {"member": "metric_targets.metric_id", "operator": "equals", "values": [metric.metric_id]},
            {"member": "metric_targets.grain", "operator": "equals", "values": [grain.value]},
        ],
        "timeDimensions": [
            {
                "dimension": "metric_targets.target_date",
                "dateRange": [start_date, end_date],
            }
        ],
    }
    cube_response = [
        {
            "metric_targets.metric_id": "metric1",
            "metric_targets.grain": "week",
            "metric_targets.target_value": 100.0,
            "metric_targets.target_date": "2021-01-01",
            "metric_targets.aim": "maximize",
            "metric_targets.yellow_buffer": 1.5,
            "metric_targets.red_buffer": 3.0,
        }
    ]
    exp_target_values = [
        {
            "metric_id": "metric1",
            "grain": "week",
            "target_date": "2021-01-01",
            "aim": "maximize",
            "target_value": 100.0,
            "yellow_buffer": 1.5,
            "red_buffer": 3.0,
        }
    ]
    # mock the load_query_data method
    load_query_data_mock = AsyncMock(return_value=cube_response)
    mocker.patch.object(cube_client, "load_query_data", load_query_data_mock)

    # Execute
    target_values = await cube_client.load_metric_targets_from_cube(
        metric, grain=grain, start_date=start_date, end_date=end_date
    )

    # Assert
    assert target_values == exp_target_values
    load_query_data_mock.assert_called_once_with(query)


@pytest.mark.asyncio
async def test_load_metric_targets_from_cube_error(mocker, metric, cube_client):
    # Prepare
    cube_client = await cube_client
    metric = MetricDetail.parse_obj(metric)
    grain = Granularity.WEEK
    start_date = date(2021, 1, 1)
    end_date = date(2021, 2, 1)
    # mock the load_query_data method
    load_query_data_mock = AsyncMock(side_effect=HttpClientError("Error"))
    mocker.patch.object(cube_client, "load_query_data", load_query_data_mock)

    # Execute
    with pytest.raises(QueryManagerError):
        await cube_client.load_metric_targets_from_cube(metric, grain=grain, start_date=start_date, end_date=end_date)


@pytest.mark.asyncio
async def test_load_dimension_members_from_cube(mocker, dimension, cube_client):
    # Prepare
    cube_client = await cube_client
    dimension = DimensionDetail.model_validate(dimension)
    query = {"dimensions": ["cube1.billing_plan"]}
    cube_response = [{"cube1.billing_plan": "Enterprise"}, {"cube1.billing_plan": "Partnership"}]
    # mock the load_query_data method
    load_query_data_mock = AsyncMock(return_value=cube_response)
    mocker.patch.object(cube_client, "load_query_data", load_query_data_mock)

    # Execute
    dimension_members = await cube_client.load_dimension_members_from_cube(dimension)

    # Assert
    assert dimension_members == ["Enterprise", "Partnership"]
    load_query_data_mock.assert_called_once_with(query)


@pytest.mark.asyncio
async def test_load_dimension_members_from_cube_error(mocker, dimension, cube_client):
    # Prepare
    cube_client = await cube_client
    dimension = DimensionDetail.model_validate(dimension)
    # mock the load_query_data method
    load_query_data_mock = AsyncMock(side_effect=HttpClientError("Error"))
    mocker.patch.object(cube_client, "load_query_data", load_query_data_mock)

    # Execute
    members = await cube_client.load_dimension_members_from_cube(dimension)

    # Assert
    assert members == []
    load_query_data_mock.assert_called_once()


@pytest.mark.asyncio
async def test_list_cubes(mocker, cube_client):
    # Prepare
    cube_client = await cube_client
    mock_response = {
        "cubes": [
            {
                "name": "cube1",
                "title": "Cube One",
                "measures": [
                    {
                        "name": "revenue",
                        "title": "Total Revenue",
                        "shortTitle": "Revenue",
                        "type": "number",
                        "description": "Total revenue from all sources",
                    }
                ],
                "dimensions": [
                    {
                        "name": "created_at",
                        "title": "Created Date",
                        "shortTitle": "Created",
                        "type": "time",
                        "description": "Date when record was created",
                    }
                ],
            }
        ]
    }

    # Mock the get method
    get_mock = AsyncMock(return_value=mock_response)
    mocker.patch.object(cube_client, "get", get_mock)

    # Execute
    cubes = await cube_client.list_cubes()

    # Assert
    assert len(cubes) == 1
    assert cubes[0]["name"] == "cube1"
    assert cubes[0]["title"] == "Cube One"
    assert len(cubes[0]["measures"]) == 1
    assert cubes[0]["measures"][0]["name"] == "revenue"
    assert len(cubes[0]["dimensions"]) == 1
    assert cubes[0]["dimensions"][0]["name"] == "created_at"
    get_mock.assert_called_once_with("/meta")


@pytest.mark.asyncio
async def test_list_cubes_error(mocker, cube_client):
    # Prepare
    cube_client = await cube_client

    # Mock the get method to raise error
    get_mock = AsyncMock(side_effect=HttpClientError("Error"))
    mocker.patch.object(cube_client, "get", get_mock)

    # Execute and Assert
    with pytest.raises(HttpClientError):
        await cube_client.list_cubes()
    get_mock.assert_called_once_with("/meta")


@pytest.mark.asyncio
async def test_list_cubes_empty(mocker, cube_client):
    # Prepare
    cube_client = await cube_client
    mock_response = {"cubes": []}

    # Mock the get method
    get_mock = AsyncMock(return_value=mock_response)
    mocker.patch.object(cube_client, "get", get_mock)

    # Execute
    cubes = await cube_client.list_cubes()

    # Assert
    assert len(cubes) == 0
    get_mock.assert_called_once_with("/meta")
