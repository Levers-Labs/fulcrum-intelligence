from datetime import date
from unittest.mock import AsyncMock

import pytest

from commons.clients.auth import JWTAuth, JWTSecretKeyAuth
from commons.clients.base import HttpClientError
from commons.models.enums import Granularity
from query_manager.core.dependencies import get_cube_client
from query_manager.core.schemas import MetricDetail, SemanticMetaDimension, SemanticMetaMetric
from query_manager.exceptions import MalformedMetricMetadataError, MetricValueNotFoundError
from query_manager.services.cube import CubeClient, CubeJWTAuthType


@pytest.fixture
async def cube_client() -> CubeClient:
    client = await get_cube_client()
    return client


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

    # bearer token auth
    cube_client = CubeClient(
        "https://analytics.cube.dev/cubejs-api/v1",
        auth_type=CubeJWTAuthType.TOKEN,
        auth_options={"token": "SampleToken"},
    )

    assert isinstance(cube_client.auth, JWTAuth)


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

    # bearer token auth
    cube_client = CubeClient(
        "https://analytics.cube.dev/cubejs-api/v1",
        auth_type=CubeJWTAuthType.TOKEN,
        auth_options={"token": "SampleToken"},
    )
    cube_client._validate_auth_options()

    # missing secret key
    with pytest.raises(ValueError):
        CubeClient("https://analytics.cube.dev/cubejs-api/v1", auth_type=CubeJWTAuthType.SECRET_KEY, auth_options={})

    # missing token
    with pytest.raises(ValueError):
        CubeClient("https://analytics.cube.dev/cubejs-api/v1", auth_type=CubeJWTAuthType.TOKEN, auth_options={})

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
    metric.metadata.semantic_meta = SemanticMetaMetric(
        cube="dim_opportunity",
        member="sqo_rate",
        member_type="measure",
        time_dimension={"cube": "dim_opportunity", "member": "sqo_date"},
    )
    metric.dimensions[0].id = "account_name"
    metric.dimensions[0].metadata.semantic_meta = SemanticMetaDimension(
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
    metric.metadata.semantic_meta = SemanticMetaMetric(
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
    metric.metadata.semantic_meta = SemanticMetaMetric(
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
    metric.metadata.semantic_meta = SemanticMetaMetric(
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


def test_generate_query_for_metric_dimensions(metric):
    # Prepare
    metric = MetricDetail.parse_obj(metric)
    metric.metadata.semantic_meta = SemanticMetaMetric(
        cube="dim_opportunity",
        member="sqo_rate",
        member_type="measure",
        time_dimension={"cube": "dim_opportunity", "member": "sqo_date"},
    )
    metric.dimensions[0].id = "account_name"
    metric.dimensions[0].metadata.semantic_meta = SemanticMetaDimension(
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
    metric.metadata.semantic_meta = SemanticMetaMetric(
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
    metric.metadata.semantic_meta = SemanticMetaMetric(
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
