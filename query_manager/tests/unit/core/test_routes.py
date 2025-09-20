from unittest.mock import ANY, AsyncMock

import pytest
from httpx import AsyncClient
from sqlalchemy.exc import IntegrityError

from commons.clients.base import HttpClientError
from commons.db.crud import NotFoundError
from commons.llm.exceptions import LLMError
from query_manager.core.models import Metric
from query_manager.core.schemas import (
    DimensionCompact,
    DimensionDetail,
    MetricDetail,
    MetricList,
)
from query_manager.exceptions import DimensionNotFoundError, MetricNotFoundError
from query_manager.services.cube import CubeClient
from query_manager.services.parquet import ParquetService
from query_manager.services.query_client import QueryClient
from query_manager.services.s3 import NoSuchKeyError


@pytest.mark.asyncio
async def test_health(async_client: AsyncClient):
    # Act
    response = await async_client.get("/v1/health")
    # Assert
    assert response.status_code == 200
    res = response.json()
    assert res["graph_api_is_online"]
    assert res["cube_api_is_online"]


@pytest.mark.asyncio
async def test_list_metrics(async_client: AsyncClient, mocker, metric):
    mock_list_metrics = AsyncMock(return_value=([Metric.parse_obj(metric)], 10))
    mocker.patch.object(QueryClient, "list_metrics", mock_list_metrics)

    response = await async_client.get("/v1/metrics")

    assert response.status_code == 200
    assert response.json() == {
        "count": 10,
        "limit": 10,
        "offset": 0,
        "pages": 1,
        "results": [MetricList(**metric).model_dump(mode="json")],
    }


@pytest.mark.asyncio
async def test_get_metric(async_client: AsyncClient, mocker, metric):
    mock_get_metric_details = AsyncMock(return_value=metric)
    mocker.patch.object(QueryClient, "get_metric_details", mock_get_metric_details)

    metric_id = metric["id"]
    response = await async_client.get(f"/v1/metrics/{metric_id}")
    assert response.status_code == 200
    assert response.json() == MetricDetail(**metric).model_dump(mode="json")


@pytest.mark.asyncio
async def test_list_dimensions(async_client: AsyncClient, mocker, dimension):
    mock_list_dimensions = AsyncMock(return_value=([dimension], 10))
    mocker.patch.object(QueryClient, "list_dimensions", mock_list_dimensions)

    response = await async_client.get("/v1/dimensions")
    assert response.status_code == 200
    assert response.json() == {
        "count": 10,
        "limit": 10,
        "offset": 0,
        "pages": 1,
        "results": [DimensionCompact(**dimension).model_dump(mode="json")],
    }


@pytest.mark.asyncio
async def test_get_dimension(async_client: AsyncClient, mocker, dimension):
    mock_get_dimension_details = AsyncMock(return_value=dimension)
    mocker.patch.object(QueryClient, "get_dimension_details", mock_get_dimension_details)

    dimension_id = dimension["id"]
    response = await async_client.get(f"/v1/dimensions/{dimension_id}")
    assert response.status_code == 200
    assert response.json() == DimensionDetail(**dimension).model_dump(mode="json")


@pytest.mark.asyncio
async def test_get_dimension_members(async_client: AsyncClient, mocker, dimension):
    members_list = ["Enterprise", "Basic"]
    mock_get_dimension_members = AsyncMock(return_value=members_list)
    mocker.patch.object(QueryClient, "get_dimension_members", mock_get_dimension_members)

    dimension_id = dimension["id"]
    response = await async_client.get(f"/v1/dimensions/{dimension_id}/members")
    assert response.status_code == 200
    assert response.json() == members_list


@pytest.mark.asyncio
async def test_get_metric_values(async_client: AsyncClient, mocker):
    mock_get_metric_values = AsyncMock(return_value=[{"date": "2022-01-01", "value": 100, "metric_id": "CAC"}])
    mocker.patch.object(QueryClient, "get_metric_values", mock_get_metric_values)

    response = await async_client.post(
        "/v1/metrics/test_metric/values",
        json={"start_date": "2022-01-01", "end_date": "2022-01-31"},
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
async def test_get_metric_values_parquet(async_client: AsyncClient, mocker):
    mock_get_metric_values = AsyncMock(return_value=[{"date": "2022-01-01", "value": 100}])
    mocker.patch.object(QueryClient, "get_metric_values", mock_get_metric_values)

    mock_convert_and_upload = AsyncMock(return_value="http://file.parquet")
    mocker.patch.object(ParquetService, "convert_and_upload", mock_convert_and_upload)

    response = await async_client.post(
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
async def test_get_metric_values_404(async_client: AsyncClient, mocker):
    mock_get_metric_values = AsyncMock(side_effect=NoSuchKeyError(key="test_metric"))
    mocker.patch.object(QueryClient, "get_metric_values", mock_get_metric_values)

    response = await async_client.post(
        "/v1/metrics/test_metric/values",
        json={"start_date": "2022-01-01", "end_date": "2022-01-31"},
    )
    assert response.status_code == 404
    assert response.json()["error"] == "metric_not_found"
    mock_get_metric_values.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_metric_targets(mocker, async_client: AsyncClient):
    target_values = [
        {
            "metric_id": "test_metric",
            "grain": "week",
            "target_value": 123.0,
            "target_date": "2022-01-01",
            "target_lower_bound": None,
            "target_upper_bound": None,
            "yellow_buffer": None,
            "red_buffer": None,
        }
    ]
    mock_get_metric_targets = AsyncMock(return_value=target_values)
    mocker.patch.object(QueryClient, "get_metric_targets", mock_get_metric_targets)

    response = await async_client.get(
        "/v1/metrics/test_metric/targets?start_date=2022-01-01&end_date=2022-01-31&grain=week"
    )

    assert response.status_code == 200
    assert response.json() == {"results": target_values, "url": None}
    mock_get_metric_targets.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_metric_targets_404(async_client: AsyncClient, mocker):
    mock_get_metric_targets = AsyncMock(side_effect=MetricNotFoundError("test_metric"))
    mocker.patch.object(QueryClient, "get_metric_targets", mock_get_metric_targets)

    response = await async_client.get("/v1/metrics/test_metric/targets?start_date=2022-01-01&end_date=2022-01-31")
    assert response.status_code == 404
    assert response.json()["error"] == "metric_not_found"
    mock_get_metric_targets.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_metric_targets_parquet(async_client: AsyncClient, mocker):
    result = {
        "results": None,
        "url": "https://fulcrum-metrics-pq.s3.amazonaws.com/targets/metrics/NewBizDeals/NewBizDeals_70fbe643-d0ad"
        "-4ff1-aaa4-2ecee3e56ddd.parquet?AWSAccessKeyId=ASIASSZID53AJMCQYVH6&Signature"
        "=5oTidSq2mLn0pLDTRB7y9bDFvM0%3D&x-amz-security-token=IQoJb3JpZ2luX2VjELr%2F%2F%2F%2F%2F%2F%2F%2F%2F"
        "%2FwEaCXVzLWVhc3QtMSJHMEUCIQD6x9UyBJgOs8uI7ODczuOl3B2KHy4DYEqlr%2BdT05cs%2FQIgKQQ0tXX0nFsSjIUqQrNJzC"
        "%2FEbmiFFHg2Fx0xPluyqKIq%2BgIIQxAAGgwxNzc3ODg0MTU2ODAiDO83Xb4GSmi5Tq07HyrXAh2pc1lEdZC"
        "%2FqIDDqff8YGwFhE1jU%2Bs1uTCgMD5IpxoF8nEIvXVNSL44uQ6xrQd5EP%2FBWH"
        "%2F68wplUum2gkMy6g88XyNgCxQ3j5LiGiblqyK3GozPYLIdCxE1%2FQarcw1PBN4NjenEG8pPt7DdUVItr5lm5VAv"
        "%2BH1jMHrTHN1uO5OgbdTpCk%2BzIoya5UTCDYOXxuYIN3PFbIf4sZsxl4MWgoHEwI3N2dOT%2B0RESmyXfn%2FXq3IVwFQ7"
        "%2Bp6rFce0Iyvr7gq%2BC6atzZa%2B61e5uOrkpUiCU0CyFYu64Gf"
        "%2B9bcoj50lBAedFq1AckQLTGTPPYMNed9lTnYe8rF6AAsqi57BR04fMyiRI4OLNxAGgA4qeTo7es8dK"
        "DAxHK14LP6jvXVr4nt10Z1EjFS9pI5vTHN8cq2Ko%2Bn7pHvMb2OAYV7OMQT31ZwclgFt%2FJdOGW689Tcj%2FF12WQYso"
        "Sgwya3msgY6pwFoS9yUGltHK%2F6dqpqRdr5VdDWBFAvV98w5xsTnKVCk9S%2F19s5aWqj7jshYWetF8aYUovrRjOOFgC"
        "lTCJMn6Zbf2murzt56GGHz65V1p6Tv15PPV8WmuHw3Wp6QiKBq5pFh5mfeoQWPFDQgFNWlR825Y3pDT8Vie%2FH%2FoKz"
        "PshY4ng6tw3J5PuzoRdrJXfYY42YdYcfkn9%2F0lNBtBgqwLCdaB2rNkdV6bQ%3D%3D&Expires=1717152017",
    }
    mock_get_metric_targets = AsyncMock(return_value=result)
    mocker.patch.object(QueryClient, "get_metric_targets", mock_get_metric_targets)

    mock_convert_and_upload = AsyncMock(return_value=result["url"])
    mocker.patch.object(ParquetService, "convert_and_upload", mock_convert_and_upload)

    response = await async_client.get(
        "/v1/metrics/test_metric/targets?start_date=2022-01-01&end_date=2022-01-31&output_format=PARQUET",
    )
    assert response.status_code == 200
    assert response.json() == {
        "results": None,
        "url": result["url"],
    }
    mock_get_metric_targets.assert_awaited_once()
    mock_convert_and_upload.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_dimension(async_client: AsyncClient, mocker, dimension):
    mock_create_dimension = AsyncMock(return_value=DimensionDetail(**dimension))
    mocker.patch.object(QueryClient, "create_dimension", mock_create_dimension)

    dimension_data = {
        "dimension_id": "new_dimension",
        "label": "New Dimension",
        "reference": "new_dimension_ref",
        "definition": "New Dimension Definition",
        "meta_data": {"semantic_meta": {"cube": "cube1", "member": "new_dimension", "member_type": "dimension"}},
    }
    response = await async_client.post("/v1/dimensions", json=dimension_data)
    assert response.status_code == 200
    assert response.json() == DimensionDetail(**dimension).model_dump(mode="json")

    # test with duplicate dimension_id
    mock_create_dimension = AsyncMock(side_effect=IntegrityError("new_dimension", ANY, ANY))
    mocker.patch.object(QueryClient, "create_dimension", mock_create_dimension)
    response = await async_client.post("/v1/dimensions", json=dimension_data)
    assert response.status_code == 422
    assert response.json()["detail"]["type"] == "already_exists"


@pytest.mark.asyncio
async def test_update_dimension(async_client: AsyncClient, mocker, dimension):
    dimension_id = dimension["dimension_id"]
    updated_data = dimension.copy()
    updated_data["label"] = "Updated Dimension"

    mock_update_dimension = AsyncMock(return_value=DimensionDetail(**updated_data))
    mocker.patch.object(QueryClient, "update_dimension", mock_update_dimension)

    response = await async_client.put(f"/v1/dimensions/{dimension_id}", json=updated_data)
    assert response.status_code == 200
    assert response.json() == DimensionDetail(**updated_data).model_dump(mode="json")

    # test not found
    mock_update_dimension = AsyncMock(side_effect=DimensionNotFoundError("test_dimension"))
    mocker.patch.object(QueryClient, "update_dimension", mock_update_dimension)

    response = await async_client.put(f"/v1/dimensions/{dimension_id}", json=updated_data)
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_connect_cube(async_client: AsyncClient, mocker):
    # Mock the CubeClient's load_query_data method
    mock_load_query_data = AsyncMock(return_value={"data": []})
    mocker.patch.object(CubeClient, "load_query_data", mock_load_query_data)

    # Prepare the CubeConnectionConfig data
    config_data = {
        "cube_api_url": "http://localhost:4000/cubejs-api/v1",
        "cube_auth_type": "SECRET_KEY",
        "cube_auth_token": None,
        "cube_auth_secret_key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3Mjg1NzAzMzh9.qvRWKaXmRXrhCY5c"
        "-K1NlodDN95mRv_utQAr99Pcr8c",
    }

    # Act
    response = await async_client.post(
        "/v1/connection/cube/verify",  # Updated to the correct endpoint
        json=config_data,  # Send the config data as JSON
    )

    # Assert
    assert response.status_code == 200  # Expecting a 200 status code for success
    assert response.json() == {"message": "Connection successful"}
    mock_load_query_data.assert_awaited_once_with({"dimensions": ["metric_targets.grain"]})


@pytest.mark.asyncio
async def test_connect_cube_invalid_credentials(async_client: AsyncClient, mocker):
    # Mock the CubeClient's load_query_data method to raise an exception
    mock_load_query_data = AsyncMock(side_effect=Exception("Invalid credentials"))
    mocker.patch.object(CubeClient, "load_query_data", mock_load_query_data)

    # Prepare the CubeConnectionConfig data with invalid credentials
    config_data = {
        "cube_api_url": "https://your-cube-api-url",
        "cube_auth_type": "SECRET_KEY",
        "cube_auth_token": None,
        "cube_auth_secret_key": "invalid_secret_key",
    }

    # Act
    response = await async_client.post(
        "/v1/connection/cube/verify",  # Updated to the correct endpoint
        json=config_data,  # Send the config data as JSON
    )

    # Assert
    assert response.status_code == 400  # Expecting a 400 status code for invalid credentials
    assert response.json()["detail"] == "Invalid credentials"


@pytest.mark.asyncio
async def test_parse_expression_success(async_client: AsyncClient, mocker, metric):
    """Test successful expression parsing."""
    # Mock expression parser service
    mock_process = AsyncMock(return_value=metric["metric_expression"])
    mocker.patch("query_manager.llm.services.expression_parser.ExpressionParserService.process", mock_process)

    # Test data
    metric_id = "test_metric"
    expression = "revenue"

    # Act
    response = await async_client.post(f"/v1/metrics/{metric_id}/expression/parse", json={"expression": expression})

    # Assert
    assert response.status_code == 200
    assert response.json() == {
        "expression_str": "{SalesMktSpend\u209c} / {NewCust\u209c}",
        "expression": {
            "operands": [
                {
                    "coefficient": 1,
                    "expression": None,
                    "metric_id": "SalesMktSpend",
                    "period": 0,
                    "power": 1,
                    "type": "metric",
                },
                {
                    "coefficient": 1,
                    "expression": None,
                    "metric_id": "NewCust",
                    "period": 0,
                    "power": 1,
                    "type": "metric",
                },
            ],
            "operator": "/",
            "type": "expression",
        },
    }
    mock_process.assert_awaited_once_with(expression)


@pytest.mark.asyncio
async def test_parse_expression_llm_error(async_client: AsyncClient, mocker):
    """Test expression parsing with LLM error."""
    # Mock expression parser service to raise LLM error
    mock_process = AsyncMock(side_effect=LLMError("Failed to parse expression"))
    mocker.patch("query_manager.llm.services.expression_parser.ExpressionParserService.process", mock_process)

    # Test data
    metric_id = "test_metric"
    expression = "invalid expression"

    # Act
    response = await async_client.post(f"/v1/metrics/{metric_id}/expression/parse", json={"expression": expression})

    # Assert
    assert response.status_code == 400
    assert response.json()["detail"] == "Failed to parse expression"
    mock_process.assert_awaited_once_with(expression)


@pytest.mark.asyncio
async def test_list_cubes(async_client: AsyncClient, mocker):
    """Test listing all cubes."""
    # Mock data
    mock_cubes = [
        {
            "name": "cube1",
            "title": "Cube One",
            "measures": [
                {
                    "name": "revenue",
                    "title": "Total Revenue",
                    "short_title": "Revenue",
                    "type": "number",
                    "description": "Total revenue from all sources",
                    "metric_id": "Revenue",
                    "grain_aggregation": "sum",
                }
            ],
            "dimensions": [
                {
                    "name": "created_at",
                    "title": "Created Date",
                    "short_title": "Created",
                    "type": "time",
                    "description": "Date when record was created",
                    "dimension_id": "Created",
                }
            ],
        }
    ]

    # Mock the cube client list_cubes method
    mock_list_cubes = AsyncMock(return_value=mock_cubes)
    mocker.patch.object(CubeClient, "list_cubes", mock_list_cubes)

    # Transform mock_cubes to match API response format
    expected_response = [
        {
            **cube,
            "measures": [
                {**{("shortTitle" if k == "short_title" else k): v for k, v in m.items()}}  # type: ignore
                for m in cube["measures"]
            ],
            "dimensions": [
                {**{("shortTitle" if k == "short_title" else k): v for k, v in d.items()}}  # type: ignore
                for d in cube["dimensions"]
            ],
        }
        for cube in mock_cubes
    ]

    # Act
    response = await async_client.get("/v1/meta/cubes")

    # Assert
    assert response.status_code == 200
    assert response.json() == expected_response
    mock_list_cubes.assert_awaited_once()


@pytest.mark.asyncio
async def test_list_cubes_with_name_filter(async_client: AsyncClient, mocker):
    """Test listing cubes filtered by name."""
    # Mock data
    mock_cubes = [
        {"name": "cube1", "title": "Cube One", "measures": [], "dimensions": []},
        {"name": "cube2", "title": "Cube Two", "measures": [], "dimensions": []},
    ]

    # Mock the cube client list_cubes method
    mock_list_cubes = AsyncMock(return_value=mock_cubes)
    mocker.patch.object(CubeClient, "list_cubes", mock_list_cubes)

    # Act
    response = await async_client.get("/v1/meta/cubes?cube_name=cube1")

    # Assert
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["name"] == "cube1"
    mock_list_cubes.assert_awaited_once()


@pytest.mark.asyncio
async def test_list_cubes_error(async_client: AsyncClient, mocker):
    """Test listing cubes with API error."""
    # Mock the cube client to raise error
    mock_list_cubes = AsyncMock(side_effect=HttpClientError("Failed to fetch cubes"))
    mocker.patch.object(CubeClient, "list_cubes", mock_list_cubes)

    # Act
    response = await async_client.get("/v1/meta/cubes")

    # Assert
    assert response.status_code == 500
    assert response.json()["detail"] == "Failed to fetch cubes from Cube API"
    mock_list_cubes.assert_awaited_once()


@pytest.mark.asyncio
async def test_preview_metric_from_yaml_success(async_client: AsyncClient, mocker):
    """Test successful metric preview from YAML"""
    yaml_content = """
        metric_id: test
        label: test
        abbreviation: test
        hypothetical_max: 100
        definition: test is a metric
        expression: null
        aggregation: sum
        unit_of_measure: quantity
        unit: 'n'
        measure: "cube1.revenue"
        time_dimension: "cube1.created_at"
    """
    # Mock data with matching measure name
    mock_cubes = [
        {
            "name": "cube1",
            "title": "Cube One",
            "measures": [
                {
                    "name": "cube1.revenue",
                    "title": "Total Revenue",
                    "shortTitle": "Revenue",
                    "short_title": "Revenue",
                    "type": "number",
                    "description": "Total revenue from all sources",
                    "metric_id": "Revenue",
                    "grain_aggregation": "sum",
                }
            ],
            "dimensions": [
                {
                    "name": "cube1.created_at",
                    "title": "Created Date",
                    "shortTitle": "Created",
                    "short_title": "Created",
                    "type": "time",
                    "description": "Date when record was created",
                    "dimension_id": "Created",
                }
            ],
        }
    ]

    # Mock the cube client list_cubes method
    mock_list_cubes = AsyncMock(return_value=mock_cubes)
    mocker.patch.object(CubeClient, "list_cubes", mock_list_cubes)

    response = await async_client.post(
        "/v1/metrics/preview", content=yaml_content, headers={"Content-Type": "application/x-yaml"}
    )

    assert response.status_code == 200
    result = response.json()

    assert result["metric_id"] == "test"
    assert result["label"] == "test"
    assert result["complexity"] == "Atomic"
    assert result["abbreviation"] == "test"
    assert result["hypothetical_max"] == 100

    # Verify mock was called
    mock_list_cubes.assert_called_once()


@pytest.mark.asyncio
async def test_preview_metric_from_yaml_invalid_yaml(async_client: AsyncClient):
    """Test metric preview with invalid YAML"""
    invalid_content = "invalid: yaml: content:"

    response = await async_client.post(
        "/v1/metrics/preview", content=invalid_content, headers={"Content-Type": "application/x-yaml"}
    )

    assert response.status_code == 422
    assert "Invalid format" in response.json()["detail"]


@pytest.mark.asyncio
async def test_preview_metric_from_yaml_missing_fields(async_client: AsyncClient):
    """Test metric preview with missing required fields"""
    incomplete_content = """
    label: test
    abbreviation: test
    hypothetical_max: 100
    definition: test is a metric
    expression: "{newInqs} + {newMqls} + 1"
    aggregation: sum
    unit_of_measure: quantity
    unit: 'n'
    measure: "DimContactLifecycleStages.mqlToSalRate"
    time_dimension: "DimContactLifecycleStages.lastMqlOn"
    """

    response = await async_client.post(
        "/v1/metrics/preview", content=incomplete_content, headers={"Content-Type": "application/x-yaml"}
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_preview_metric_from_yaml_with_aim(async_client: AsyncClient, mocker):
    """Test metric preview with aim field"""
    yaml_content = """
        metric_id: test_with_aim
        label: test with aim
        abbreviation: test
        hypothetical_max: 100
        definition: test is a metric
        expression: null
        aggregation: sum
        unit_of_measure: quantity
        unit: 'n'
        measure: "cube1.revenue"
        time_dimension: "cube1.created_at"
        aim: minimize
    """
    # Mock data with matching measure name
    mock_cubes = [
        {
            "name": "cube1",
            "title": "Cube One",
            "measures": [
                {
                    "name": "cube1.revenue",
                    "title": "Total Revenue",
                    "shortTitle": "Revenue",
                    "short_title": "Revenue",
                    "type": "number",
                    "description": "Total revenue from all sources",
                    "metric_id": "Revenue",
                    "grain_aggregation": "sum",
                }
            ],
            "dimensions": [
                {
                    "name": "cube1.created_at",
                    "title": "Created Date",
                    "shortTitle": "Created",
                    "short_title": "Created",
                    "type": "time",
                    "description": "Date when record was created",
                    "dimension_id": "Created",
                }
            ],
        }
    ]

    # Mock the cube client list_cubes method
    mock_list_cubes = AsyncMock(return_value=mock_cubes)
    mocker.patch.object(CubeClient, "list_cubes", mock_list_cubes)

    response = await async_client.post(
        "/v1/metrics/preview", content=yaml_content, headers={"Content-Type": "application/x-yaml"}
    )

    assert response.status_code == 200
    result = response.json()

    assert result["metric_id"] == "test_with_aim"
    assert result["label"] == "test with aim"
    assert result["aim"] == "Minimize"
    assert result["complexity"] == "Atomic"

    # Verify mock was called
    mock_list_cubes.assert_called_once()


@pytest.mark.asyncio
async def test_preview_metric_from_yaml_with_cube_filters(async_client: AsyncClient, mocker):
    """Test metric preview with cube_filters field"""
    yaml_content = """
        metric_id: test_with_filters
        label: test with filters
        abbreviation: test
        hypothetical_max: 100
        definition: test is a metric
        expression: null
        aggregation: sum
        unit_of_measure: quantity
        unit: 'n'
        measure: "cube1.revenue"
        time_dimension: "cube1.created_at"
        cube_filters:
            - [region, equals, ["US", "CA"]]
            - [status, notEquals, ["closed"]]
            - [category, contains, ["premium", "enterprise"]]
    """
    # Mock data with matching measure name
    mock_cubes = [
        {
            "name": "cube1",
            "title": "Cube One",
            "measures": [
                {
                    "name": "cube1.revenue",
                    "title": "Total Revenue",
                    "shortTitle": "Revenue",
                    "short_title": "Revenue",
                    "type": "number",
                    "description": "Total revenue from all sources",
                    "metric_id": "Revenue",
                    "grain_aggregation": "sum",
                }
            ],
            "dimensions": [
                {
                    "name": "cube1.created_at",
                    "title": "Created Date",
                    "shortTitle": "Created",
                    "short_title": "Created",
                    "type": "time",
                    "description": "Date when record was created",
                    "dimension_id": "Created",
                }
            ],
        }
    ]

    # Mock the cube client list_cubes method
    mock_list_cubes = AsyncMock(return_value=mock_cubes)
    mocker.patch.object(CubeClient, "list_cubes", mock_list_cubes)

    response = await async_client.post(
        "/v1/metrics/preview", content=yaml_content, headers={"Content-Type": "application/x-yaml"}
    )

    assert response.status_code == 200
    result = response.json()

    assert result["metric_id"] == "test_with_filters"
    assert result["label"] == "test with filters"
    assert result["complexity"] == "Atomic"

    # Verify cube filters are properly formatted
    semantic_meta = result["meta_data"]["semantic_meta"]
    assert "cube_filters" in semantic_meta
    assert len(semantic_meta["cube_filters"]) == 3

    # Check first filter
    assert semantic_meta["cube_filters"][0]["dimension"] == "cube1.region"
    assert semantic_meta["cube_filters"][0]["operator"] == "equals"
    assert semantic_meta["cube_filters"][0]["values"] == ["US", "CA"]

    # Check second filter
    assert semantic_meta["cube_filters"][1]["dimension"] == "cube1.status"
    assert semantic_meta["cube_filters"][1]["operator"] == "notEquals"
    assert semantic_meta["cube_filters"][1]["values"] == ["closed"]

    # Check third filter
    assert semantic_meta["cube_filters"][2]["dimension"] == "cube1.category"
    assert semantic_meta["cube_filters"][2]["operator"] == "contains"
    assert semantic_meta["cube_filters"][2]["values"] == ["premium", "enterprise"]

    # Verify mock was called
    mock_list_cubes.assert_called_once()


@pytest.mark.asyncio
async def test_preview_metric_from_yaml_with_aim_and_cube_filters(async_client: AsyncClient, mocker):
    """Test metric preview with both aim and cube_filters fields"""
    yaml_content = """
        metric_id: test_with_aim_and_filters
        label: test with aim and filters
        abbreviation: test
        hypothetical_max: 100
        definition: test is a metric
        expression: null
        aggregation: sum
        unit_of_measure: quantity
        unit: 'n'
        measure: "cube1.revenue"
        time_dimension: "cube1.created_at"
        aim: maximize
        cube_filters:
            - [region, equals, ["US", "CA", "UK"]]
            - [active, equals, [true]]
    """
    # Mock data with matching measure name
    mock_cubes = [
        {
            "name": "cube1",
            "title": "Cube One",
            "measures": [
                {
                    "name": "cube1.revenue",
                    "title": "Total Revenue",
                    "shortTitle": "Revenue",
                    "short_title": "Revenue",
                    "type": "number",
                    "description": "Total revenue from all sources",
                    "metric_id": "Revenue",
                    "grain_aggregation": "sum",
                }
            ],
            "dimensions": [
                {
                    "name": "cube1.created_at",
                    "title": "Created Date",
                    "shortTitle": "Created",
                    "short_title": "Created",
                    "type": "time",
                    "description": "Date when record was created",
                    "dimension_id": "Created",
                }
            ],
        }
    ]

    # Mock the cube client list_cubes method
    mock_list_cubes = AsyncMock(return_value=mock_cubes)
    mocker.patch.object(CubeClient, "list_cubes", mock_list_cubes)

    response = await async_client.post(
        "/v1/metrics/preview", content=yaml_content, headers={"Content-Type": "application/x-yaml"}
    )

    assert response.status_code == 200
    result = response.json()

    assert result["metric_id"] == "test_with_aim_and_filters"
    assert result["label"] == "test with aim and filters"
    assert result["aim"] == "Maximize"
    assert result["complexity"] == "Atomic"

    # Verify cube filters are properly formatted
    semantic_meta = result["meta_data"]["semantic_meta"]
    assert "cube_filters" in semantic_meta
    assert len(semantic_meta["cube_filters"]) == 2

    # Check first filter
    assert semantic_meta["cube_filters"][0]["dimension"] == "cube1.region"
    assert semantic_meta["cube_filters"][0]["operator"] == "equals"
    assert semantic_meta["cube_filters"][0]["values"] == ["US", "CA", "UK"]

    # Check second filter
    assert semantic_meta["cube_filters"][1]["dimension"] == "cube1.active"
    assert semantic_meta["cube_filters"][1]["operator"] == "equals"
    assert semantic_meta["cube_filters"][1]["values"] == [True]

    # Verify mock was called
    mock_list_cubes.assert_called_once()


@pytest.mark.asyncio
async def test_preview_metric_from_yaml_invalid_cube_filters_format(async_client: AsyncClient, mocker):
    """Test metric preview with invalid cube_filters format"""
    yaml_content = """
        metric_id: test_invalid_filters
        label: test invalid filters
        abbreviation: test
        hypothetical_max: 100
        definition: test is a metric
        expression: null
        aggregation: sum
        unit_of_measure: quantity
        unit: 'n'
        measure: "cube1.revenue"
        time_dimension: "cube1.created_at"
        cube_filters:
            - [region, equals]
            - [status, notEquals, ["closed"], "extra_field"]
    """
    # Mock data with matching measure name
    mock_cubes = [
        {
            "name": "cube1",
            "title": "Cube One",
            "measures": [
                {
                    "name": "cube1.revenue",
                    "title": "Total Revenue",
                    "shortTitle": "Revenue",
                    "short_title": "Revenue",
                    "type": "number",
                    "description": "Total revenue from all sources",
                    "metric_id": "Revenue",
                    "grain_aggregation": "sum",
                }
            ],
            "dimensions": [
                {
                    "name": "cube1.created_at",
                    "title": "Created Date",
                    "shortTitle": "Created",
                    "short_title": "Created",
                    "type": "time",
                    "description": "Date when record was created",
                    "dimension_id": "Created",
                }
            ],
        }
    ]

    # Mock the cube client list_cubes method
    mock_list_cubes = AsyncMock(return_value=mock_cubes)
    mocker.patch.object(CubeClient, "list_cubes", mock_list_cubes)

    response = await async_client.post(
        "/v1/metrics/preview", content=yaml_content, headers={"Content-Type": "application/x-yaml"}
    )

    assert response.status_code == 422
    error_detail = response.json()["detail"]
    assert "Invalid cube filter format" in error_detail


@pytest.mark.asyncio
async def test_preview_metric_from_yaml_cube_filters_invalid_dimension_type(async_client: AsyncClient, mocker):
    """Test metric preview with invalid dimension type in cube_filters"""
    yaml_content = """
        metric_id: test_invalid_dimension_type
        label: test invalid dimension type
        abbreviation: test
        hypothetical_max: 100
        definition: test is a metric
        expression: null
        aggregation: sum
        unit_of_measure: quantity
        unit: 'n'
        measure: "cube1.revenue"
        time_dimension: "cube1.created_at"
        cube_filters:
            - [123, equals, ["US", "CA"]]
    """
    # Mock data with matching measure name
    mock_cubes = [
        {
            "name": "cube1",
            "title": "Cube One",
            "measures": [
                {
                    "name": "cube1.revenue",
                    "title": "Total Revenue",
                    "shortTitle": "Revenue",
                    "short_title": "Revenue",
                    "type": "number",
                    "description": "Total revenue from all sources",
                    "metric_id": "Revenue",
                    "grain_aggregation": "sum",
                }
            ],
            "dimensions": [
                {
                    "name": "cube1.created_at",
                    "title": "Created Date",
                    "shortTitle": "Created",
                    "short_title": "Created",
                    "type": "time",
                    "description": "Date when record was created",
                    "dimension_id": "Created",
                }
            ],
        }
    ]

    # Mock the cube client list_cubes method
    mock_list_cubes = AsyncMock(return_value=mock_cubes)
    mocker.patch.object(CubeClient, "list_cubes", mock_list_cubes)

    response = await async_client.post(
        "/v1/metrics/preview", content=yaml_content, headers={"Content-Type": "application/x-yaml"}
    )

    assert response.status_code == 422
    error_detail = response.json()["detail"]
    assert "Invalid dimension at index 0" in error_detail
    assert "Expected string but got int" in error_detail


@pytest.mark.asyncio
async def test_preview_metric_from_yaml_cube_filters_invalid_values_type(async_client: AsyncClient, mocker):
    """Test metric preview with invalid values type in cube_filters"""
    yaml_content = """
        metric_id: test_invalid_values_type
        label: test invalid values type
        abbreviation: test
        hypothetical_max: 100
        definition: test is a metric
        expression: null
        aggregation: sum
        unit_of_measure: quantity
        unit: 'n'
        measure: "cube1.revenue"
        time_dimension: "cube1.created_at"
        cube_filters:
            - [region, equals, "US"]
    """
    # Mock data with matching measure name
    mock_cubes = [
        {
            "name": "cube1",
            "title": "Cube One",
            "measures": [
                {
                    "name": "cube1.revenue",
                    "title": "Total Revenue",
                    "shortTitle": "Revenue",
                    "short_title": "Revenue",
                    "type": "number",
                    "description": "Total revenue from all sources",
                    "metric_id": "Revenue",
                    "grain_aggregation": "sum",
                }
            ],
            "dimensions": [
                {
                    "name": "cube1.created_at",
                    "title": "Created Date",
                    "shortTitle": "Created",
                    "short_title": "Created",
                    "type": "time",
                    "description": "Date when record was created",
                    "dimension_id": "Created",
                }
            ],
        }
    ]

    # Mock the cube client list_cubes method
    mock_list_cubes = AsyncMock(return_value=mock_cubes)
    mocker.patch.object(CubeClient, "list_cubes", mock_list_cubes)

    response = await async_client.post(
        "/v1/metrics/preview", content=yaml_content, headers={"Content-Type": "application/x-yaml"}
    )

    assert response.status_code == 422
    error_detail = response.json()["detail"]
    assert "Invalid values at index 0" in error_detail
    assert "Expected list but got str" in error_detail


@pytest.mark.asyncio
async def test_preview_metric_from_yaml_empty_cube_filters(async_client: AsyncClient, mocker):
    """Test metric preview with empty cube_filters array"""
    yaml_content = """
        metric_id: test_empty_filters
        label: test empty filters
        abbreviation: test
        hypothetical_max: 100
        definition: test is a metric
        expression: null
        aggregation: sum
        unit_of_measure: quantity
        unit: 'n'
        measure: "cube1.revenue"
        time_dimension: "cube1.created_at"
        cube_filters: []
    """
    # Mock data with matching measure name
    mock_cubes = [
        {
            "name": "cube1",
            "title": "Cube One",
            "measures": [
                {
                    "name": "cube1.revenue",
                    "title": "Total Revenue",
                    "shortTitle": "Revenue",
                    "short_title": "Revenue",
                    "type": "number",
                    "description": "Total revenue from all sources",
                    "metric_id": "Revenue",
                    "grain_aggregation": "sum",
                }
            ],
            "dimensions": [
                {
                    "name": "cube1.created_at",
                    "title": "Created Date",
                    "shortTitle": "Created",
                    "short_title": "Created",
                    "type": "time",
                    "description": "Date when record was created",
                    "dimension_id": "Created",
                }
            ],
        }
    ]

    # Mock the cube client list_cubes method
    mock_list_cubes = AsyncMock(return_value=mock_cubes)
    mocker.patch.object(CubeClient, "list_cubes", mock_list_cubes)

    response = await async_client.post(
        "/v1/metrics/preview", content=yaml_content, headers={"Content-Type": "application/x-yaml"}
    )

    assert response.status_code == 200
    result = response.json()

    assert result["metric_id"] == "test_empty_filters"
    assert result["label"] == "test empty filters"
    assert result["complexity"] == "Atomic"

    # Verify cube filters are empty
    semantic_meta = result["meta_data"]["semantic_meta"]
    assert "cube_filters" in semantic_meta
    assert semantic_meta["cube_filters"] == []

    # Verify mock was called
    mock_list_cubes.assert_called_once()


@pytest.mark.asyncio
async def test_delete_metrics_bulk_success(async_client: AsyncClient, mocker):
    """Test successful bulk deletion of metrics."""
    # Mock data
    metric_ids = ["metric1", "metric2", "metric3"]

    # Mock the delete_metric method
    mock_delete_metric = AsyncMock()
    mocker.patch.object(QueryClient, "delete_metric", mock_delete_metric)

    # Make the request
    response = await async_client.request("DELETE", "/v1/metrics/bulk", json=metric_ids)

    # Assert response
    assert response.status_code == 200
    assert response.json() == {"message": "Successfully deleted 3 metrics."}

    # Verify delete_metric was called for each metric
    assert mock_delete_metric.call_count == 3
    mock_delete_metric.assert_has_awaits([mocker.call("metric1"), mocker.call("metric2"), mocker.call("metric3")])


@pytest.mark.asyncio
async def test_delete_metrics_bulk_partial_success(async_client: AsyncClient, mocker):
    """Test bulk deletion with some failures."""
    # Mock data
    metric_ids = ["metric1", "metric2", "metric3"]

    # Mock the delete_metric method to fail for metric2
    async def mock_delete(metric_id: str):
        if metric_id == "metric2":
            raise NotFoundError("Metric not found")
        return None

    mock_delete_metric = AsyncMock(side_effect=mock_delete)
    mocker.patch.object(QueryClient, "delete_metric", mock_delete_metric)

    # Make the request
    response = await async_client.request("DELETE", "/v1/metrics/bulk", json=metric_ids)

    # Assert response
    assert response.status_code == 200
    assert response.json() == {"message": "Successfully deleted 2 metrics. Failed to delete 1 metrics: ['metric2']"}

    # Verify delete_metric was called for each metric
    assert mock_delete_metric.call_count == 3


@pytest.mark.asyncio
async def test_delete_metrics_bulk_all_fail(async_client: AsyncClient, mocker):
    """Test bulk deletion when all metrics fail."""
    # Mock data
    metric_ids = ["metric1", "metric2"]

    # Mock the delete_metric method to fail for all metrics
    mock_delete_metric = AsyncMock(side_effect=NotFoundError("Metric not found"))
    mocker.patch.object(QueryClient, "delete_metric", mock_delete_metric)

    # Make the request
    response = await async_client.request("DELETE", "/v1/metrics/bulk", json=metric_ids)

    # Assert response
    assert response.status_code == 404
    assert response.json() == {
        "detail": {
            "loc": ["body", "metric_ids"],
            "msg": "None of the metrics were found: ['metric1', 'metric2']",
            "type": "not_found",
        }
    }

    # Verify delete_metric was called for each metric
    assert mock_delete_metric.call_count == 2


@pytest.mark.asyncio
async def test_delete_metrics_bulk_empty_list(async_client: AsyncClient, mocker):
    """Test bulk deletion with empty list."""
    # Mock the delete_metric method
    mock_delete_metric = AsyncMock()
    mocker.patch.object(QueryClient, "delete_metric", mock_delete_metric)

    # Make the request with empty list
    response = await async_client.request("DELETE", "/v1/metrics/bulk", json=[])

    # Assert response
    assert response.status_code == 200
    assert response.json() == {"message": "Successfully deleted 0 metrics."}

    # Verify delete_metric was not called
    mock_delete_metric.assert_not_called()


@pytest.mark.asyncio
async def test_delete_metric_success(async_client: AsyncClient, mocker):
    """Test successful deletion of a single metric."""
    # Mock the delete_metric method
    mock_delete_metric = AsyncMock()
    mocker.patch.object(QueryClient, "delete_metric", mock_delete_metric)

    # Make the request
    metric_id = "test_metric"
    response = await async_client.request("DELETE", f"/v1/metrics/{metric_id}")

    # Assert response
    assert response.status_code == 200
    assert response.json() == {
        "message": f"Metric '{metric_id}' and all its relationships have been successfully deleted."
    }

    # Verify delete_metric was called once with correct argument
    mock_delete_metric.assert_awaited_once_with(metric_id)


@pytest.mark.asyncio
async def test_delete_metric_not_found(async_client: AsyncClient, mocker):
    """Test deletion of a non-existent metric."""
    # Mock the delete_metric method to raise NotFoundError
    mock_delete_metric = AsyncMock(side_effect=NotFoundError("Metric not found"))
    mocker.patch.object(QueryClient, "delete_metric", mock_delete_metric)

    # Make the request
    metric_id = "non_existent_metric"
    response = await async_client.request("DELETE", f"/v1/metrics/{metric_id}")

    # Assert response
    assert response.status_code == 404
    assert response.json() == {
        "detail": {"loc": ["path", "metric_id"], "msg": f"Metric with id '{metric_id}' not found.", "type": "not_found"}
    }

    # Verify delete_metric was called once with correct argument
    mock_delete_metric.assert_awaited_once_with(metric_id)


# Tests for cache-related routes added in the diff


@pytest.mark.asyncio
async def test_list_grain_configs(async_client: AsyncClient, mocker):
    """Test listing grain cache configurations."""
    from query_manager.core.crud import CRUDMetricCacheGrainConfig

    # Mock grain configs
    mock_configs = [
        {
            "id": 1,
            "grain": "day",
            "is_enabled": True,
            "initial_sync_period": 730,
            "delta_sync_period": 90,
            "tenant_id": 1,
        },
        {
            "id": 2,
            "grain": "week",
            "is_enabled": False,
            "initial_sync_period": 365,
            "delta_sync_period": 30,
            "tenant_id": 1,
        },
    ]

    mock_paginate = AsyncMock(return_value=(mock_configs, 2))
    mocker.patch.object(CRUDMetricCacheGrainConfig, "paginate", mock_paginate)

    response = await async_client.get("/v1/grains/cache-config")

    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 2
    assert len(data["results"]) == 2
    assert data["results"][0]["grain"] == "day"
    assert data["results"][0]["is_enabled"] is True


@pytest.mark.asyncio
async def test_get_metric_cache_config(async_client: AsyncClient, mocker):
    """Test getting cache configuration for a specific metric."""
    from query_manager.core.crud import CRUDMetricCacheConfig

    mock_config = {"id": 1, "metric_id": "test_metric", "is_enabled": True, "last_sync_date": None, "sync_status": None}

    mock_get_by_metric_id = AsyncMock(return_value=mock_config)
    mocker.patch.object(CRUDMetricCacheConfig, "get_by_metric_id", mock_get_by_metric_id)

    response = await async_client.get("/v1/metrics/test_metric/cache-config")

    assert response.status_code == 200
    data = response.json()
    assert data["metric_id"] == "test_metric"
    assert data["is_enabled"] is True
    mock_get_by_metric_id.assert_awaited_once_with("test_metric")


@pytest.mark.asyncio
async def test_update_metric_cache_config(async_client: AsyncClient, mocker):
    """Test updating cache configuration for a specific metric."""
    from query_manager.core.crud import CRUDMetricCacheConfig

    mock_updated_config = {
        "id": 1,
        "metric_id": "test_metric",
        "is_enabled": False,
        "last_sync_date": None,
        "sync_status": None,
    }

    mock_create_or_update = AsyncMock(return_value=mock_updated_config)
    mocker.patch.object(CRUDMetricCacheConfig, "create_or_update_metric_config", mock_create_or_update)

    response = await async_client.put("/v1/metrics/test_metric/cache-config", json={"is_enabled": False})

    assert response.status_code == 200
    data = response.json()
    assert data["metric_id"] == "test_metric"
    assert data["is_enabled"] is False
    mock_create_or_update.assert_awaited_once_with("test_metric", False)


@pytest.mark.asyncio
async def test_list_metric_cache_configs(async_client: AsyncClient, mocker, app):
    """Test listing metric cache configurations with sync information."""
    from query_manager.semantic_manager.dependencies import get_cache_manager

    mock_configs = [
        {"id": 1, "metric_id": "metric1", "is_enabled": True, "last_sync_date": None, "sync_status": None},
        {"id": 2, "metric_id": "metric2", "is_enabled": False, "last_sync_date": None, "sync_status": None},
    ]

    # Create mock cache manager
    mock_cache_manager = AsyncMock()
    mock_cache_manager.get_metric_cache_configs = AsyncMock(return_value=(mock_configs, 2))

    # Override the dependency
    app.dependency_overrides[get_cache_manager] = lambda: mock_cache_manager

    try:
        response = await async_client.get("/v1/metrics/cache-config/all")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["results"]) == 2
        assert data["results"][0]["metric_id"] == "metric1"
        assert data["results"][0]["is_enabled"] is True
    finally:
        # Clean up the override
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_tenant_sync_status(async_client: AsyncClient, mocker, app):
    """Test getting tenant sync status history."""
    from query_manager.semantic_manager.dependencies import get_cache_manager

    mock_sync_history = [
        {
            "id": 1,
            "sync_operation": "SNOWFLAKE_CACHE",
            "grain": "day",
            "last_sync_at": "2024-01-01T10:00:00",
            "sync_status": "SUCCESS",
            "metrics_processed": 10,
            "metrics_succeeded": 8,
            "metrics_failed": 2,
            "error": None,
            "run_info": {},
            "created_at": "2024-01-01T09:00:00",
            "updated_at": "2024-01-01T10:00:00",
        }
    ]

    # Create mock cache manager
    mock_cache_manager = AsyncMock()
    mock_cache_manager.get_tenant_sync_history = AsyncMock(return_value=(mock_sync_history, 1))

    # Override the dependency
    app.dependency_overrides[get_cache_manager] = lambda: mock_cache_manager

    try:
        response = await async_client.get("/v1/tenant/sync-status")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert len(data["results"]) == 1
        assert data["results"][0]["sync_operation"] == "SNOWFLAKE_CACHE"
        assert data["results"][0]["sync_status"] == "SUCCESS"
    finally:
        # Clean up the override
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_bulk_update_metric_cache_configs(async_client: AsyncClient, mocker):
    """Test bulk updating metric cache configurations."""
    from query_manager.core.crud import CRUDMetricCacheConfig

    mock_updated_configs = [
        {"id": 1, "metric_id": "metric1", "is_enabled": False, "last_sync_date": None, "sync_status": None},
        {"id": 2, "metric_id": "metric2", "is_enabled": False, "last_sync_date": None, "sync_status": None},
    ]

    mock_bulk_update = AsyncMock(return_value=mock_updated_configs)
    mocker.patch.object(CRUDMetricCacheConfig, "bulk_update_metric_configs", mock_bulk_update)

    response = await async_client.post(
        "/v1/metrics/cache-config/bulk", json={"metric_ids": ["metric1", "metric2"], "is_enabled": False}
    )

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["metric_id"] == "metric1"
    assert data[0]["is_enabled"] is False
    assert data[1]["metric_id"] == "metric2"
    assert data[1]["is_enabled"] is False
    mock_bulk_update.assert_awaited_once_with(["metric1", "metric2"], False)


@pytest.mark.asyncio
async def test_delete_dimension_success(async_client: AsyncClient, mocker, dimension):
    """Test successful deletion of a single dimension."""
    # Mock the delete_dimension method
    mock_delete_dimension = AsyncMock()
    mocker.patch.object(QueryClient, "delete_dimension", mock_delete_dimension)

    # Make the request
    dimension_id = dimension["dimension_id"]
    response = await async_client.delete(f"/v1/dimensions/{dimension_id}")

    # Assert response
    assert response.status_code == 200

    # Verify delete_dimension was called once with correct argument
    mock_delete_dimension.assert_awaited_once_with(dimension_id)


@pytest.mark.asyncio
async def test_delete_dimension_not_found(async_client: AsyncClient, mocker):
    """Test deletion of a non-existent dimension."""
    # Mock the delete_dimension method to raise NotFoundError
    mock_delete_dimension = AsyncMock(side_effect=NotFoundError("Dimension not found"))
    mocker.patch.object(QueryClient, "delete_dimension", mock_delete_dimension)

    # Make the request
    dimension_id = "non_existent_dimension"
    response = await async_client.delete(f"/v1/dimensions/{dimension_id}")

    # Assert response
    assert response.status_code == 404
    assert response.json() == {
        "detail": {
            "loc": ["path", "dimension_id"],
            "msg": f"Dimension with id '{dimension_id}' not found.",
            "type": "not_found",
        }
    }

    # Verify delete_dimension was called once with correct argument
    mock_delete_dimension.assert_awaited_once_with(dimension_id)


@pytest.mark.asyncio
async def test_list_dimensions_with_filter(async_client: AsyncClient, mocker, dimension):
    mock_list_dimensions = AsyncMock(return_value=([dimension], 1))
    mocker.patch.object(QueryClient, "list_dimensions", mock_list_dimensions)

    response = await async_client.get("/v1/dimensions?dimension_label=Plan")
    assert response.status_code == 200
    assert response.json() == {
        "count": 1,
        "limit": 10,
        "offset": 0,
        "pages": 1,
        "results": [DimensionCompact(**dimension).model_dump(mode="json")],
    }


@pytest.mark.asyncio
async def test_list_dimensions_with_filter_not_found(async_client: AsyncClient, mocker):
    response = await async_client.get("/v1/dimensions?dimension_label=test")
    assert response.status_code == 200
    assert response.json() == {
        "count": 0,
        "limit": 10,
        "offset": 0,
        "pages": 0,
        "results": [],
    }


@pytest.mark.asyncio
async def test_list_metrics_with_pagination(async_client: AsyncClient, mocker, metric):
    """Test listing metrics with custom pagination parameters."""
    mock_list_metrics = AsyncMock(return_value=([Metric.parse_obj(metric)], 25))
    mocker.patch.object(QueryClient, "list_metrics", mock_list_metrics)

    response = await async_client.get("/v1/metrics?limit=5&offset=10")

    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 25
    assert data["limit"] == 5
    assert data["offset"] == 10
    assert data["pages"] == 5
    assert len(data["results"]) == 1


@pytest.mark.asyncio
async def test_list_metrics_with_filters(async_client: AsyncClient, mocker, metric):
    """Test listing metrics with filters."""
    mock_list_metrics = AsyncMock(return_value=([Metric.parse_obj(metric)], 1))
    mocker.patch.object(QueryClient, "list_metrics", mock_list_metrics)

    response = await async_client.get("/v1/metrics?metric_label=test&metric_ids=metric1,metric2")

    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert len(data["results"]) == 1

    # Verify the mock was called with the correct parameters
    args, kwargs = mock_list_metrics.call_args
    assert kwargs["metric_label"] == "test"
    assert kwargs["metric_ids"] == ["metric1,metric2"]


@pytest.mark.asyncio
async def test_update_metric_success(async_client: AsyncClient, mocker, metric):
    """Test successful metric update."""
    updated_metric = metric.copy()
    updated_metric["label"] = "Updated Label"

    mock_update_metric = AsyncMock(return_value=MetricDetail(**updated_metric))
    mocker.patch.object(QueryClient, "update_metric", mock_update_metric)

    update_data = {"label": "Updated Label", "definition": "Updated definition"}
    response = await async_client.patch(f"/v1/metrics/{metric['metric_id']}", json=update_data)

    assert response.status_code == 200
    assert response.json()["label"] == "Updated Label"


@pytest.mark.asyncio
async def test_list_dimensions_with_ids_filter(async_client: AsyncClient, mocker, dimension):
    """Test listing dimensions with dimension IDs filter."""
    mock_list_dimensions = AsyncMock(return_value=([dimension], 1))
    mocker.patch.object(QueryClient, "list_dimensions", mock_list_dimensions)

    response = await async_client.get("/v1/dimensions?dimension_ids=dim1&dimension_ids=dim2")

    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert len(data["results"]) == 1

    # Verify the mock was called with the correct parameters
    args, kwargs = mock_list_dimensions.call_args
    assert kwargs["dimension_ids"] == ["dim1", "dim2"]


@pytest.mark.asyncio
async def test_get_dimension_members_empty(async_client: AsyncClient, mocker):
    """Test getting dimension members when there are none."""
    mock_get_dimension_members = AsyncMock(return_value=[])
    mocker.patch.object(QueryClient, "get_dimension_members", mock_get_dimension_members)

    response = await async_client.get("/v1/dimensions/empty_dimension/members")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_get_cache_info_success(async_client: AsyncClient, mocker, app):
    """Test getting cache info successfully."""
    from query_manager.semantic_manager.dependencies import get_cache_manager

    mock_cache_info = {
        "table_name": "test_tenant_cache",
        "tenant_id": 1,
        "status": "active",
        "metrics_cached": 50,
        "available_grains": ["day", "week", "month"],
        "date_range": {"start": "2023-01-01", "end": "2024-12-31"},
        "last_sync": "2024-01-15T10:30:00Z",
        "performance_metrics": {"size_mb": 1024.5, "avg_query_time_ms": 150, "cache_hit_ratio": 0.85},
        "error": None,
    }

    mock_cache_manager = AsyncMock()
    mock_cache_manager.get_comprehensive_cache_info = AsyncMock(return_value=mock_cache_info)
    app.dependency_overrides[get_cache_manager] = lambda: mock_cache_manager

    try:
        response = await async_client.get("/v1/cache/info")
        assert response.status_code == 200
        data = response.json()
        assert data["table_name"] == "test_tenant_cache"
    finally:
        app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_update_grain_config_error(async_client: AsyncClient, mocker):
    """Test updating grain config with server error."""
    from query_manager.core.crud import CRUDMetricCacheGrainConfig

    mock_update = AsyncMock(side_effect=Exception("Database error"))
    mocker.patch.object(CRUDMetricCacheGrainConfig, "update_grain_config", mock_update)

    update_data = {"is_enabled": True, "initial_sync_period": 365}
    response = await async_client.put("/v1/grains/day/cache-config", json=update_data)

    assert response.status_code == 500
    assert "Failed to update grain configuration" in response.json()["detail"]


@pytest.mark.asyncio
async def test_enable_all_grain_caching_success(async_client: AsyncClient, mocker):
    """Test enabling all grain caching successfully."""
    from query_manager.core.crud import CRUDMetricCacheGrainConfig

    mock_configs = [
        {
            "id": 1,
            "grain": "day",
            "is_enabled": True,
            "initial_sync_period": 730,
            "delta_sync_period": 90,
            "tenant_id": 1,
        },
        {
            "id": 2,
            "grain": "week",
            "is_enabled": True,
            "initial_sync_period": 365,
            "delta_sync_period": 30,
            "tenant_id": 1,
        },
    ]

    mock_create_default = AsyncMock(return_value=mock_configs)
    mocker.patch.object(CRUDMetricCacheGrainConfig, "create_default_grain_configs", mock_create_default)

    response = await async_client.post("/v1/grains/cache-config/enable-all")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert all(config["is_enabled"] for config in data)


@pytest.mark.asyncio
async def test_get_metric_cache_config_not_found(async_client: AsyncClient, mocker):
    """Test getting metric cache config that doesn't exist."""
    from query_manager.core.crud import CRUDMetricCacheConfig

    mock_get_by_metric_id = AsyncMock(side_effect=NotFoundError("Config not found"))
    mocker.patch.object(CRUDMetricCacheConfig, "get_by_metric_id", mock_get_by_metric_id)

    response = await async_client.get("/v1/metrics/nonexistent_metric/cache-config")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_enable_all_metric_caching_success(async_client: AsyncClient, mocker):
    """Test enabling caching for all metrics."""
    from query_manager.core.crud import CRUDMetricCacheConfig

    mock_configs = [
        {"id": 1, "metric_id": "metric1", "is_enabled": True, "last_sync_date": None, "sync_status": None},
        {"id": 2, "metric_id": "metric2", "is_enabled": True, "last_sync_date": None, "sync_status": None},
    ]

    mock_enable_all = AsyncMock(return_value=mock_configs)
    mocker.patch.object(CRUDMetricCacheConfig, "enable_all_metrics", mock_enable_all)

    response = await async_client.post("/v1/metrics/cache-config/enable-all")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert all(config["is_enabled"] for config in data)
