from copy import deepcopy
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from commons.models.enums import Granularity
from commons.utilities.pagination import PaginationParams
from query_manager.core.dependencies import get_cube_client, get_query_client
from query_manager.core.enums import Complexity
from query_manager.core.models import Dimension, Metric
from query_manager.core.schemas import (
    DimensionCreate,
    DimensionUpdate,
    MetricCreate,
    MetricDetail,
    MetricUpdate,
)
from query_manager.exceptions import DimensionNotFoundError, MetricNotFoundError


@pytest.fixture
async def insights_backend_client():
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
async def query_client(insights_backend_client):
    cube_client = await get_cube_client(await insights_backend_client)
    dimensions_crud = AsyncMock()
    metrics_crud = AsyncMock()
    return await get_query_client(cube_client, dimensions_crud, metrics_crud)


@pytest.mark.asyncio
async def test_list_metrics(mocker, metric, query_client):
    client = await query_client
    mock_paginate = AsyncMock(return_value=([metric], 1))
    mocker.patch.object(client.metric_crud, "paginate", mock_paginate)

    params = PaginationParams(page=1, size=10)
    result, count = await client.list_metrics(metric_ids=[metric["metric_id"]], params=params)
    assert len(result) == 1
    assert result[0] == metric
    assert count == 1

    filter_params = {"metric_ids": [metric["metric_id"]], "metric_label": None}
    mock_paginate.assert_called_once_with(params, filter_params=filter_params)


@pytest.mark.asyncio
async def test_list_metrics_with_metric_id_filter(mocker, metric, query_client):
    client = await query_client
    metric2 = deepcopy(metric)
    metric2["id"] = 2
    metric2["metric_id"] = "metric_id2"
    mock_paginate = AsyncMock(return_value=([metric2], 1))
    mocker.patch.object(client.metric_crud, "paginate", mock_paginate)

    params = PaginationParams(page=1, size=10)
    result, count = await client.list_metrics(metric_ids=["metric_id2"], params=params)
    assert len(result) == 1
    assert result[0] == metric2
    assert count == 1

    mock_paginate.assert_called_once_with(params, filter_params={"metric_ids": ["metric_id2"], "metric_label": None})


@pytest.mark.asyncio
async def test_get_metric_details(mocker, metric, query_client):
    client = await query_client
    metric = Metric.parse_obj(metric)
    mock_get_by_metric_id = AsyncMock(return_value=metric)
    mocker.patch.object(client.metric_crud, "get_by_metric_id", mock_get_by_metric_id)

    result = await client.get_metric_details(metric.metric_id)
    assert result == metric

    mock_get_by_metric_id.assert_called_once_with(metric.metric_id)


@pytest.mark.asyncio
async def test_get_metric_details_not_found(mocker, query_client):
    client = await query_client
    mock_get_by_metric_id = AsyncMock(side_effect=MetricNotFoundError("non_existent_metric_id"))
    mocker.patch.object(client.metric_crud, "get_by_metric_id", mock_get_by_metric_id)

    with pytest.raises(MetricNotFoundError):
        await client.get_metric_details("non_existent_metric_id")


@pytest.fixture
def dimensions():
    return Dimension(
        id=1,
        dimension_id="billing_plan",
        label="Billing Plan",
        reference="billing_plan",
        definition="Billing Plan Definition",
        metadata={"semantic_meta": {"cube": "cube1", "member": "billing_plan", "member_type": "dimension"}},
        created_at=datetime(2024, 7, 24, 9, 54, 10, 411357),
        updated_at=datetime(2024, 7, 24, 9, 54, 10, 411359),
    )


@pytest.mark.asyncio
async def test_list_dimensions(mocker, dimensions, query_client):
    client = await query_client
    mock_paginate = AsyncMock(return_value=([dimensions], 1))
    mocker.patch.object(client.dimensions_crud, "paginate", mock_paginate)

    params = PaginationParams(page=1, size=10)
    result, count = await client.list_dimensions(params=params, dimension_ids=None, dimension_label=None)
    assert len(result) == 1
    assert result[0] == dimensions
    assert count == 1

    mock_paginate.assert_called_once_with(params, filter_params={"dimension_ids": None, "dimension_label": None})


@pytest.mark.asyncio
async def test_get_dimension_details(mocker, dimensions, query_client):
    client = await query_client
    mock_get_by_dimension_id = AsyncMock(return_value=dimensions)
    mocker.patch.object(client.dimensions_crud, "get_by_dimension_id", mock_get_by_dimension_id)

    result = await client.get_dimension_details(dimensions.id)
    assert result == dimensions

    mock_get_by_dimension_id.assert_called_once_with(dimensions.id)


@pytest.mark.asyncio
async def test_get_dimension_details_not_found(mocker, query_client):
    client = await query_client
    mock_get_by_dimension_id = AsyncMock(side_effect=DimensionNotFoundError("non_existent_dimension_id"))
    mocker.patch.object(client.dimensions_crud, "get_by_dimension_id", mock_get_by_dimension_id)

    with pytest.raises(DimensionNotFoundError):
        await client.get_dimension_details("non_existent_dimension_id")


@pytest.mark.asyncio
async def test_get_dimension_members(mocker, dimension, query_client):
    # Mock response from cube for dimension members
    client = await query_client
    mock_load_dimension_members_from_cube = AsyncMock(return_value=["Enterprise", "Basic"])
    mocker.patch.object(client.cube_client, "load_dimension_members_from_cube", mock_load_dimension_members_from_cube)

    result = await client.get_dimension_members(dimension["id"])
    assert result == ["Enterprise", "Basic"]

    # no dimension match
    mock_load_dimension_members_from_cube = AsyncMock(return_value=[])
    mocker.patch.object(client.cube_client, "load_dimension_members_from_cube", mock_load_dimension_members_from_cube)

    result = await client.get_dimension_members("non_existent_dimension_id")
    assert result == []


@pytest.mark.asyncio
async def test_get_metric_targets_with_time_range(mocker, metric, query_client):
    client = await query_client
    mock_load_targets_from_cube = AsyncMock(
        return_value=[
            {
                "metric_id": metric["id"],
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
    )
    mocker.patch.object(client.cube_client, "load_metric_targets_from_cube", mock_load_targets_from_cube)
    mocker.patch.object(client, "get_metric_details", AsyncMock(return_value=metric))

    result = await client.get_metric_targets(metric["id"], date(2021, 1, 1), date(2021, 1, 31), Granularity.WEEK)

    assert len(result) == 1
    assert result[0]["metric_id"] == metric["id"]
    assert result[0]["grain"] == "week"
    assert result[0]["target_date"] == "2021-01-01"


@pytest.mark.asyncio
async def test_get_metric_targets_invalid_metric_id(mocker, query_client):
    client = await query_client
    mocker.patch.object(client, "get_metric_details", AsyncMock(return_value=None))
    with pytest.raises(MetricNotFoundError):
        await client.get_metric_targets("invalid_metric_id")


@pytest.mark.asyncio
async def test_get_metric_values_invalid_metric_id(mocker, query_client):
    client = await query_client
    mocker.patch.object(client, "get_metric_details", AsyncMock(return_value=None))
    with pytest.raises(MetricNotFoundError):
        await client.get_metric_values("invalid_metric_id")


@pytest.mark.asyncio
async def test_get_metric_values_without_dimensions(mocker, metric, query_client):
    # Mock response from cube for values without dimensions
    client = await query_client
    mock_load_metric_values_from_cube = AsyncMock(return_value=[{"date": "2022-01-01", "value": 200}])
    mocker.patch.object(client.cube_client, "load_metric_values_from_cube", mock_load_metric_values_from_cube)
    mocker.patch.object(client, "get_metric_details", AsyncMock(return_value=metric))

    result = await client.get_metric_values("metric_id", date(2022, 1, 1), date(2022, 1, 31), None)

    assert len(result) == 1
    assert result[0] == {"date": "2022-01-01", "value": 200}
    mock_load_metric_values_from_cube.assert_awaited_with(metric, None, date(2022, 1, 1), date(2022, 1, 31), [])


@pytest.mark.asyncio
async def test_get_metric_values_with_dimensions(mocker, metric, query_client):
    client = await query_client
    metric = MetricDetail.parse_obj(metric)
    # Mock response from cube for values with dimensions
    mock_load_metric_values_from_cube = AsyncMock(
        return_value=[{"date": "2022-01-01", "value": 100, "billing_plan": "Enterprise"}]
    )
    mocker.patch.object(client.cube_client, "load_metric_values_from_cube", mock_load_metric_values_from_cube)
    mocker.patch.object(client, "get_metric_details", AsyncMock(return_value=metric))
    result = await client.get_metric_values("metric_id", date(2022, 1, 1), date(2022, 1, 31), ["billing_plan"])

    assert len(result) == 1
    assert result[0] == {"date": "2022-01-01", "value": 100, "billing_plan": "Enterprise"}
    mock_load_metric_values_from_cube.assert_awaited_with(
        MetricDetail.parse_obj(metric), None, date(2022, 1, 1), date(2022, 1, 31), ["billing_plan"]
    )


@pytest.mark.asyncio
async def test_get_metric_values_for_month_grain(mocker, metric, query_client):
    # Mock response from cube
    client = await query_client
    mock_load_metric_values_from_cube = AsyncMock(
        return_value=[{"date": "2022-01-01", "value": 100}, {"date": "2022-01-02", "value": 200}]
    )
    mocker.patch.object(client.cube_client, "load_metric_values_from_cube", mock_load_metric_values_from_cube)
    mocker.patch.object(client, "get_metric_details", AsyncMock(return_value=metric))

    result = await client.get_metric_values(
        "metric_id", grain=Granularity.MONTH, start_date=date(2022, 1, 1), end_date=date(2022, 2, 1)
    )

    assert len(result) == 2
    assert result[0] == {"date": "2022-01-01", "value": 100}
    assert result[1] == {"date": "2022-01-02", "value": 200}
    mock_load_metric_values_from_cube.assert_awaited_with(
        metric, Granularity.MONTH, date(2022, 1, 1), date(2022, 2, 1), []
    )


@pytest.mark.asyncio
async def test_create_metric(mocker, query_client):
    client = await query_client
    metric_data = dict(
        metric_id="new_metric",
        label="New Metric",
        abbreviation="NM",
        unit_of_measure="Count",
        unit="#",
        terms=["term1", "term2"],
        complexity=Complexity.ATOMIC,
        metric_expression=None,
        periods=[Granularity.DAY, Granularity.WEEK],
        grain_aggregation="sum",
        aggregations=["sum"],
        owned_by_team=["team1"],
        definition="New metric definition",
        metadata={"some": "metadata"},
        aim=None,
    )
    metric_create = MetricCreate(**metric_data)
    expected_metric = Metric(id=1, **metric_data)

    mock_create = AsyncMock(return_value=expected_metric)
    mocker.patch.object(MetricCreate, "create", mock_create)

    result = await client.create_metric(metric_create)

    assert result == expected_metric
    mock_create.assert_called_once()


@pytest.mark.asyncio
async def test_update_metric(mocker, query_client, metric):
    client = await query_client
    metric = Metric.model_validate(metric)
    update_data = MetricUpdate(label="Updated Metric")

    expected_metric = Metric(**{**metric.dict(), "label": "Updated Metric"})
    mock_update = AsyncMock(return_value=expected_metric)
    mocker.patch.object(MetricUpdate, "update", mock_update)

    result = await client.update_metric(metric.metric_id, update_data)

    assert result == expected_metric
    mock_update.assert_called_once()


@pytest.mark.asyncio
async def test_update_metric_not_found(mocker, query_client):
    client = await query_client
    mock_get_by_metric_id = AsyncMock(return_value=None)
    mocker.patch.object(client.metric_crud, "get_by_metric_id", mock_get_by_metric_id)

    with pytest.raises(MetricNotFoundError):
        await client.update_metric("non_existent_metric", MetricUpdate(label="Updated Metric"))


@pytest.mark.asyncio
async def test_create_dimension(mocker, query_client):
    client = await query_client
    dimension_data = DimensionCreate(
        dimension_id="new_dimension",
        label="New Dimension",
        definition="New dimension definition",
        reference="new_dimension",
        meta_data={"semantic_meta": {"cube": "cube1", "member": "new_dimension", "member_type": "dimension"}},
    )
    expected_dimension = Dimension(id=1, **dimension_data.model_dump())

    mock_create = AsyncMock(return_value=expected_dimension)
    mocker.patch.object(DimensionCreate, "create", mock_create)

    result = await client.create_dimension(dimension_data)

    assert result == expected_dimension
    mock_create.assert_called_once_with(client.dimensions_crud.session, dimension_data.model_dump())


@pytest.mark.asyncio
async def test_update_dimension(mocker, query_client, dimensions):
    client = await query_client
    update_data = DimensionUpdate(
        label="Updated Dimension",
        definition="New dimension definition",
        reference="new_dimension",
        meta_data={"semantic_meta": {"cube": "cube1", "member": "new_dimension", "member_type": "dimension"}},
    )
    expected_dimension = Dimension(**{**dimensions.dict(), "label": "Updated Dimension"})
    mock_update = AsyncMock(return_value=expected_dimension)
    mocker.patch.object(DimensionUpdate, "update", mock_update)

    result = await client.update_dimension(dimensions.dimension_id, update_data)

    assert result == expected_dimension
    mock_update.assert_called_once()


@pytest.mark.asyncio
async def test_update_dimension_not_found(mocker, query_client):
    client = await query_client
    mock_get_by_dimension_id = AsyncMock(return_value=None)
    mocker.patch.object(client.dimensions_crud, "get_by_dimension_id", mock_get_by_dimension_id)

    update_data = DimensionUpdate(
        label="Updated Dimension",
        definition="New dimension definition",
        reference="new_dimension",
        meta_data={"semantic_meta": {"cube": "cube1", "member": "new_dimension", "member_type": "dimension"}},
    )

    with pytest.raises(DimensionNotFoundError):
        await client.update_dimension("non_existent_dimension", update_data)


@pytest.mark.asyncio
async def test_list_metrics_with_metric_label_filter(mocker, metric, query_client):
    client = await query_client
    metric2 = deepcopy(metric)
    metric2["id"] = 2
    metric2["metric_id"] = "metric_id2"
    mock_paginate = AsyncMock(return_value=([metric2], 1))
    mocker.patch.object(client.metric_crud, "paginate", mock_paginate)

    params = PaginationParams(page=1, size=10)
    result, count = await client.list_metrics(metric_ids=None, metric_label="metric_id2", params=params)
    assert len(result) == 1
    assert result[0] == metric2
    assert count == 1

    mock_paginate.assert_called_once_with(params, filter_params={"metric_ids": None, "metric_label": "metric_id2"})


@pytest.mark.asyncio
async def test_list_metrics_with_fuzzy_label_search(mocker, metric, query_client):
    client = await query_client
    metric2 = deepcopy(metric)
    metric2["id"] = 2
    metric2["metric_id"] = "metric_id2"
    mock_paginate = AsyncMock(return_value=([metric2], 1))
    mocker.patch.object(client.metric_crud, "paginate", mock_paginate)

    params = PaginationParams(page=1, size=10)
    result, count = await client.list_metrics(metric_ids=None, metric_label="met", params=params)
    assert len(result) == 1
    assert result[0] == metric2
    assert count == 1

    mock_paginate.assert_called_once_with(params, filter_params={"metric_ids": None, "metric_label": "met"})
