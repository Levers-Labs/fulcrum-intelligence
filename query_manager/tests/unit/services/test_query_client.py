from copy import deepcopy
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import Result
from sqlalchemy.ext.asyncio import AsyncSession

from commons.models.enums import Granularity
from commons.utilities.pagination import PaginationParams
from query_manager.core.dependencies import get_cube_client, get_query_client
from query_manager.core.models import Dimensions
from query_manager.core.schemas import MetricDetail
from query_manager.exceptions import MetricNotFoundError


@pytest.fixture
async def query_client():
    cube_client = await get_cube_client()
    client = await get_query_client(cube_client, session=AsyncSession)
    return client


# @pytest.mark.asyncio
# async def test_load_data(query_client):
#     query_client = await query_client
#     result = await query_client.load_data(query_client.metric_file_path)
#     assert len(result) > 0


# @pytest.mark.asyncio
# async def test_load_data_exception(query_client):
#     query_client = await query_client
#     with patch("query_manager.services.query_client.aiofiles.open") as mock_open:
#         mock_open.side_effect = FileNotFoundError
#         with pytest.raises(FileNotFoundError):
#             await query_client.load_data(query_client.metric_file_path)


@pytest.mark.asyncio
async def test_list_metrics(mocker, metric, query_client):
    query_client = await query_client

    # Mock the execute method of AsyncSession
    mock_result = mocker.Mock(spec=Result)
    mock_result.scalars.return_value.all.return_value = [metric]
    mock_execute = AsyncMock(return_value=mock_result)
    mocker.patch.object(query_client.session, "execute", mock_execute)

    params = PaginationParams(page=1, size=10)
    result = await query_client.list_metrics(params=params)
    assert len(result) == 1
    assert result[0] == metric

    # Ensure execute was called with the correct statement
    mock_execute.assert_called_once()


@pytest.mark.asyncio
async def test_list_metrics_with_metric_id_filter(mocker, metric, query_client):
    query_client = await query_client
    metric2 = deepcopy(metric)
    metric2["id"] = "metric_id2"
    mock_load_data = AsyncMock(return_value=[metric, metric2])
    mocker.patch.object(query_client, "load_data", mock_load_data)
    params = PaginationParams(page=1, size=10)
    result = await query_client.list_metrics(metric_ids=["metric_id2"], params=params)
    assert len(result) == 1
    assert result[0] == metric2


@pytest.mark.asyncio
async def test_get_metric_details(mocker, metric, query_client):
    query_client = await query_client

    # Mock the execute method of AsyncSession
    mock_result = mocker.Mock(spec=Result)
    mock_scalars = mocker.Mock()
    mock_scalars.one_or_none.return_value = metric
    mock_result.scalars.return_value = mock_scalars
    mock_execute = AsyncMock(return_value=mock_result)
    mocker.patch.object(query_client.session, "execute", mock_execute)

    result = await query_client.get_metric_details(metric["id"])
    assert result == metric

    # Ensure execute was called with the correct statement
    mock_execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_metric_details_not_found(mocker, metric, query_client):
    query_client = await query_client
    mock_load_data = AsyncMock(return_value=[metric])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    with pytest.raises(MetricNotFoundError):
        await query_client.get_metric_details("non_existent_metric_id")


@pytest.fixture
def dimensions():
    return Dimensions(
        id="billing_plan",
        label="Billing Plan",
        reference="billing_plan",
        definition="Billing Plan Definition",
        metadata={"semantic_meta": {"cube": "cube1", "member": "billing_plan", "member_type": "dimension"}},
        created_at=datetime(2024, 7, 24, 9, 54, 10, 411357),
        updated_at=datetime(2024, 7, 24, 9, 54, 10, 411359),
    )


@pytest.mark.asyncio
async def test_list_dimensions(mocker, dimensions, query_client):
    query_client = await query_client
    # Mock the execute method of AsyncSession
    mock_result = mocker.Mock(spec=Result)
    mock_result.scalars.return_value.all.return_value = [dimensions]
    mock_execute = AsyncMock(return_value=mock_result)
    mocker.patch.object(AsyncSession, "execute", mock_execute)

    params = PaginationParams(page=1, size=10)  # Adjust the parameters as needed
    result = await query_client.list_dimensions(params=params)
    assert len(result) == 1
    assert result[0] == dimensions.dict()

    # Ensure execute was called with the correct statement
    mock_execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_dimension_details(mocker, dimensions, query_client):
    query_client = await query_client

    # Mock the execute method of AsyncSession
    mock_result = mocker.Mock(spec=Result)
    mock_scalars = mocker.Mock()
    mock_scalars.one_or_none.return_value = dimensions
    mock_result.scalars.return_value = mock_scalars
    mock_execute = AsyncMock(return_value=mock_result)
    mocker.patch.object(query_client.session, "execute", mock_execute)

    result = await query_client.get_dimension_details("billing_plan")
    assert result == dimensions

    # Ensure execute was called with the correct statement
    mock_execute.assert_called_once()


@pytest.mark.asyncio
async def test_get_dimension_members(mocker, dimension, query_client):
    query_client = await query_client
    # Mock response from cube for dimension members
    mock_load_dimension_members_from_cube = AsyncMock(return_value=["Enterprise", "Basic"])
    mocker.patch.object(
        query_client.cube_client, "load_dimension_members_from_cube", mock_load_dimension_members_from_cube
    )

    mock_load_data = AsyncMock(return_value=[dimension])
    mocker.patch.object(query_client, "load_data", mock_load_data)

    result = await query_client.get_dimension_members(dimension["id"])
    assert result == ["Enterprise", "Basic"]

    # no dimension match
    result = await query_client.get_dimension_members("non_existent_dimension_id")
    assert result == []


@pytest.mark.asyncio
async def test_get_metric_targets_with_time_range(mocker, metric, query_client):
    query_client = await query_client
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
    mocker.patch.object(query_client.cube_client, "load_metric_targets_from_cube", mock_load_targets_from_cube)
    mocker.patch.object(query_client, "get_metric_details", AsyncMock(return_value=metric))

    # Act
    result = await query_client.get_metric_targets(metric["id"], date(2021, 1, 1), date(2021, 1, 31), Granularity.WEEK)

    # Assert
    assert len(result) == 1
    assert result[0]["metric_id"] == metric["id"]
    assert result[0]["grain"] == "week"
    assert result[0]["target_date"] == "2021-01-01"


@pytest.mark.asyncio
async def test_get_metric_targets_invalid_metric_id(mocker, query_client):
    query_client = await query_client
    mocker.patch.object(query_client, "get_metric_details", AsyncMock(return_value=None))
    with pytest.raises(MetricNotFoundError):
        await query_client.get_metric_targets("invalid_metric_id")


@pytest.mark.asyncio
async def test_get_metric_values_invalid_metric_id(mocker, query_client):
    query_client = await query_client
    mocker.patch.object(query_client, "get_metric_details", AsyncMock(return_value=None))
    with pytest.raises(MetricNotFoundError):
        await query_client.get_metric_values("invalid_metric_id")


@pytest.mark.asyncio
async def test_get_metric_values_without_dimensions(mocker, metric, query_client):
    query_client = await query_client
    # Mock response from cube for values without dimensions
    mock_load_metric_values_from_cube = AsyncMock(return_value=[{"date": "2022-01-01", "value": 200}])
    mocker.patch.object(query_client.cube_client, "load_metric_values_from_cube", mock_load_metric_values_from_cube)
    mocker.patch.object(query_client, "get_metric_details", AsyncMock(return_value=metric))

    result = await query_client.get_metric_values("metric_id", date(2022, 1, 1), date(2022, 1, 31), None)

    assert len(result) == 1
    assert result[0] == {"date": "2022-01-01", "value": 200}
    mock_load_metric_values_from_cube.assert_awaited_with(
        MetricDetail.parse_obj(metric), None, date(2022, 1, 1), date(2022, 1, 31), []
    )


@pytest.mark.asyncio
async def test_get_metric_values_with_dimensions(mocker, metric, query_client):
    query_client = await query_client
    # Mock response from cube for values with dimensions
    mock_load_metric_values_from_cube = AsyncMock(
        return_value=[{"date": "2022-01-01", "value": 100, "billing_plan": "Enterprise"}]
    )
    mocker.patch.object(query_client.cube_client, "load_metric_values_from_cube", mock_load_metric_values_from_cube)
    mocker.patch.object(query_client, "get_metric_details", AsyncMock(return_value=metric))
    result = await query_client.get_metric_values("metric_id", date(2022, 1, 1), date(2022, 1, 31), ["billing_plan"])

    assert len(result) == 1
    assert result[0] == {"date": "2022-01-01", "value": 100, "billing_plan": "Enterprise"}
    mock_load_metric_values_from_cube.assert_awaited_with(
        MetricDetail.parse_obj(metric), None, date(2022, 1, 1), date(2022, 1, 31), ["billing_plan"]
    )


@pytest.mark.asyncio
async def test_get_metric_values_for_month_grain(mocker, metric, query_client):
    query_client = await query_client
    # Mock response from cube
    mock_load_metric_values_from_cube = AsyncMock(
        return_value=[{"date": "2022-01-01", "value": 100}, {"date": "2022-01-02", "value": 200}]
    )
    mocker.patch.object(query_client.cube_client, "load_metric_values_from_cube", mock_load_metric_values_from_cube)
    mocker.patch.object(query_client, "get_metric_details", AsyncMock(return_value=metric))

    result = await query_client.get_metric_values(
        "metric_id", grain=Granularity.MONTH, start_date=date(2022, 1, 1), end_date=date(2022, 2, 1)
    )

    assert len(result) == 2
    assert result[0] == {"date": "2022-01-01", "value": 100}
    assert result[1] == {"date": "2022-01-02", "value": 200}
    mock_load_metric_values_from_cube.assert_awaited_with(
        MetricDetail.parse_obj(metric), Granularity.MONTH, date(2022, 1, 1), date(2022, 2, 1), []
    )
