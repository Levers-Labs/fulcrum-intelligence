"""Tests for the performance status CRUD operations."""

from datetime import date

import pytest

from analysis_manager.patterns.crud.performance_status import PerformanceStatusCRUD
from commons.utilities.context import set_tenant_id
from levers.models.common import AnalysisWindow, Granularity
from levers.models.patterns.performance_status import MetricGVAStatus, MetricPerformance

pytestmark = pytest.mark.asyncio


@pytest.fixture
def performance_status_data():
    """Create test performance status data."""
    return MetricPerformance(
        metric_id="test_metric",
        pattern="performance_status",
        current_value=100.0,
        prior_value=90.0,
        target_value=110.0,
        status=MetricGVAStatus.ON_TRACK,
        absolute_delta_from_prior=10.0,
        pop_change_percent=0.11,
        absolute_gap=10.0,
        percent_gap=0.09,
        analysis_date=date.today(),
        analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2023-01-01", end_date="2023-01-31"),
        num_periods=31,
    )


@pytest.fixture
def patterns_crud(db_session):
    """Create a patterns CRUD instance."""
    return PerformanceStatusCRUD(session=db_session)


async def test_performance_status_crud(performance_status_data, patterns_crud, jwt_payload):
    """Test performance status CRUD."""
    # Prepare the data
    set_tenant_id(jwt_payload["tenant_id"])
    # Act
    result = await patterns_crud.store_pattern_result(performance_status_data)
    # Assert
    assert result is not None
    assert result.id is not None
    assert result.metric_id == performance_status_data.metric_id
    assert result.pattern == performance_status_data.pattern
    assert result.current_value == performance_status_data.current_value
    assert result.prior_value == performance_status_data.prior_value
    assert result.target_value == performance_status_data.target_value
    assert result.status == performance_status_data.status


async def test_performance_status_crud_get_by_metric(performance_status_data, patterns_crud, jwt_payload):
    """Test performance status CRUD get by metric."""
    # Prepare the data
    set_tenant_id(jwt_payload["tenant_id"])
    # Insert the data
    await patterns_crud.store_pattern_result(performance_status_data)
    # Act
    result = await patterns_crud.get_by_metric(performance_status_data.metric_id)
    # Assert
    assert result is not None
    assert len(result) == 1
    assert result[0].metric_id == performance_status_data.metric_id
    assert result[0].pattern == performance_status_data.pattern
    assert result[0].current_value == performance_status_data.current_value


async def test_performance_status_crud_get_latest_for_metric(performance_status_data, patterns_crud, jwt_payload):
    """Test performance status CRUD get latest for metric."""
    # Prepare the data
    set_tenant_id(jwt_payload["tenant_id"])
    # Insert the data
    await patterns_crud.store_pattern_result(performance_status_data)
    # Act
    result = await patterns_crud.get_latest_for_metric(performance_status_data.metric_id)
    # Assert
    assert result is not None
    assert result.metric_id == performance_status_data.metric_id
    assert result.pattern == performance_status_data.pattern
    assert result.current_value == performance_status_data.current_value
    assert result.prior_value == performance_status_data.prior_value
    assert result.target_value == performance_status_data.target_value
    assert result.status == performance_status_data.status


async def test_performance_status_crud_get_by_metric_and_date(performance_status_data, patterns_crud, jwt_payload):
    """Test performance status CRUD get by metric and date."""
    # Prepare the data
    set_tenant_id(jwt_payload["tenant_id"])
    # Insert the data
    await patterns_crud.store_pattern_result(performance_status_data)
    # Act
    result = await patterns_crud.get_by_metric_and_date(
        performance_status_data.metric_id, performance_status_data.analysis_date
    )
    # Assert
    assert result is not None
    assert result.metric_id == performance_status_data.metric_id
    assert result.pattern == performance_status_data.pattern
    assert result.analysis_date == performance_status_data.analysis_date
    assert result.current_value == performance_status_data.current_value
    assert result.prior_value == performance_status_data.prior_value
    assert result.target_value == performance_status_data.target_value
    assert result.status == performance_status_data.status


async def test_performance_status_crud_clear_data(performance_status_data, patterns_crud, jwt_payload):
    """Test performance status CRUD clear data."""
    # Prepare the data
    set_tenant_id(jwt_payload["tenant_id"])
    # Insert the data
    await patterns_crud.store_pattern_result(performance_status_data)
    # Act
    result = await patterns_crud.clear_data(performance_status_data.metric_id)
    # Assert
    assert result is not None
    assert result["rows_deleted"] == 1


async def test_performance_status_crud_bulk_upsert(performance_status_data, patterns_crud, jwt_payload):
    """Test performance status CRUD bulk upsert."""
    # Prepare the data
    set_tenant_id(jwt_payload["tenant_id"])
    # Insert the data
    await patterns_crud.store_pattern_result(performance_status_data)
    dict_data = performance_status_data.model_dump()
    dict_data.pop("pattern_run_id", None)
    # Act
    result = await patterns_crud.bulk_upsert([dict_data])
    # Assert
    assert result is not None
    assert result["processed"] == 1
    assert result["failed"] == 0
