"""Tests for the generic PatternCRUD operations."""

from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import select

from analysis_manager.patterns.crud import PatternCRUD
from analysis_manager.patterns.models.pattern_result import PatternResult
from commons.utilities.context import set_tenant_id
from levers.models.common import AnalysisWindow, Granularity
from levers.models.patterns.performance_status import MetricGVAStatus, MetricPerformance

pytestmark = pytest.mark.asyncio


@pytest.fixture
def pattern_data():
    """Create test pattern data."""
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
def pattern_crud(db_session):
    """Create a pattern CRUD instance."""
    return PatternCRUD(PatternResult, session=db_session)


async def test_store_pattern_result(pattern_data, pattern_crud, jwt_payload):
    """Test storing a pattern result."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    pattern_name = "performance_status"

    # Act
    result = await pattern_crud.store_pattern_result(pattern_name, pattern_data)

    # Assert
    assert result is not None
    assert result.id is not None
    assert result.tenant_id == jwt_payload["tenant_id"]
    assert result.metric_id == pattern_data.metric_id
    assert result.pattern == pattern_name
    assert result.run_result is not None
    assert result.run_result["current_value"] == pattern_data.current_value
    assert result.run_result["prior_value"] == pattern_data.prior_value
    assert result.run_result["target_value"] == pattern_data.target_value
    assert result.run_result["status"] == pattern_data.status


async def test_get_latest_for_metric(pattern_data, pattern_crud, jwt_payload):
    """Test getting the latest pattern result for a metric."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    pattern_name = "performance_status"
    # Change the version to avoid conflict
    pattern_data.version = "2.0"

    # Store multiple pattern results with different dates
    res = await pattern_crud.store_pattern_result(pattern_name, pattern_data)

    # Create a newer result
    newer_data = MetricPerformance(
        metric_id=pattern_data.metric_id,
        pattern="performance_status",
        version="2.0",
        current_value=105.0,  # Different value to distinguish
        prior_value=90.0,
        target_value=110.0,
        status=MetricGVAStatus.ON_TRACK,
        analysis_window=pattern_data.analysis_window,
        analysis_date=res.analysis_date + timedelta(days=1),
    )

    # Store with current timestamp (will be newer)
    res = await pattern_crud.store_pattern_result(pattern_name, newer_data)

    # Act
    result = await pattern_crud.get_latest_for_metric(pattern_name, pattern_data.metric_id)

    # Assert
    assert result is not None
    assert result.metric_id == pattern_data.metric_id
    assert result.pattern == pattern_name
    assert result.run_result["current_value"] == 105.0  # Should get the newer value


async def test_get_results_for_metric(pattern_data, pattern_crud, jwt_payload):
    """Test getting multiple pattern results for a metric."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    pattern_name = "performance_status"

    # Create and store multiple pattern results
    for i in range(3):
        data = MetricPerformance(
            metric_id=pattern_data.metric_id,
            pattern="performance_status",
            version=f"{i}.0",
            current_value=100.0 + i,  # Different values to distinguish
            prior_value=90.0,
            target_value=110.0,
            status=MetricGVAStatus.ON_TRACK,
            analysis_window=pattern_data.analysis_window,
        )
        await pattern_crud.store_pattern_result(pattern_name, data)

    # Act
    results = await pattern_crud.get_results_for_metric(pattern_name, pattern_data.metric_id, limit=5)

    # Assert
    assert results is not None
    assert len(results) == 3
    # Results should be in descending order by analysis_date
    assert results[0].run_result["current_value"] == 100
    assert results[1].run_result["current_value"] == 101
    assert results[2].run_result["current_value"] == 102


async def test_to_pattern_model(pattern_data, pattern_crud, jwt_payload):
    """Test converting a database model to a pattern model."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    pattern_name = "performance_status"
    stored = await pattern_crud.store_pattern_result(pattern_name, pattern_data)

    # Act
    res = pattern_crud.to_pattern_model(stored)

    # Assert
    assert res is not None
    assert isinstance(res, MetricPerformance)
    assert res.metric_id == pattern_data.metric_id


async def test_clear_data(pattern_data, pattern_crud, jwt_payload):
    """Test clearing pattern data."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    pattern_name = "performance_status"
    metric_id = pattern_data.metric_id

    # Store a pattern result
    await pattern_crud.store_pattern_result(pattern_name, pattern_data)

    # Act - Clear without any filters except metric_id
    result = await pattern_crud.clear_data(metric_id)

    # Assert
    assert result["success"] is True
    assert result["rows_deleted"] == 1

    # Verify data is gone
    db_result = await pattern_crud.get_latest_for_metric(pattern_name, metric_id)
    assert db_result is None


async def test_clear_data_with_pattern_filter(pattern_data, pattern_crud, jwt_payload):
    """Test clearing pattern data with pattern filter."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    metric_id = pattern_data.metric_id

    # Store pattern results for different patterns
    await pattern_crud.store_pattern_result("performance_status", pattern_data)

    # Create data for a different pattern
    historical_data = MetricPerformance(
        metric_id=metric_id,
        pattern="historical_performance",  # Different pattern
        current_value=100.0,
        prior_value=90.0,
        target_value=110.0,
        status=MetricGVAStatus.ON_TRACK,
        analysis_window=pattern_data.analysis_window,
    )
    await pattern_crud.store_pattern_result("historical_performance", historical_data)

    # Act - Clear only performance_status pattern
    result = await pattern_crud.clear_data(metric_id, pattern_name="performance_status")

    # Assert
    assert result["success"] is True
    assert result["rows_deleted"] == 1

    # Verify performance_status is gone but historical_performance remains
    ps_result = await pattern_crud.get_latest_for_metric("performance_status", metric_id)
    hp_result = await pattern_crud.get_latest_for_metric("historical_performance", metric_id)

    assert ps_result is None
    assert hp_result is not None


async def test_clear_data_with_date_range(pattern_data, pattern_crud, jwt_payload):
    """Test clearing pattern data with date range filter."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    pattern_name = "performance_status"
    metric_id = pattern_data.metric_id

    # Create pattern result with specific dates
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    two_days_ago = now - timedelta(days=2)

    # Insert test data with specific dates
    # First, manually create a PatternResult for yesterday
    yesterday_result = PatternResult(
        tenant_id=jwt_payload["tenant_id"],
        metric_id=metric_id,
        pattern=pattern_name,
        analysis_date=yesterday.date(),
        run_result=pattern_data.model_dump(mode="json"),
    )
    pattern_crud.session.add(yesterday_result)

    # Create another one for two days ago
    two_days_ago_result = PatternResult(
        tenant_id=jwt_payload["tenant_id"],
        metric_id=metric_id,
        pattern=pattern_name,
        analysis_date=two_days_ago.date(),
        run_result=pattern_data.model_dump(mode="json"),
    )
    pattern_crud.session.add(two_days_ago_result)

    # Commit both
    await pattern_crud.session.commit()

    # Act - Clear only data from yesterday
    result = await pattern_crud.clear_data(
        metric_id,
        pattern_name=pattern_name,
        start_date=yesterday.date(),  # Slightly before yesterday
        end_date=yesterday.date(),  # Slightly after yesterday
    )

    # Assert
    assert result["success"] is True
    assert result["rows_deleted"] == 1

    # Verify only yesterday's data is gone
    # Query the database directly
    query = select(PatternResult).where(
        PatternResult.tenant_id == jwt_payload["tenant_id"],
        PatternResult.metric_id == metric_id,
        PatternResult.pattern == pattern_name,
    )
    result = await pattern_crud.session.execute(query)
    remaining = list(result.scalars().all())

    assert len(remaining) == 1
    assert remaining[0].analysis_date == two_days_ago.date()


async def test_store_pattern_result_with_error(pattern_crud, jwt_payload, db_session):
    """Test database error handling when storing a pattern result."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])

    # Create a mock pattern result that will cause an error
    # due to missing required fields
    invalid_pattern_data = {}

    # Act & Assert
    with pytest.raises(AttributeError):
        await pattern_crud.store_pattern_result("test_pattern", invalid_pattern_data)
