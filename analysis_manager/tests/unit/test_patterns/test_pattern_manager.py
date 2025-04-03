"""Tests for the pattern manager."""

from datetime import date

import pytest

from analysis_manager.patterns.crud.performance_status import PerformanceStatusCRUD
from analysis_manager.patterns.manager import PatternManager
from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from levers.models.common import AnalysisWindow
from levers.models.patterns.performance_status import MetricGVAStatus, MetricPerformance

pytestmark = pytest.mark.asyncio


@pytest.fixture
def performance_status_model():
    """Create a performance status model."""
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
    )


@pytest.fixture
def pattern_manager(db_session):
    """Create a pattern manager."""
    return PatternManager(db_session)


async def test_store_pattern_result(performance_status_model, pattern_manager, jwt_payload):
    """Test the store pattern result method."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    # Act
    result = await pattern_manager.store_pattern_result("performance_status", performance_status_model)
    # Assert
    assert result is not None
    assert result.metric_id == performance_status_model.metric_id
    assert result.pattern == performance_status_model.pattern
    assert result.current_value == performance_status_model.current_value
    assert result.prior_value == performance_status_model.prior_value
    assert result.target_value == performance_status_model.target_value
    assert result.status == performance_status_model.status


async def test_get_latest_pattern_result(performance_status_model, pattern_manager, jwt_payload):
    """Test the get latest pattern result method."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])
    # Store the pattern result
    await pattern_manager.store_pattern_result("performance_status", performance_status_model)
    # Act
    result = await pattern_manager.get_latest_pattern_result("performance_status", performance_status_model.metric_id)
    # Assert
    assert result is not None
    assert result.metric_id == performance_status_model.metric_id
    assert result.pattern == performance_status_model.pattern
    assert result.current_value == performance_status_model.current_value


async def test_get_pattern_crud(pattern_manager):
    """Test the get pattern crud method."""
    # Act
    result = pattern_manager._get_pattern_crud("performance_status")
    # Assert
    assert result is not None
    assert result.__class__ == PerformanceStatusCRUD


async def test_get_pattern_crud_unknown_pattern(pattern_manager):
    """Test the get pattern crud method with an unknown pattern."""
    # Act
    with pytest.raises(ValueError):
        pattern_manager._get_pattern_crud("unknown_pattern")
