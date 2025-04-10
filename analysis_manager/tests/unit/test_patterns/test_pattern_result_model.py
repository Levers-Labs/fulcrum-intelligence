"""Tests for the PatternResult model."""

from unittest.mock import patch

import pytest

from analysis_manager.patterns.models.pattern_result import PatternResult
from commons.utilities.context import set_tenant_id
from levers.models.common import AnalysisWindow, Granularity
from levers.models.patterns.performance_status import MetricGVAStatus, MetricPerformance

pytestmark = pytest.mark.asyncio


@pytest.fixture(name="pattern_result")
def pattern_result_data(jwt_payload):
    """Fixture to create pattern result test data."""
    # Create a MetricPerformance instance as sample pattern data
    performance_status = MetricPerformance(
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
        analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2023-01-01", end_date="2023-01-31"),
    )

    # Convert to dict for JSON storage
    run_result = performance_status.model_dump(mode="json")

    # Create a PatternResult instance
    return PatternResult(
        tenant_id=jwt_payload["tenant_id"],
        metric_id=performance_status.metric_id,
        pattern=performance_status.pattern,
        version=performance_status.version,
        analysis_date=performance_status.analysis_date,
        analysis_window=performance_status.analysis_window,
        run_result=run_result,
    )


@pytest.mark.parametrize(
    "missing_field,expected_in_data",
    [
        ("pattern", True),  # pattern missing in run_result, should be added
        (None, False),  # No missing fields, nothing should be added
    ],
)
async def test_to_pattern_model(pattern_result, missing_field, expected_in_data):
    """Test the to_pattern_model method with different scenarios."""
    # Arrange
    if missing_field:
        pattern_result.run_result.pop(missing_field, None)

    # Mock the Levers.load_pattern_model method
    with patch("levers.Levers.load_pattern_model") as mock_load:
        # Act
        PatternResult.to_pattern_model(pattern_result)

        # Assert
        # Check that the correct data was passed to load_pattern_model
        call_args = mock_load.call_args[0][0]

        if missing_field == "pattern" and expected_in_data:
            assert "pattern" in call_args
            assert call_args["pattern"] == pattern_result.pattern


async def test_create_and_retrieve_pattern_result(db_session, jwt_payload):
    """Test creation and retrieval of a pattern result."""
    # Arrange
    set_tenant_id(jwt_payload["tenant_id"])

    # Create test performance status data
    performance_status = MetricPerformance(
        metric_id="test_metric_for_db",
        pattern="performance_status",
        current_value=100.0,
        prior_value=90.0,
        target_value=110.0,
        status=MetricGVAStatus.ON_TRACK,
        absolute_delta_from_prior=10.0,
        pop_change_percent=0.11,
        absolute_gap=10.0,
        percent_gap=0.09,
        analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2023-01-01", end_date="2023-01-31"),
    )

    run_result = performance_status.model_dump(mode="json")

    # Create PatternResult
    pattern_result = PatternResult(
        tenant_id=jwt_payload["tenant_id"],
        metric_id=performance_status.metric_id,
        pattern=performance_status.pattern,
        version=performance_status.version,
        analysis_date=performance_status.analysis_date,
        analysis_window=run_result["analysis_window"],
        run_result=run_result,
    )

    # Act - Create
    db_session.add(pattern_result)
    await db_session.commit()
    await db_session.refresh(pattern_result)

    # Assert - Create
    assert pattern_result.id is not None
    assert pattern_result.metric_id == performance_status.metric_id
    assert pattern_result.pattern == performance_status.pattern

    # Act - Retrieve
    # Clear the session to ensure we're retrieving from the database
    db_session.expunge_all()

    # Query the database
    from sqlalchemy import select

    query = select(PatternResult).where(
        PatternResult.metric_id == performance_status.metric_id, PatternResult.tenant_id == jwt_payload["tenant_id"]
    )
    result = await db_session.execute(query)
    retrieved = result.scalars().first()

    # Assert - Retrieve
    assert retrieved is not None
    assert retrieved.metric_id == performance_status.metric_id
    assert retrieved.pattern == performance_status.pattern
    assert retrieved.run_result["current_value"] == 100.0
    assert retrieved.run_result["prior_value"] == 90.0
    assert retrieved.run_result["status"] == "on_track"
