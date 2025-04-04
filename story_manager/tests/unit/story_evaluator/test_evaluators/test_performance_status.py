"""
Tests for the performance status evaluator.
"""

import pytest

from commons.models.enums import Granularity
from levers.models.common import AnalysisWindow
from levers.models.patterns.performance_status import (
    HoldSteady,
    MetricGVAStatus,
    MetricPerformance,
    StatusChange,
    Streak,
)
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator.evaluators.performance_status import PerformanceStatusEvaluator


@pytest.fixture
def mock_metric_performance():
    """Fixture for mock metric performance."""
    return MetricPerformance(
        pattern="performance_status",
        id=1,
        metric_id="test_metric",
        analysis_date="2024-01-01",
        analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
        current_value=100.0,
        target_value=90.0,
        prior_value=95.0,
        pop_change_percent=5.0,
        status=MetricGVAStatus.ON_TRACK,
        percent_over_performance=10.0,
        percent_gap=-10.0,
        streak=Streak(length=3, status=MetricGVAStatus.ON_TRACK),
    )


@pytest.fixture
def mock_metric():
    """Fixture for mock metric."""
    return {"label": "Test Metric", "value": 100.0, "target": 90.0}


@pytest.fixture
def evaluator():
    """Fixture for evaluator."""
    return PerformanceStatusEvaluator()


@pytest.mark.asyncio
async def test_evaluate_on_track(mock_metric_performance, mock_metric):
    """Test evaluate method with on track performance."""
    evaluator = PerformanceStatusEvaluator()
    stories = await evaluator.evaluate(mock_metric_performance, mock_metric)
    assert len(stories) == 1
    assert stories[0]["title"] == "Test Metric is on track"
    assert stories[0]["genre"] == StoryGenre.PERFORMANCE
    assert stories[0]["story_type"] == StoryType.ON_TRACK
    assert stories[0]["story_group"] == StoryGroup.GOAL_VS_ACTUAL
    assert stories[0]["grain"] == Granularity.DAY
    assert stories[0]["metric_id"] == "test_metric"
    assert stories[0]["pattern_run_id"] == 1


@pytest.mark.asyncio
async def test_evaluate_at_risk(mock_metric_performance, mock_metric):
    """Test evaluate method with at risk performance."""
    mock_metric_performance.current_value = 85.0
    mock_metric_performance.prior_value = 88.0
    mock_metric_performance.pop_change_percent = -3.0
    mock_metric_performance.status = MetricGVAStatus.OFF_TRACK
    mock_metric_performance.percent_gap = 5.0
    mock_metric_performance.percent_over_performance = -5.0
    mock_metric_performance.streak = Streak(length=1, status=MetricGVAStatus.OFF_TRACK)

    evaluator = PerformanceStatusEvaluator()
    stories = await evaluator.evaluate(mock_metric_performance, mock_metric)

    assert len(stories) == 1
    assert stories[0]["title"] == "Test Metric is off track"
    assert stories[0]["story_type"] == StoryType.OFF_TRACK


@pytest.mark.asyncio
async def test_evaluate_off_track(mock_metric_performance, mock_metric):
    """Test evaluate method with off track performance."""
    mock_metric_performance.current_value = 75.0
    mock_metric_performance.prior_value = 85.0
    mock_metric_performance.pop_change_percent = -10.0
    mock_metric_performance.status = MetricGVAStatus.OFF_TRACK
    mock_metric_performance.percent_gap = 15.0
    mock_metric_performance.percent_over_performance = -15.0
    mock_metric_performance.streak = Streak(length=0, status=MetricGVAStatus.OFF_TRACK)

    evaluator = PerformanceStatusEvaluator()
    stories = await evaluator.evaluate(mock_metric_performance, mock_metric)

    assert len(stories) == 1
    assert stories[0]["title"] == "Test Metric is off track"
    assert stories[0]["story_type"] == StoryType.OFF_TRACK


@pytest.mark.asyncio
async def test_evaluate_no_trend(mock_metric_performance, mock_metric):
    """Test evaluate method with no trend data."""
    mock_metric_performance.prior_value = None
    mock_metric_performance.pop_change_percent = None

    evaluator = PerformanceStatusEvaluator()
    stories = await evaluator.evaluate(mock_metric_performance, mock_metric)

    assert len(stories) == 1  # Still generates on_track story


@pytest.mark.asyncio
async def test_evaluate_missing_target(mock_metric_performance, mock_metric):
    """Test evaluate method with missing target."""
    mock_metric_performance.target_value = None
    mock_metric_performance.status = MetricGVAStatus.NO_TARGET
    mock_metric_performance.percent_over_performance = None
    mock_metric_performance.percent_gap = None

    evaluator = PerformanceStatusEvaluator()
    stories = await evaluator.evaluate(mock_metric_performance, mock_metric)

    assert len(stories) == 0  # No stories generated when no target


@pytest.mark.asyncio
async def test_evaluate_different_grain(mock_metric_performance, mock_metric):
    """Test evaluate method with different grain."""
    mock_metric_performance.analysis_window = AnalysisWindow(
        grain=Granularity.WEEK, start_date="2024-01-01", end_date="2024-01-31"
    )

    evaluator = PerformanceStatusEvaluator()
    stories = await evaluator.evaluate(mock_metric_performance, mock_metric)

    assert len(stories) == 1
    assert stories[0]["grain"] == Granularity.WEEK


@pytest.mark.asyncio
async def test_evaluate_improving_status(mock_metric_performance, mock_metric):
    """Test evaluate method with improving status."""
    mock_metric_performance.status_change = StatusChange(
        has_flipped=True,
        old_status=MetricGVAStatus.OFF_TRACK,
        new_status=MetricGVAStatus.ON_TRACK,
        old_status_duration_grains=2,
    )

    evaluator = PerformanceStatusEvaluator()
    stories = await evaluator.evaluate(mock_metric_performance, mock_metric)

    assert len(stories) == 2  # One for on track, one for improving status
    assert any(s["story_type"] == StoryType.IMPROVING_STATUS for s in stories)
    assert any(s["story_group"] == StoryGroup.STATUS_CHANGE for s in stories)
    assert any("status has improved" in s["title"] for s in stories)
    assert any("now On-Track" in s["detail"] for s in stories)


@pytest.mark.asyncio
async def test_evaluate_worsening_status(mock_metric_performance, mock_metric):
    """Test evaluate method with worsening status."""
    mock_metric_performance.status = MetricGVAStatus.OFF_TRACK
    mock_metric_performance.percent_gap = 10.0
    mock_metric_performance.percent_over_performance = -10.0
    mock_metric_performance.status_change = StatusChange(
        has_flipped=True,
        old_status=MetricGVAStatus.ON_TRACK,
        new_status=MetricGVAStatus.OFF_TRACK,
        old_status_duration_grains=3,
    )

    evaluator = PerformanceStatusEvaluator()
    stories = await evaluator.evaluate(mock_metric_performance, mock_metric)

    assert len(stories) == 2  # One for off track, one for worsening status
    assert any(s["story_type"] == StoryType.WORSENING_STATUS for s in stories)
    assert any(s["story_group"] == StoryGroup.STATUS_CHANGE for s in stories)
    assert any("status has worsened" in s["title"] for s in stories)
    assert any("now Off-Track" in s["detail"] for s in stories)


@pytest.mark.asyncio
async def test_evaluate_hold_steady(mock_metric_performance, mock_metric):
    """Test evaluate method with hold steady."""
    mock_metric_performance.hold_steady = HoldSteady(
        is_currently_at_or_above_target=True,
        current_margin_percent=10.0,
        time_to_maintain_grains=1,
    )

    evaluator = PerformanceStatusEvaluator()
    stories = await evaluator.evaluate(mock_metric_performance, mock_metric)

    assert len(stories) == 2  # One for on track, one for hold steady
    assert any(s["story_type"] == StoryType.HOLD_STEADY for s in stories)
    assert any(s["story_group"] == StoryGroup.LIKELY_STATUS for s in stories)
    assert any("needs to hold steady" in s["title"] for s in stories)
    assert any("needs to maintain this lead" in s["detail"] for s in stories)


@pytest.mark.asyncio
async def test_evaluate_all_stories(mock_metric_performance, mock_metric):
    """Test evaluate method with all story types."""
    mock_metric_performance.status_change = StatusChange(
        has_flipped=True,
        old_status=MetricGVAStatus.OFF_TRACK,
        new_status=MetricGVAStatus.ON_TRACK,
        old_status_duration_grains=2,
    )
    mock_metric_performance.hold_steady = HoldSteady(
        is_currently_at_or_above_target=True,
        current_margin_percent=10.0,
        time_to_maintain_grains=1,
    )

    evaluator = PerformanceStatusEvaluator()
    stories = await evaluator.evaluate(mock_metric_performance, mock_metric)

    assert len(stories) == 3  # One for on track, one for improving status, one for hold steady
    assert any(s["story_type"] == StoryType.ON_TRACK for s in stories)
    assert any(s["story_type"] == StoryType.IMPROVING_STATUS for s in stories)
    assert any(s["story_type"] == StoryType.HOLD_STEADY for s in stories)


def test_populate_template_context(evaluator, mock_metric_performance, mock_metric):
    """Test _populate_template_context method."""
    context = evaluator._populate_template_context(mock_metric_performance, mock_metric, Granularity.DAY)

    assert context["current_value"] == 100.0
    assert context["target_value"] == 90.0
    assert context["performance_percent"] == 10.0
    assert context["gap_percent"] == 10.0
    assert context["change_percent"] == 5.0
    assert context["pop"] == "d/d"
    assert context["grain_label"] == "day"
    assert context["streak_length"] == 3
    assert context["trend_direction"] == "up"
    assert context["gap_trend"] == "is widening"
    assert context["performance_trend"] == "improving"


def test_create_on_track_story(evaluator, mock_metric_performance, mock_metric):
    """Test _create_on_track_story method."""
    story = evaluator._create_on_track_story(
        mock_metric_performance, mock_metric_performance.metric_id, mock_metric, Granularity.DAY
    )

    assert story["story_type"] == StoryType.ON_TRACK
    assert story["story_group"] == StoryGroup.GOAL_VS_ACTUAL
    assert "is on track" in story["title"]
    assert "beating its target" in story["detail"]


def test_create_off_track_story(evaluator, mock_metric_performance, mock_metric):
    """Test _create_off_track_story method."""
    mock_metric_performance.status = MetricGVAStatus.OFF_TRACK
    mock_metric_performance.percent_gap = 10.0
    mock_metric_performance.percent_over_performance = -10.0

    story = evaluator._create_off_track_story(
        mock_metric_performance, mock_metric_performance.metric_id, mock_metric, Granularity.DAY
    )

    assert story["story_type"] == StoryType.OFF_TRACK
    assert story["story_group"] == StoryGroup.GOAL_VS_ACTUAL
    assert "is off track" in story["title"]
    assert "missing its target" in story["detail"]


def test_create_improving_status_story(evaluator, mock_metric_performance, mock_metric):
    """Test _create_improving_status_story method."""
    mock_metric_performance.status_change = StatusChange(
        has_flipped=True,
        old_status=MetricGVAStatus.OFF_TRACK,
        new_status=MetricGVAStatus.ON_TRACK,
        old_status_duration_grains=2,
    )

    story = evaluator._create_improving_status_story(
        mock_metric_performance, mock_metric_performance.metric_id, mock_metric, Granularity.DAY
    )

    assert story["story_type"] == StoryType.IMPROVING_STATUS
    assert story["story_group"] == StoryGroup.STATUS_CHANGE
    assert "status has improved" in story["title"]
    assert "now On-Track" in story["detail"]


def test_create_worsening_status_story(evaluator, mock_metric_performance, mock_metric):
    """Test _create_worsening_status_story method."""
    mock_metric_performance.status = MetricGVAStatus.OFF_TRACK
    mock_metric_performance.status_change = StatusChange(
        has_flipped=True,
        old_status=MetricGVAStatus.ON_TRACK,
        new_status=MetricGVAStatus.OFF_TRACK,
        old_status_duration_grains=3,
    )

    story = evaluator._create_worsening_status_story(
        mock_metric_performance, mock_metric_performance.metric_id, mock_metric, Granularity.DAY
    )

    assert story["story_type"] == StoryType.WORSENING_STATUS
    assert story["story_group"] == StoryGroup.STATUS_CHANGE
    assert "status has worsened" in story["title"]
    assert "now Off-Track" in story["detail"]


def test_create_hold_steady_story(evaluator, mock_metric_performance, mock_metric):
    """Test _create_hold_steady_story method."""
    mock_metric_performance.hold_steady = HoldSteady(
        is_currently_at_or_above_target=True,
        current_margin_percent=10.0,
        time_to_maintain_grains=1,
    )

    story = evaluator._create_hold_steady_story(
        mock_metric_performance, mock_metric_performance.metric_id, mock_metric, Granularity.DAY
    )

    assert story["story_type"] == StoryType.HOLD_STEADY
    assert story["story_group"] == StoryGroup.LIKELY_STATUS
    assert "needs to hold steady" in story["title"]
    assert "needs to maintain this lead" in story["detail"]
