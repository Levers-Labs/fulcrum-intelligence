"""
Tests for the historical performance evaluator.
"""

import pytest

from levers.models import (
    AnalysisWindow,
    Granularity,
    TrendExceptionType,
    TrendType,
)
from levers.models.patterns import (
    BenchmarkComparison,
    GrowthStats,
    HistoricalPerformance,
    RankSummary,
    Seasonality,
    TrendException,
    TrendInfo,
)
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator.evaluators import HistoricalPerformanceEvaluator


@pytest.fixture
def mock_historical_performance():
    """Fixture for mock historical performance."""
    return HistoricalPerformance(
        pattern="historical_performance",
        pattern_run_id=1,
        id=1,
        metric_id="test_metric",
        analysis_date="2024-01-01",
        analysis_window=AnalysisWindow(grain=Granularity.MONTH, start_date="2023-01-01", end_date="2024-01-01"),
        period_metrics=[],
        growth_stats=GrowthStats(
            current_pop_growth=5.0,
            average_pop_growth=3.0,
            current_growth_acceleration=2.0,
            num_periods_accelerating=3,
            num_periods_slowing=0,
        ),
        current_trend=TrendInfo(
            trend_type=TrendType.UPWARD,
            start_date="2023-10-01",
            average_pop_growth=5.0,
            duration_grains=3,
        ),
        previous_trend=TrendInfo(
            trend_type=TrendType.PLATEAU,
            start_date="2023-07-01",
            average_pop_growth=1.0,
            duration_grains=3,
        ),
        high_rank=RankSummary(
            value=100.0,
            rank=1,
            duration_grains=12,
            prior_record_value=95.0,
            prior_record_date="2023-06-01",
            absolute_delta_from_prior_record=5.0,
            relative_delta_from_prior_record=5.26,
        ),
        low_rank=RankSummary(
            value=80.0,
            rank=4,
            duration_grains=12,
            prior_record_value=75.0,
            prior_record_date="2023-03-01",
            absolute_delta_from_prior_record=5.0,
            relative_delta_from_prior_record=6.67,
        ),
        seasonality=Seasonality(
            is_following_expected_pattern=True,
            expected_change_percent=4.0,
            actual_change_percent=4.2,
            deviation_percent=0.2,
        ),
        benchmark_comparison=BenchmarkComparison(
            reference_period="week",
            absolute_change=10.0,
            change_percent=11.1,
        ),
        trend_exception=None,
        grain=Granularity.MONTH,
    )


@pytest.fixture
def mock_metric():
    """Fixture for mock metric."""
    return {"label": "Test Metric", "value": 100.0}


@pytest.fixture
def evaluator():
    """Fixture for evaluator."""
    return HistoricalPerformanceEvaluator()


@pytest.mark.asyncio
async def test_evaluate_accelerating_growth(mock_historical_performance, mock_metric):
    """Test evaluate method with accelerating growth."""
    evaluator = HistoricalPerformanceEvaluator()
    mock_historical_performance.growth_stats.current_growth_acceleration = 8.0

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.ACCELERATING_GROWTH for s in stories)
    assert any(s["story_group"] == StoryGroup.GROWTH_RATES for s in stories)

    accelerating_story = next(s for s in stories if s["story_type"] == StoryType.ACCELERATING_GROWTH)
    assert accelerating_story["genre"] == StoryGenre.GROWTH
    assert "growth is speeding up" in accelerating_story["title"]
    assert "growth has accelerated" in accelerating_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_slowing_growth(mock_historical_performance, mock_metric):
    """Test evaluate method with slowing growth."""
    evaluator = HistoricalPerformanceEvaluator()
    mock_historical_performance.growth_stats.current_growth_acceleration = -8.0
    mock_historical_performance.growth_stats.num_periods_slowing = 2

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.SLOWING_GROWTH for s in stories)
    slowing_story = next(s for s in stories if s["story_type"] == StoryType.SLOWING_GROWTH)
    assert slowing_story["genre"] == StoryGenre.GROWTH
    assert "Growth is slowing down" in slowing_story["title"]
    assert "growth has slowed" in slowing_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_new_upward_trend(mock_historical_performance, mock_metric):
    """Test evaluate method with new upward trend."""
    evaluator = HistoricalPerformanceEvaluator()

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.NEW_UPWARD_TREND for s in stories)
    upward_trend_story = next(s for s in stories if s["story_type"] == StoryType.NEW_UPWARD_TREND)
    assert upward_trend_story["genre"] == StoryGenre.TRENDS
    assert "New upward trend" in upward_trend_story["title"]
    assert "has been following a new, upward trend line" in upward_trend_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_new_downward_trend(mock_historical_performance, mock_metric):
    """Test evaluate method with new downward trend."""
    evaluator = HistoricalPerformanceEvaluator()
    mock_historical_performance.current_trend.trend_type = TrendType.DOWNWARD
    mock_historical_performance.current_trend.average_pop_growth = -3.0

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.NEW_DOWNWARD_TREND for s in stories)
    downward_trend_story = next(s for s in stories if s["story_type"] == StoryType.NEW_DOWNWARD_TREND)
    assert downward_trend_story["genre"] == StoryGenre.TRENDS
    assert "New downward trend" in downward_trend_story["title"]
    assert "has been following a new, downward trend line" in downward_trend_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_stable_trend(mock_historical_performance, mock_metric):
    """Test evaluate method with stable trend."""
    evaluator = HistoricalPerformanceEvaluator()
    mock_historical_performance.previous_trend = None
    mock_historical_performance.current_trend.trend_type = TrendType.STABLE

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.STABLE_TREND for s in stories)
    stable_trend_story = next(s for s in stories if s["story_type"] == StoryType.STABLE_TREND)
    assert stable_trend_story["genre"] == StoryGenre.TRENDS
    assert "Following a stable trend" in stable_trend_story["title"]
    assert "continues to follow the trend line" in stable_trend_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_performance_plateau(mock_historical_performance, mock_metric):
    """Test evaluate method with performance plateau."""
    evaluator = HistoricalPerformanceEvaluator()
    mock_historical_performance.current_trend.trend_type = TrendType.PLATEAU

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.PERFORMANCE_PLATEAU for s in stories)
    plateau_story = next(s for s in stories if s["story_type"] == StoryType.PERFORMANCE_PLATEAU)
    assert plateau_story["genre"] == StoryGenre.TRENDS
    assert "Performance has leveled off" in plateau_story["title"]
    assert "growth has steadied into a new normal" in plateau_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_spike(mock_historical_performance, mock_metric):
    """Test evaluate method with spike."""
    evaluator = HistoricalPerformanceEvaluator()
    mock_historical_performance.trend_exception = TrendException(
        type=TrendExceptionType.SPIKE,
        current_value=120.0,
        normal_range_low=90.0,
        normal_range_high=110.0,
        absolute_delta_from_normal_range=10.0,
        magnitude_percent=9.09,
    )

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.SPIKE for s in stories)
    spike_story = next(s for s in stories if s["story_type"] == StoryType.SPIKE)
    assert spike_story["genre"] == StoryGenre.TRENDS
    assert "Performance spike above trend" in spike_story["title"]
    assert "above its normal range" in spike_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_drop(mock_historical_performance, mock_metric):
    """Test evaluate method with drop."""
    evaluator = HistoricalPerformanceEvaluator()
    mock_historical_performance.trend_exception = TrendException(
        type=TrendExceptionType.DROP,
        current_value=80.0,
        normal_range_low=90.0,
        normal_range_high=110.0,
        absolute_delta_from_normal_range=10.0,
        magnitude_percent=11.11,
    )

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.DROP for s in stories)
    drop_story = next(s for s in stories if s["story_type"] == StoryType.DROP)
    assert drop_story["genre"] == StoryGenre.TRENDS
    assert "Performance drop below trend" in drop_story["title"]
    assert "below its normal range" in drop_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_improving_performance(mock_historical_performance, mock_metric):
    """Test evaluate method with improving performance."""
    evaluator = HistoricalPerformanceEvaluator()

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.IMPROVING_PERFORMANCE for s in stories)
    improving_story = next(s for s in stories if s["story_type"] == StoryType.IMPROVING_PERFORMANCE)
    assert improving_story["genre"] == StoryGenre.TRENDS
    assert "Improved performance" in improving_story["title"]
    assert "has been averaging" in improving_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_worsening_performance(mock_historical_performance, mock_metric):
    """Test evaluate method with worsening performance."""
    evaluator = HistoricalPerformanceEvaluator()
    mock_historical_performance.current_trend.average_pop_growth = -3.0

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.WORSENING_PERFORMANCE for s in stories)
    worsening_story = next(s for s in stories if s["story_type"] == StoryType.WORSENING_PERFORMANCE)
    assert worsening_story["genre"] == StoryGenre.TRENDS
    assert "Worsening performance" in worsening_story["title"]
    assert "has been declining" in worsening_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_record_high(mock_historical_performance, mock_metric):
    """Test evaluate method with record high."""
    evaluator = HistoricalPerformanceEvaluator()

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.RECORD_HIGH for s in stories)
    record_high_story = next(s for s in stories if s["story_type"] == StoryType.RECORD_HIGH)
    assert record_high_story["genre"] == StoryGenre.TRENDS
    assert "highest" in record_high_story["title"]
    assert "highest value" in record_high_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_record_low(mock_historical_performance, mock_metric):
    """Test evaluate method with record low."""
    evaluator = HistoricalPerformanceEvaluator()
    mock_historical_performance.low_rank.rank = 2
    mock_historical_performance.high_rank.rank = 3

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.RECORD_LOW for s in stories)
    record_low_story = next(s for s in stories if s["story_type"] == StoryType.RECORD_LOW)
    assert record_low_story["genre"] == StoryGenre.TRENDS
    assert "lowest" in record_low_story["title"]
    assert "lowest value" in record_low_story["detail"]


@pytest.mark.asyncio
async def test_evaluate_benchmarks(mock_historical_performance, mock_metric):
    """Test evaluate method with benchmarks."""
    evaluator = HistoricalPerformanceEvaluator()

    stories = await evaluator.evaluate(mock_historical_performance, mock_metric)

    assert any(s["story_type"] == StoryType.BENCHMARKS for s in stories)
    benchmark_story = next(s for s in stories if s["story_type"] == StoryType.BENCHMARKS)
    assert benchmark_story["genre"] == StoryGenre.TRENDS
    assert "Performance Against Historical Benchmarks" in benchmark_story["title"]
    assert "This month marks the" in benchmark_story["detail"]


def test_populate_template_context(evaluator, mock_historical_performance, mock_metric):
    """Test _populate_template_context method."""
    context = evaluator._populate_template_context(mock_historical_performance, mock_metric, Granularity.MONTH)

    assert context["grain_label"] == "month"
    assert context["pop"] == "m/m"
    assert context["current_growth"] == 5.0
    assert context["average_growth"] == 3.0
    assert context["growth_acceleration"] == 2.0
    assert context["num_periods_accelerating"] == 3
    assert context["num_periods_slowing"] == 0
    assert context["trend_start_date"] == "2023-10-01"
    assert context["trend_duration"] == 3
    assert context["trend_avg_growth"] == 5.0
    assert context["trend_direction"] == "increase"
    assert context["prev_trend_duration"] == 3
    assert context["prev_trend_avg_growth"] == 1.0
    assert context["high_value"] == 100.0
    assert context["high_rank"] == 1
    assert context["high_duration"] == 12


def test_should_create_growth_story(evaluator, mock_historical_performance):
    """Test _should_create_growth_story method."""
    evaluator._should_create_growth_story = (
        lambda pattern_result: abs(pattern_result.growth_stats.current_growth_acceleration or 0) > 5.0
    )

    # Should return True when acceleration is significant
    mock_historical_performance.growth_stats.current_growth_acceleration = 8.0
    assert evaluator._should_create_growth_story(mock_historical_performance) is True

    # Should return True when deceleration is significant
    mock_historical_performance.growth_stats.current_growth_acceleration = -8.0
    assert evaluator._should_create_growth_story(mock_historical_performance) is True

    # Should return False when acceleration is not significant
    mock_historical_performance.growth_stats.current_growth_acceleration = 2.0
    assert evaluator._should_create_growth_story(mock_historical_performance) is False


def test_is_stable_trend(evaluator, mock_historical_performance):
    """Test _is_stable_trend method."""
    # Should return True when current trend exists, duration is sufficient, and no previous trend
    mock_historical_performance.previous_trend = None
    mock_historical_performance.current_trend.trend_type = TrendType.STABLE
    assert evaluator._is_stable_trend(mock_historical_performance) is True

    # Should return False when previous trend exists
    mock_historical_performance.previous_trend = TrendInfo(
        trend_type=TrendType.PLATEAU,
        start_date="2023-07-01",
        average_pop_growth=1.0,
        duration_grains=3,
    )
    assert evaluator._is_stable_trend(mock_historical_performance) is False


def test_is_new_trend(evaluator, mock_historical_performance):
    """Test _is_new_trend method."""
    # Should return True when current and previous trends exist with different types
    assert evaluator._is_new_trend(mock_historical_performance) is True

    # Should return False when trend types are the same
    mock_historical_performance.previous_trend.trend_type = TrendType.UPWARD
    assert evaluator._is_new_trend(mock_historical_performance) is False

    # Should return False when no previous trend
    mock_historical_performance.previous_trend = None
    assert evaluator._is_new_trend(mock_historical_performance) is False
