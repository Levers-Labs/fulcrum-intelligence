"""
Tests for forecasting story evaluator.
"""

from datetime import date

import pytest

from commons.models.enums import Granularity
from levers.models import AnalysisWindow
from levers.models.enums import MetricGVAStatus, PeriodType
from levers.models.forecasting import (
    Forecast,
    ForecastVsTargetStats,
    PacingProjection,
    RequiredPerformance,
)
from levers.models.patterns.forecasting import Forecasting
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator.evaluators.forecasting import ForecastingEvaluator


@pytest.fixture
def mock_metric():
    """Mock metric data."""
    return {
        "label": "Test Inquiries",
        "metric_id": "test_inquiries",
    }


@pytest.fixture
def mock_analysis_window():
    """Mock analysis window."""
    return AnalysisWindow(grain=Granularity.WEEK, start_date="2024-01-01", end_date="2024-01-31")


@pytest.fixture
def mock_forecast_vs_target_stats_on_track():
    """Mock forecast vs target stats for on-track scenario."""
    return ForecastVsTargetStats(
        forecasted_value=1200.0,
        lower_bound=1100.0,
        upper_bound=1300.0,
        confidence_level=0.95,
        target_date="2024-01-31",
        target_value=1000.0,
        forecasted_gap_percent=20.0,
        forecast_status=MetricGVAStatus.ON_TRACK,
    )


@pytest.fixture
def mock_forecast_vs_target_stats_off_track():
    """Mock forecast vs target stats for off-track scenario."""
    return ForecastVsTargetStats(
        forecasted_value=800.0,
        lower_bound=700.0,
        upper_bound=900.0,
        confidence_level=0.95,
        target_date="2024-01-31",
        target_value=1000.0,
        forecasted_gap_percent=-20.0,
        forecast_status=MetricGVAStatus.OFF_TRACK,
    )


@pytest.fixture
def mock_pacing_on_track():
    """Mock pacing projection for on-track scenario."""
    return PacingProjection(
        percent_of_period_elapsed=75.0,
        cumulative_value=900.0,
        projected_value=1200.0,
        gap_percent=20.0,
        status="on_track",
    )


@pytest.fixture
def mock_pacing_off_track():
    """Mock pacing projection for off-track scenario."""
    return PacingProjection(
        percent_of_period_elapsed=75.0,
        cumulative_value=600.0,
        projected_value=800.0,
        gap_percent=-20.0,
        status="off_track",
    )


@pytest.fixture
def mock_required_performance():
    """Mock required performance."""
    return RequiredPerformance(
        remaining_periods_count=2,
        required_pop_growth_percent=15.0,
        past_pop_growth_percent=10.0,
        delta_from_historical_growth=5.0,
    )


@pytest.fixture
def mock_period_forecast():
    """Mock period forecast."""
    return [
        Forecast(
            date="2024-01-29", forecasted_value=1100.0, lower_bound=1000.0, upper_bound=1200.0, confidence_level=0.95
        ),
        Forecast(
            date="2024-01-30", forecasted_value=1150.0, lower_bound=1050.0, upper_bound=1250.0, confidence_level=0.95
        ),
        Forecast(
            date="2024-01-31", forecasted_value=1200.0, lower_bound=1100.0, upper_bound=1300.0, confidence_level=0.95
        ),
    ]


@pytest.fixture
def forecasting_evaluator():
    """Forecasting evaluator instance."""
    return ForecastingEvaluator()


class TestForecastingEvaluator:
    """Test cases for ForecastingEvaluator."""

    @pytest.mark.asyncio
    async def test_evaluate_forecasted_on_track(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_on_track,
        mock_period_forecast,
    ):
        """Test evaluation of forecasted on track scenario."""
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_on_track,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        assert len(stories) == 1
        story = stories[0]

        assert story["story_type"] == StoryType.FORECASTED_ON_TRACK
        assert story["story_group"] == StoryGroup.LIKELY_STATUS
        assert story["genre"] == StoryGenre.PERFORMANCE
        assert story["metric_id"] == "test_inquiries"
        assert "Forecasted to beat end of month target" in story["title"]
        assert "Test Inquiries is forecasted to end the month" in story["detail"]
        assert "1200.00" in story["detail"]
        assert "1000.00" in story["detail"]
        assert "20.0%" in story["detail"]

    @pytest.mark.asyncio
    async def test_evaluate_forecasted_off_track(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_off_track,
        mock_period_forecast,
    ):
        """Test evaluation of forecasted off track scenario."""
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_off_track,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        assert len(stories) == 1
        story = stories[0]

        assert story["story_type"] == StoryType.FORECASTED_OFF_TRACK
        assert story["story_group"] == StoryGroup.LIKELY_STATUS
        assert story["genre"] == StoryGenre.PERFORMANCE
        assert story["metric_id"] == "test_inquiries"
        assert "Forecasted to miss end of month target by 20.0%" in story["title"]
        assert "Test Inquiries is forecasted to end the month" in story["detail"]
        assert "800" in story["detail"]
        assert "1000.00" in story["detail"]

    @pytest.mark.asyncio
    async def test_evaluate_pacing_on_track(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_on_track,
        mock_pacing_on_track,
        mock_period_forecast,
    ):
        """Test evaluation of pacing on track scenario."""
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_on_track,
            pacing=mock_pacing_on_track,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        assert len(stories) == 2  # Both forecasted and pacing stories

        pacing_stories = [s for s in stories if s["story_type"] == StoryType.PACING_ON_TRACK]
        assert len(pacing_stories) == 1

        story = pacing_stories[0]
        assert story["story_group"] == StoryGroup.LIKELY_STATUS
        assert story["genre"] == StoryGenre.PERFORMANCE
        assert story["metric_id"] == "test_inquiries"
        assert "Pacing to beat end of month target" in story["title"]
        assert "75.0% through the month" in story["detail"]
        assert "Test Inquiries is pacing to end this month" in story["detail"]

    @pytest.mark.asyncio
    async def test_evaluate_pacing_off_track(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_off_track,
        mock_pacing_off_track,
        mock_period_forecast,
    ):
        """Test evaluation of pacing off track scenario."""
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_off_track,
            pacing=mock_pacing_off_track,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        assert len(stories) == 2  # Both forecasted and pacing stories

        pacing_stories = [s for s in stories if s["story_type"] == StoryType.PACING_OFF_TRACK]
        assert len(pacing_stories) == 1

        story = pacing_stories[0]
        assert story["story_group"] == StoryGroup.LIKELY_STATUS
        assert story["genre"] == StoryGenre.PERFORMANCE
        assert story["metric_id"] == "test_inquiries"
        assert "Pacing to miss end of month target by 20.0%" in story["title"]
        assert "75.0% through the month" in story["detail"]

    @pytest.mark.asyncio
    async def test_evaluate_required_performance(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_on_track,
        mock_required_performance,
        mock_period_forecast,
    ):
        """Test evaluation of required performance scenario."""
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_on_track,
            required_performance=mock_required_performance,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        assert len(stories) == 2  # Both forecasted and required performance stories

        required_stories = [s for s in stories if s["story_type"] == StoryType.REQUIRED_PERFORMANCE]
        assert len(required_stories) == 1

        story = required_stories[0]
        assert story["story_group"] == StoryGroup.LIKELY_STATUS
        assert story["genre"] == StoryGenre.PERFORMANCE
        assert story["metric_id"] == "test_inquiries"
        assert "Must grow 15.0% w/w to meet end of month target" in story["title"]
        assert "Test Inquiries must average a 15.0% w/w growth rate" in story["detail"]
        assert "next 2 weeks" in story["detail"]

    @pytest.mark.asyncio
    async def test_evaluate_all_scenarios(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_on_track,
        mock_pacing_on_track,
        mock_required_performance,
        mock_period_forecast,
    ):
        """Test evaluation with all forecasting scenarios present."""
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_on_track,
            pacing=mock_pacing_on_track,
            required_performance=mock_required_performance,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        assert len(stories) == 3  # Forecasted, pacing, and required performance stories

        story_types = {story["story_type"] for story in stories}
        expected_types = {StoryType.FORECASTED_ON_TRACK, StoryType.PACING_ON_TRACK, StoryType.REQUIRED_PERFORMANCE}
        assert story_types == expected_types

    @pytest.mark.asyncio
    async def test_evaluate_no_stories_without_forecast_status(
        self, forecasting_evaluator, mock_metric, mock_analysis_window, mock_period_forecast
    ):
        """Test that no stories are generated without forecast status."""
        forecast_stats_no_status = ForecastVsTargetStats(
            forecasted_value=1200.0,
            lower_bound=1100.0,
            upper_bound=1300.0,
            confidence_level=0.95,
            target_date="2024-01-31",
            target_value=1000.0,
            forecasted_gap_percent=20.0,
            forecast_status=None,  # No forecast status
        )

        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_vs_target_stats=forecast_stats_no_status,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        assert len(stories) == 0

    @pytest.mark.asyncio
    async def test_evaluate_no_stories_with_zero_remaining_periods(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_on_track,
        mock_period_forecast,
    ):
        """Test that no required performance story is generated with zero remaining periods."""
        required_perf_zero_periods = RequiredPerformance(
            remaining_periods_count=0,  # No remaining periods
            required_pop_growth_percent=15.0,
            past_pop_growth_percent=10.0,
            delta_from_historical_growth=5.0,
        )

        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_on_track,
            required_performance=required_perf_zero_periods,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        # Only forecasted story, no required performance story
        assert len(stories) == 1
        assert stories[0]["story_type"] == StoryType.FORECASTED_ON_TRACK

    @pytest.mark.asyncio
    async def test_populate_template_context(self, forecasting_evaluator, mock_metric, mock_analysis_window):
        """Test the template context population."""
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_QUARTER,
        )

        context = forecasting_evaluator._populate_template_context(pattern_result, mock_metric, Granularity.WEEK)

        # Check base context
        assert context["metric"]["label"] == "Test Inquiries"
        assert context["metric"]["metric_id"] == "test_inquiries"
        assert context["grain_label"] == "week"
        assert context["pop"] == "w/w"

        # Check period type mapping
        assert context["period_type"] == "quarter"

    def test_period_type_labels(self, forecasting_evaluator, mock_metric):
        """Test period type label mapping."""
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_WEEK,
        )

        context = forecasting_evaluator._populate_template_context(pattern_result, mock_metric, Granularity.DAY)
        assert context["period_type"] == "week"

        # Test other period types
        test_cases = [
            (PeriodType.END_OF_MONTH, "month"),
            (PeriodType.END_OF_QUARTER, "quarter"),
            (PeriodType.END_OF_YEAR, "year"),
        ]

        for period_type, expected_label in test_cases:
            pattern_result = Forecasting(
                metric_id="test_inquiries",
                analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
                analysis_date=date(2024, 1, 25),
                period_type=period_type,
            )
            context = forecasting_evaluator._populate_template_context(pattern_result, mock_metric, Granularity.DAY)
            assert context["period_type"] == expected_label

    @pytest.mark.asyncio
    async def test_evaluate_no_pacing_story_without_pacing_status(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_on_track,
        mock_period_forecast,
    ):
        """Test that no pacing story is generated without pacing status."""
        pacing_no_status = PacingProjection(
            percent_of_period_elapsed=75.0,
            cumulative_value=900.0,
            projected_value=1200.0,
            gap_percent=20.0,
            status=None,  # No status
        )

        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_on_track,
            pacing=pacing_no_status,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        # Only forecasted story, no pacing story
        assert len(stories) == 1
        assert stories[0]["story_type"] == StoryType.FORECASTED_ON_TRACK

    @pytest.mark.asyncio
    async def test_evaluate_no_pacing_story_without_projected_value(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_on_track,
        mock_period_forecast,
    ):
        """Test that no pacing story is generated without projected value."""
        pacing_no_value = PacingProjection(
            percent_of_period_elapsed=75.0,
            cumulative_value=900.0,
            projected_value=None,  # No projected value
            gap_percent=20.0,
            status="on_track",
        )

        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_on_track,
            pacing=pacing_no_value,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        # Only forecasted story, no pacing story
        assert len(stories) == 1
        assert stories[0]["story_type"] == StoryType.FORECASTED_ON_TRACK
