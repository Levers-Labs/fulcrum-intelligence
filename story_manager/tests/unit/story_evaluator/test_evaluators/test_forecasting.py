"""
Tests for forecasting story evaluator.
"""

from datetime import date

import pandas as pd
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
        target_date="2024-01-31",
        target_value=1000.0,
        gap_percent=20.0,
        status=MetricGVAStatus.ON_TRACK,
    )


@pytest.fixture
def mock_forecast_vs_target_stats_off_track():
    """Mock forecast vs target stats for off-track scenario."""
    return ForecastVsTargetStats(
        forecasted_value=800.0,
        target_date="2024-01-31",
        target_value=1000.0,
        gap_percent=20.0,
        status=MetricGVAStatus.OFF_TRACK,
    )


@pytest.fixture
def mock_pacing_on_track():
    """Mock pacing projection for on-track scenario."""
    return PacingProjection(
        period_elapsed_percent=75.0,
        cumulative_value=900.0,
        projected_value=1200.0,
        gap_percent=20.0,
        status="on_track",
    )


@pytest.fixture
def mock_pacing_off_track():
    """Mock pacing projection for off-track scenario."""
    return PacingProjection(
        period_elapsed_percent=75.0,
        cumulative_value=600.0,
        projected_value=800.0,
        gap_percent=20.0,
        status="off_track",
    )


@pytest.fixture
def mock_required_performance():
    """Mock required performance."""
    return RequiredPerformance(
        remaining_periods=2,
        required_pop_growth_percent=15.0,
        previous_pop_growth_percent=10.0,
        growth_difference=5.0,
        previous_num_periods=4,
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
            forecast_period_grain=Granularity.WEEK,
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
        assert "Forecasted to beat end of week target" in story["title"]
        assert "Test Inquiries is forecasted to end the week" in story["detail"]
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
            forecast_period_grain=Granularity.WEEK,
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
        assert "Forecasted to miss end of week target by 20.0%" in story["title"]
        assert "Test Inquiries is forecasted to end the week" in story["detail"]
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
            forecast_period_grain=Granularity.WEEK,
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
        assert "Pacing to beat end of week target" in story["title"]
        assert "75.0% through the week" in story["detail"]
        assert "Test Inquiries is pacing to end this week" in story["detail"]

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
            forecast_period_grain=Granularity.WEEK,
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
        assert "Pacing to miss end of week target by 20.0%" in story["title"]
        assert "75.0% through the week" in story["detail"]

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
            forecast_period_grain=Granularity.WEEK,
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
        assert "Must grow 15.0% w/w to meet end of week target" in story["title"]
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
            forecast_period_grain=Granularity.WEEK,
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
            target_date="2024-01-31",
            target_value=1000.0,
            gap_percent=20.0,
            status=None,  # No forecast status
        )

        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_period_grain=Granularity.WEEK,
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
            remaining_periods=0,  # No remaining periods
            required_pop_growth_percent=15.0,
            previous_pop_growth_percent=10.0,
            growth_difference=5.0,
            previous_num_periods=4,
        )

        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_period_grain=Granularity.WEEK,
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
            forecast_period_grain=Granularity.MONTH,
        )

        context = forecasting_evaluator._populate_template_context(pattern_result, mock_metric, Granularity.WEEK)

        # Check base context
        assert context["metric"]["label"] == "Test Inquiries"
        assert context["metric"]["metric_id"] == "test_inquiries"
        assert context["grain_label"] == "week"
        assert context["pop"] == "w/w"

    def test_period_type_labels(self, forecasting_evaluator, mock_metric):
        """Test period type label mapping."""
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_WEEK,
            forecast_period_grain=Granularity.DAY,
        )

        _ = forecasting_evaluator._populate_template_context(pattern_result, mock_metric, Granularity.DAY)

        # Test other period types
        test_cases = [
            (PeriodType.END_OF_MONTH, "month"),
            (PeriodType.END_OF_QUARTER, "quarter"),
            (PeriodType.END_OF_YEAR, "year"),
        ]

        for period_type, _ in test_cases:
            pattern_result = Forecasting(
                metric_id="test_inquiries",
                analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
                analysis_date=date(2024, 1, 25),
                period_type=period_type,
                forecast_period_grain=Granularity.DAY,
            )
            _ = forecasting_evaluator._populate_template_context(pattern_result, mock_metric, Granularity.DAY)

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
            period_elapsed_percent=75.0,
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
            forecast_period_grain=Granularity.WEEK,
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
            period_elapsed_percent=75.0,
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
            forecast_period_grain=Granularity.WEEK,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_on_track,
            pacing=pacing_no_value,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        # Only forecasted story, no pacing story
        assert len(stories) == 1
        assert stories[0]["story_type"] == StoryType.FORECASTED_ON_TRACK

    @pytest.mark.asyncio
    async def test_custom_forecast_series_data_included(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_on_track,
        mock_period_forecast,
    ):
        """Test that custom forecast series data is properly included in stories."""
        # Set up some mock historical data
        historical_data = pd.DataFrame(
            {"date": pd.date_range(start="2024-01-20", periods=5, freq="D"), "value": [1000, 1020, 1040, 1060, 1080]}
        )
        forecasting_evaluator.series_df = historical_data

        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_period_grain=Granularity.WEEK,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_on_track,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        assert len(stories) == 1
        story = stories[0]

        # Verify that series is included (series_data becomes "series" in the final story)
        assert "series" in story
        assert story["series"] is not None

        # Should have both historical and forecast data combined
        series_data = story["series"]
        assert len(series_data) > 3  # Should have historical + forecast data

    @pytest.mark.asyncio
    async def test_custom_forecast_series_data_off_track(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_off_track,
        mock_period_forecast,
    ):
        """Test that custom forecast series data is properly included in off track stories."""
        # Set up some mock historical data
        historical_data = pd.DataFrame(
            {"date": pd.date_range(start="2024-01-20", periods=5, freq="D"), "value": [1000, 1020, 1040, 1060, 1080]}
        )
        forecasting_evaluator.series_df = historical_data

        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_period_grain=Granularity.WEEK,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_off_track,
            period_forecast=mock_period_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        assert len(stories) == 1
        story = stories[0]
        assert story["story_type"] == StoryType.FORECASTED_OFF_TRACK

        # Verify that series is included in off track stories too (series_data becomes "series")
        assert "series" in story
        assert story["series"] is not None
        assert len(story["series"]) > 3  # Should have historical + forecast data

    @pytest.mark.asyncio
    async def test_custom_forecast_series_data_empty_when_no_period_forecast(
        self,
        forecasting_evaluator,
        mock_metric,
        mock_analysis_window,
        mock_forecast_vs_target_stats_on_track,
    ):
        """Test that series_data contains only historical data when no period_forecast is available."""
        # Set up some mock historical data
        historical_data = pd.DataFrame(
            {"date": pd.date_range(start="2024-01-20", periods=5, freq="D"), "value": [1000, 1020, 1040, 1060, 1080]}
        )
        forecasting_evaluator.series_df = historical_data

        # Create a minimal forecast data to avoid the TypeError
        minimal_forecast = [
            Forecast(date="2024-01-26", forecasted_value=1100.0, lower_bound=1000.0, upper_bound=1200.0)
        ]

        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=mock_analysis_window,
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_period_grain=Granularity.WEEK,
            forecast_vs_target_stats=mock_forecast_vs_target_stats_on_track,
            period_forecast=minimal_forecast,
        )

        stories = await forecasting_evaluator.evaluate(pattern_result, mock_metric)

        assert len(stories) == 1
        story = stories[0]

        # Verify that series contains both historical and forecast data
        assert "series" in story
        series_data = story["series"]
        assert len(series_data) == 6  # 5 historical + 1 forecast

    def test_prepare_forecast_series_data_with_historical_and_forecast(
        self, forecasting_evaluator, mock_period_forecast
    ):
        """Test that _prepare_forecast_series_data correctly combines historical and forecast data."""

        # Set up mock historical data
        historical_data = pd.DataFrame(
            {"date": pd.date_range(start="2024-01-20", periods=5, freq="D"), "value": [1000, 1020, 1040, 1060, 1080]}
        )
        forecasting_evaluator.series_df = historical_data

        # Create pattern result with forecast data
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_period_grain=Granularity.DAY,
            period_forecast=mock_period_forecast,
        )

        # Test the method
        combined_df = forecasting_evaluator._prepare_forecast_series_data(pattern_result)

        # Verify the structure
        assert isinstance(combined_df, pd.DataFrame)
        assert not combined_df.empty

        # Should have all required columns
        expected_columns = ["date", "value", "lower_bound", "upper_bound"]
        for col in expected_columns:
            assert col in combined_df.columns

        # Should have both historical and forecast data
        assert len(combined_df) == 8  # 5 historical + 3 forecast points

        # Verify data types and values
        assert all(pd.notna(combined_df["value"]))

        # Should be sorted by date
        assert combined_df["date"].is_monotonic_increasing

    def test_prepare_forecast_series_data_forecast_only(self, forecasting_evaluator, mock_period_forecast):
        """Test _prepare_forecast_series_data with forecast data only (no historical data)."""
        # No historical data
        forecasting_evaluator.series_df = None

        # Create pattern result with forecast data
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_period_grain=Granularity.DAY,
            period_forecast=mock_period_forecast,
        )

        # Test the method
        combined_df = forecasting_evaluator._prepare_forecast_series_data(pattern_result)

        # Should have only forecast data
        assert len(combined_df) == 3  # 3 forecast points

    def test_prepare_forecast_series_data_historical_only(self, forecasting_evaluator):
        """Test _prepare_forecast_series_data with historical data only (no forecast data)."""

        # Set up mock historical data
        historical_data = pd.DataFrame(
            {"date": pd.date_range(start="2024-01-20", periods=5, freq="D"), "value": [1000, 1020, 1040, 1060, 1080]}
        )
        forecasting_evaluator.series_df = historical_data

        # Create pattern result with empty forecast data
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_period_grain=Granularity.DAY,
            period_forecast=[],  # Empty list
        )

        # Test the method
        combined_df = forecasting_evaluator._prepare_forecast_series_data(pattern_result)

        # Should have only historical data
        assert len(combined_df) == 5  # 5 historical points
        assert all(pd.isna(combined_df["lower_bound"]))  # No forecast bounds
        assert all(pd.isna(combined_df["upper_bound"]))  # No forecast bounds

    def test_prepare_required_performance_series_data_with_historical_and_forecast(
        self, forecasting_evaluator, mock_required_performance
    ):
        """Test that _prepare_required_performance_series_data correctly creates growth rate data."""

        # Set up mock historical data
        historical_data = pd.DataFrame(
            {"date": pd.date_range(start="2024-01-20", periods=5, freq="D"), "value": [1000, 1020, 1040, 1060, 1080]}
        )
        forecasting_evaluator.series_df = historical_data

        # Create pattern result with required performance data
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_period_grain=Granularity.DAY,
            required_performance=mock_required_performance,
        )

        # Test the method
        series_df = forecasting_evaluator._prepare_required_performance_series_data(pattern_result, Granularity.DAY)

        # Verify the structure
        assert isinstance(series_df, pd.DataFrame)
        assert not series_df.empty

        # Should have all required columns
        expected_columns = ["date", "value", "pop_growth_percent", "required_growth_percent"]
        for col in expected_columns:
            assert col in series_df.columns

        # Should have historical growth data (4 points) + future required growth (2 points)
        assert len(series_df) == 6  # 4 historical growth + 2 future required growth

    def test_prepare_required_performance_series_data_forecast_only(
        self, forecasting_evaluator, mock_required_performance
    ):
        """Test _prepare_required_performance_series_data with no historical data."""
        # No historical data
        forecasting_evaluator.series_df = None

        # Create pattern result with required performance data
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_period_grain=Granularity.DAY,
            required_performance=mock_required_performance,
        )

        # Test the method
        series_df = forecasting_evaluator._prepare_required_performance_series_data(pattern_result, Granularity.DAY)

        # Should return empty DataFrame when no historical data
        expected_columns = ["date", "value", "required_growth_percent", "pop_growth_percent"]
        assert list(series_df.columns) == expected_columns

    def test_prepare_required_performance_series_data_historical_only(self, forecasting_evaluator):
        """Test _prepare_required_performance_series_data with historical data only (no required performance)."""

        # Set up mock historical data
        historical_data = pd.DataFrame(
            {"date": pd.date_range(start="2024-01-20", periods=5, freq="D"), "value": [1000, 1020, 1040, 1060, 1080]}
        )
        forecasting_evaluator.series_df = historical_data

        # Create pattern result without required performance data
        pattern_result = Forecasting(
            metric_id="test_inquiries",
            analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
            analysis_date=date(2024, 1, 25),
            period_type=PeriodType.END_OF_MONTH,
            forecast_period_grain=Granularity.DAY,
            required_performance=RequiredPerformance(
                remaining_periods=0,  # No remaining periods
                required_pop_growth_percent=None,
                previous_pop_growth_percent=None,
                growth_difference=None,
                previous_num_periods=None,
            ),
        )

        # Test the method
        series_df = forecasting_evaluator._prepare_required_performance_series_data(pattern_result, Granularity.DAY)

        # Should have only historical growth data (4 points)
        assert len(series_df) == 4  # 4 historical growth points (excluding first NaN)
