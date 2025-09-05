"""
Tests for the forecasting evaluator.
"""

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from commons.models.enums import Granularity
from levers.models import (
    AnalysisWindow,
    Forecast,
    ForecastVsTargetStats,
    ForecastWindow,
    PacingProjection,
    RequiredPerformance,
)
from levers.models.enums import MetricGVAStatus, PeriodType
from levers.models.patterns.forecasting import Forecasting
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator.evaluators.forecasting import ForecastingEvaluator


@pytest.fixture
def mock_analysis_window():
    """Fixture for mock analysis window."""
    return AnalysisWindow(start_date="2024-01-01", end_date="2024-01-31", grain=Granularity.DAY)


@pytest.fixture
def mock_forecast_vs_target_stats():
    """Fixture for mock forecast vs target stats."""
    return [
        ForecastVsTargetStats(
            period=PeriodType.END_OF_MONTH,
            forecasted_value=1200.0,
            target_value=1000.0,
            gap_percent=20.0,
            status=MetricGVAStatus.ON_TRACK,
        ),
        ForecastVsTargetStats(
            period=PeriodType.END_OF_QUARTER,
            forecasted_value=900.0,
            target_value=1000.0,
            gap_percent=-10.0,
            status=MetricGVAStatus.OFF_TRACK,
        ),
    ]


@pytest.fixture
def mock_pacing_projections():
    """Fixture for mock pacing projections."""
    return [
        PacingProjection(
            period=PeriodType.END_OF_MONTH,
            period_elapsed_percent=75.0,
            projected_value=1100.0,
            target_value=1000.0,
            gap_percent=10.0,
            status=MetricGVAStatus.ON_TRACK,
        ),
        PacingProjection(
            period=PeriodType.END_OF_QUARTER,
            period_elapsed_percent=33.3,
            projected_value=900.0,
            target_value=1200.0,
            gap_percent=-25.0,
            status=MetricGVAStatus.OFF_TRACK,
        ),
    ]


@pytest.fixture
def mock_required_performance():
    """Fixture for mock required performance."""
    return [
        RequiredPerformance(
            period=PeriodType.END_OF_MONTH,
            remaining_periods=4,
            required_pop_growth_percent=15.0,
            previous_pop_growth_percent=10.0,
            growth_difference=5.0,
            previous_periods=3,
        ),
        RequiredPerformance(
            period=PeriodType.END_OF_QUARTER,
            remaining_periods=2,
            required_pop_growth_percent=25.0,
            previous_pop_growth_percent=5.0,
            growth_difference=20.0,
            previous_periods=6,
        ),
    ]


@pytest.fixture
def mock_forecast_data():
    """Fixture for mock forecast data."""
    return [
        Forecast(date="2024-02-01", forecasted_value=100.0, lower_bound=90.0, upper_bound=110.0, confidence_level=0.95),
        Forecast(date="2024-02-02", forecasted_value=105.0, lower_bound=95.0, upper_bound=115.0, confidence_level=0.95),
        Forecast(date="2024-02-03", forecasted_value=110.0, lower_bound=98.0, upper_bound=122.0, confidence_level=0.95),
    ]


@pytest.fixture
def mock_forecasting_pattern(
    mock_analysis_window,
    mock_forecast_vs_target_stats,
    mock_pacing_projections,
    mock_required_performance,
    mock_forecast_data,
):
    """Fixture for mock forecasting pattern."""
    return Forecasting(
        pattern="forecasting",
        pattern_run_id=1,
        metric_id="test_metric",
        analysis_window=mock_analysis_window,
        analysis_date=date(2024, 1, 31),
        forecast_window=ForecastWindow(start_date="2024-02-01", end_date="2024-02-28", num_periods=28),
        forecast_vs_target_stats=mock_forecast_vs_target_stats,
        pacing=mock_pacing_projections,
        required_performance=mock_required_performance,
        forecast=mock_forecast_data,
    )


@pytest.fixture
def mock_metric():
    """Fixture for mock metric."""
    return {
        "metric_id": "test_metric",
        "metric_name": "Test Metric",
        "metric_display_name": "Test Display Metric",
        "label": "Test Metric",  # Added required label field
        "higher_is_better": True,
        "metric_type": "revenue",
        "unit": "USD",
    }


@pytest.fixture
def mock_series_df():
    """Fixture for mock series data."""
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", "2024-01-31", freq="D"),
            "value": [100 + i * 5 for i in range(31)],  # Increasing values
        }
    )


@pytest.fixture
def evaluator():
    """Fixture for forecasting evaluator."""
    return ForecastingEvaluator()


@pytest.fixture
def evaluator_with_series(evaluator, mock_series_df):
    """Fixture for evaluator with series data."""
    evaluator.series_df = mock_series_df
    return evaluator


class TestForecastingEvaluator:
    """Test cases for ForecastingEvaluator."""

    def test_pattern_name(self, evaluator):
        """Test pattern name is set correctly."""
        assert evaluator.pattern_name == "forecasting"

    @pytest.mark.asyncio
    async def test_evaluate_all_story_types(self, evaluator_with_series, mock_forecasting_pattern, mock_metric):
        """Test that evaluate method generates all expected story types."""
        stories = await evaluator_with_series.evaluate(mock_forecasting_pattern, mock_metric)

        # Should generate 6 stories: 2 forecast stories + 2 pacing stories + 2 required performance stories
        assert len(stories) == 6

        story_types = {story["story_type"] for story in stories}
        expected_story_types = {
            StoryType.FORECASTED_ON_TRACK,
            StoryType.FORECASTED_OFF_TRACK,
            StoryType.PACING_ON_TRACK,
            StoryType.PACING_OFF_TRACK,
            StoryType.REQUIRED_PERFORMANCE,
        }
        assert expected_story_types.issubset(story_types)

        # Verify all stories have required fields
        for story in stories:
            assert "title" in story
            assert "detail" in story
            assert story["genre"] == StoryGenre.PERFORMANCE
            assert (
                story["story_group"] == StoryGroup.LIKELY_STATUS
                if story["story_type"] != StoryType.REQUIRED_PERFORMANCE
                else StoryGroup.REQUIRED_PERFORMANCE
            )
            assert story["metric_id"] == "test_metric"
            assert story["grain"] == Granularity.DAY

    @pytest.mark.asyncio
    async def test_evaluate_empty_pattern_data(self, evaluator, mock_metric):
        """Test evaluate with empty pattern data."""
        empty_pattern = Forecasting(
            pattern="forecasting",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2024-01-01", end_date="2024-01-31", grain=Granularity.DAY),
            analysis_date=date(2024, 1, 31),
            forecast_vs_target_stats=[],
            pacing=[],
            required_performance=[],
            forecast=[],
        )

        stories = await evaluator.evaluate(empty_pattern, mock_metric)
        assert len(stories) == 0

    @pytest.mark.asyncio
    async def test_evaluate_none_values_in_data(self, evaluator_with_series, mock_metric):
        """Test evaluate with None values in data."""
        pattern_with_nones = Forecasting(
            pattern="forecasting",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2024-01-01", end_date="2024-01-31", grain=Granularity.DAY),
            analysis_date=date(2024, 1, 31),
            forecast_vs_target_stats=[None, ForecastVsTargetStats(status=None)],
            pacing=[None, PacingProjection(status=None, projected_value=None)],
            required_performance=[None, RequiredPerformance(required_pop_growth_percent=None)],
            forecast=[],
        )

        stories = await evaluator_with_series.evaluate(pattern_with_nones, mock_metric)
        assert len(stories) == 0

    def test_populate_template_context_forecast_stats(self, evaluator, mock_forecasting_pattern, mock_metric):
        """Test _populate_template_context with forecast stats."""
        forecast_stats = mock_forecasting_pattern.forecast_vs_target_stats[0]

        context = evaluator._populate_template_context(
            mock_forecasting_pattern, mock_metric, Granularity.DAY, forecast_stats=forecast_stats
        )

        assert context["period"] == "month"
        assert context["forecasted_value"] == 1200.0
        assert context["target_value"] == 1000.0
        assert context["gap_percent"] == 20.0

    def test_populate_template_context_pacing(self, evaluator, mock_forecasting_pattern, mock_metric):
        """Test _populate_template_context with pacing data."""
        pacing = mock_forecasting_pattern.pacing[0]

        context = evaluator._populate_template_context(
            mock_forecasting_pattern, mock_metric, Granularity.DAY, pacing=pacing
        )

        assert context["period"] == "month"
        assert context["percent_elapsed"] == 75.0
        assert context["projected_value"] == 1100.0
        assert context["target_value"] == 1000.0
        assert context["gap_percent"] == 10.0
        assert "period_end_date" in context

    def test_populate_template_context_required_performance(self, evaluator, mock_forecasting_pattern, mock_metric):
        """Test _populate_template_context with required performance data."""
        required_perf = mock_forecasting_pattern.required_performance[0]

        context = evaluator._populate_template_context(
            mock_forecasting_pattern, mock_metric, Granularity.DAY, required_perf=required_perf
        )

        assert context["period"] == "month"
        assert context["required_growth"] == 15.0
        assert context["remaining_periods"] == 4
        assert context["growth_difference"] == 5.0
        assert context["trend_direction"] == "increase"
        assert context["previous_growth"] == 10.0
        assert context["previous_periods"] == 3

    def test_populate_template_context_negative_growth_difference(
        self, evaluator, mock_forecasting_pattern, mock_metric
    ):
        """Test _populate_template_context with negative growth difference."""
        required_perf = RequiredPerformance(
            period=PeriodType.END_OF_MONTH,
            remaining_periods=4,
            required_pop_growth_percent=5.0,
            previous_pop_growth_percent=10.0,
            growth_difference=-5.0,  # Negative difference
            previous_periods=3,
        )

        context = evaluator._populate_template_context(
            mock_forecasting_pattern, mock_metric, Granularity.DAY, required_perf=required_perf
        )

        assert context["trend_direction"] == "decrease"
        assert context["growth_difference"] == 5.0  # Should be absolute value

    def test_populate_template_context_no_period(self, evaluator, mock_forecasting_pattern, mock_metric):
        """Test _populate_template_context with no period specified."""
        forecast_stats = ForecastVsTargetStats(
            period=None,  # No period
            forecasted_value=1200.0,
            target_value=1000.0,
            gap_percent=20.0,
            status=MetricGVAStatus.ON_TRACK,
        )

        context = evaluator._populate_template_context(
            mock_forecasting_pattern, mock_metric, Granularity.DAY, forecast_stats=forecast_stats
        )

        assert context["period"] is None

    @patch("story_manager.story_evaluator.evaluators.forecasting.render_story_text")
    def test_create_forecasted_on_track_story(
        self, mock_render, evaluator_with_series, mock_forecasting_pattern, mock_metric
    ):
        """Test _create_forecasted_on_track_story method."""
        mock_render.return_value = "Mock rendered text"
        forecast_stats = mock_forecasting_pattern.forecast_vs_target_stats[0]

        story = evaluator_with_series._create_forecasted_on_track_story(
            mock_forecasting_pattern, "test_metric", mock_metric, Granularity.DAY, forecast_stats
        )

        assert story["story_type"] == StoryType.FORECASTED_ON_TRACK
        assert (
            story["story_group"] == StoryGroup.LIKELY_STATUS
            if story["story_type"] != StoryType.REQUIRED_PERFORMANCE
            else StoryGroup.REQUIRED_PERFORMANCE
        )
        assert story["genre"] == StoryGenre.PERFORMANCE
        assert story["metric_id"] == "test_metric"
        assert "title" in story
        assert "detail" in story
        assert "title" in story
        assert "detail" in story

    @patch("story_manager.story_evaluator.evaluators.forecasting.render_story_text")
    def test_create_forecasted_off_track_story(
        self, mock_render, evaluator_with_series, mock_forecasting_pattern, mock_metric
    ):
        """Test _create_forecasted_off_track_story method."""
        mock_render.return_value = "Mock rendered text"
        forecast_stats = mock_forecasting_pattern.forecast_vs_target_stats[1]

        story = evaluator_with_series._create_forecasted_off_track_story(
            mock_forecasting_pattern, "test_metric", mock_metric, Granularity.DAY, forecast_stats
        )

        assert story["story_type"] == StoryType.FORECASTED_OFF_TRACK
        assert story["story_group"] == StoryGroup.LIKELY_STATUS
        assert story["genre"] == StoryGenre.PERFORMANCE
        assert story["metric_id"] == "test_metric"

    @patch("story_manager.story_evaluator.evaluators.forecasting.render_story_text")
    def test_create_pacing_on_track_story(
        self, mock_render, evaluator_with_series, mock_forecasting_pattern, mock_metric
    ):
        """Test _create_pacing_on_track_story method."""
        mock_render.return_value = "Mock rendered text"
        pacing = mock_forecasting_pattern.pacing[0]

        story = evaluator_with_series._create_pacing_on_track_story(
            mock_forecasting_pattern, "test_metric", mock_metric, Granularity.DAY, pacing
        )

        assert story["story_type"] == StoryType.PACING_ON_TRACK
        assert story["story_group"] == StoryGroup.LIKELY_STATUS
        assert story["genre"] == StoryGenre.PERFORMANCE

    @patch("story_manager.story_evaluator.evaluators.forecasting.render_story_text")
    def test_create_pacing_off_track_story(
        self, mock_render, evaluator_with_series, mock_forecasting_pattern, mock_metric
    ):
        """Test _create_pacing_off_track_story method."""
        mock_render.return_value = "Mock rendered text"
        pacing = mock_forecasting_pattern.pacing[1]

        story = evaluator_with_series._create_pacing_off_track_story(
            mock_forecasting_pattern, "test_metric", mock_metric, Granularity.DAY, pacing
        )

        assert story["story_type"] == StoryType.PACING_OFF_TRACK
        assert story["story_group"] == StoryGroup.LIKELY_STATUS
        assert story["genre"] == StoryGenre.PERFORMANCE

    @patch("story_manager.story_evaluator.evaluators.forecasting.render_story_text")
    def test_create_required_performance_story(
        self, mock_render, evaluator_with_series, mock_forecasting_pattern, mock_metric
    ):
        """Test _create_required_performance_story method."""
        mock_render.return_value = "Mock rendered text"
        required_perf = mock_forecasting_pattern.required_performance[0]

        story = evaluator_with_series._create_required_performance_story(
            mock_forecasting_pattern, "test_metric", mock_metric, Granularity.DAY, required_perf
        )

        assert story["story_type"] == StoryType.REQUIRED_PERFORMANCE
        assert story["story_group"] == StoryGroup.LIKELY_STATUS
        assert story["genre"] == StoryGenre.PERFORMANCE

    def test_calculate_cumulative_series_basic(self, evaluator):
        """Test _calculate_cumulative_series method."""
        df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=5), "value": [10, 20, 15, 25, 30]})

        result = evaluator._calculate_cumulative_series(df, include_bounds=False)

        assert "cumulative_value" in result.columns
        expected_cumulative = [10, 30, 45, 70, 100]
        assert result["cumulative_value"].tolist() == expected_cumulative

    def test_calculate_cumulative_series_with_bounds(self, evaluator):
        """Test _calculate_cumulative_series with bounds."""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=3),
                "value": [10, 20, 15],
                "lower_bound": [8, 18, 13],
                "upper_bound": [12, 22, 17],
            }
        )

        result = evaluator._calculate_cumulative_series(df, include_bounds=True)

        assert "cumulative_value" in result.columns
        assert "cumulative_lower_bound" in result.columns
        assert "cumulative_upper_bound" in result.columns

        # Check cumulative values
        assert result["cumulative_value"].tolist() == [10, 30, 45]

    def test_calculate_cumulative_series_with_last_value(self, evaluator):
        """Test _calculate_cumulative_series with last cumulative value."""
        df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=3), "value": [10, 20, 15]})

        result = evaluator._calculate_cumulative_series(df, include_bounds=False, last_cumulative_value=100.0)

        expected_cumulative = [110, 130, 145]  # Adding 100 to each cumulative value
        assert result["cumulative_value"].tolist() == expected_cumulative

    def test_calculate_cumulative_series_empty_df(self, evaluator):
        """Test _calculate_cumulative_series with empty DataFrame."""
        df = pd.DataFrame(columns=["date", "value"])

        result = evaluator._calculate_cumulative_series(df)

        assert result.empty
        assert list(result.columns) == ["date", "value"]

    def test_calculate_cumulative_series_with_bounds_partial_none(self, evaluator):
        """Test _calculate_cumulative_series with some None bounds."""
        df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=4),
                "value": [10, 20, 15, 25],
                "lower_bound": [8, None, 13, 23],  # One None value
                "upper_bound": [12, 22, None, 27],  # One None value
            }
        )

        result = evaluator._calculate_cumulative_series(df, include_bounds=True)

        assert "cumulative_value" in result.columns
        assert "cumulative_lower_bound" in result.columns
        assert "cumulative_upper_bound" in result.columns

    def test_convert_daily_forecast_to_grain_day(self, evaluator):
        """Test _convert_daily_forecast_to_grain with day grain."""
        daily_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "value": [10, 20, 15, 25, 30],
                "lower_bound": [8, 18, 13, 23, 28],
                "upper_bound": [12, 22, 17, 27, 32],
            }
        )

        result = evaluator._convert_daily_forecast_to_grain(daily_df, Granularity.DAY)

        # Should return the same DataFrame for day grain
        pd.testing.assert_frame_equal(result, daily_df)

    def test_convert_daily_forecast_to_grain_week(self, evaluator):
        """Test _convert_daily_forecast_to_grain with week grain."""
        # Create 14 days of data (2 weeks)
        daily_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=14),  # Monday start
                "value": [10] * 14,
                "lower_bound": [8] * 14,
                "upper_bound": [12] * 14,
            }
        )

        result = evaluator._convert_daily_forecast_to_grain(daily_df, Granularity.WEEK)

        # Should aggregate by week (2 weeks = 2 rows)
        assert len(result) == 2
        assert "date" in result.columns
        assert result["value"].iloc[0] == 70  # 7 days * 10
        assert result["value"].iloc[1] == 70  # 7 days * 10

    def test_convert_daily_forecast_to_grain_month(self, evaluator):
        """Test _convert_daily_forecast_to_grain with month grain."""
        # Create data for January 2024 (31 days)
        daily_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=31),
                "value": [10] * 31,
                "lower_bound": [8] * 31,
                "upper_bound": [12] * 31,
            }
        )

        result = evaluator._convert_daily_forecast_to_grain(daily_df, Granularity.MONTH)

        # Should aggregate by month (1 month = 1 row)
        assert len(result) == 1
        assert result["value"].iloc[0] == 310  # 31 days * 10

    def test_convert_daily_forecast_to_grain_other(self, evaluator):
        """Test _convert_daily_forecast_to_grain with unsupported grain."""
        daily_df = pd.DataFrame(
            {
                "date": pd.date_range("2024-01-01", periods=5),
                "value": [10, 20, 15, 25, 30],
            }
        )

        result = evaluator._convert_daily_forecast_to_grain(daily_df, Granularity.QUARTER)

        # Should return original DataFrame for unsupported grains
        pd.testing.assert_frame_equal(result, daily_df)

    @patch("levers.primitives.get_period_range_for_grain")
    def test_prepare_forecast_series_data(self, mock_get_range, evaluator_with_series, mock_forecasting_pattern):
        """Test _prepare_forecast_series_data method."""
        mock_get_range.return_value = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31"))
        forecast_stats = mock_forecasting_pattern.forecast_vs_target_stats[0]

        series_data = evaluator_with_series._prepare_forecast_series_data(
            mock_forecasting_pattern, Granularity.DAY, forecast_stats
        )

        assert isinstance(series_data, list)
        assert len(series_data) == 1

        data_dict = series_data[0]
        assert "data" in data_dict
        assert "forecast" in data_dict
        assert isinstance(data_dict["data"], list)
        assert isinstance(data_dict["forecast"], list)

    @patch("levers.primitives.get_period_range_for_grain")
    def test_prepare_forecast_series_data_empty_series(self, mock_get_range, evaluator, mock_forecasting_pattern):
        """Test _prepare_forecast_series_data with no series data."""
        mock_get_range.return_value = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31"))
        forecast_stats = mock_forecasting_pattern.forecast_vs_target_stats[0]

        series_data = evaluator._prepare_forecast_series_data(mock_forecasting_pattern, Granularity.DAY, forecast_stats)

        assert isinstance(series_data, list)
        assert len(series_data) == 1
        data_dict = series_data[0]
        assert "data" in data_dict
        assert "forecast" in data_dict

    @patch("levers.primitives.get_period_range_for_grain")
    def test_prepare_required_performance_series_data(
        self, mock_get_range, evaluator_with_series, mock_forecasting_pattern
    ):
        """Test _prepare_required_performance_series_data method."""
        mock_get_range.return_value = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31"))
        required_perf = mock_forecasting_pattern.required_performance[0]

        series_data = evaluator_with_series._prepare_required_performance_series_data(
            mock_forecasting_pattern, required_perf
        )

        assert isinstance(series_data, list)
        assert len(series_data) == 1

        data_dict = series_data[0]
        assert "data" in data_dict

    def test_prepare_required_performance_series_data_no_series(self, evaluator, mock_forecasting_pattern):
        """Test _prepare_required_performance_series_data with no series data."""
        required_perf = mock_forecasting_pattern.required_performance[0]

        series_data = evaluator._prepare_required_performance_series_data(mock_forecasting_pattern, required_perf)

        assert series_data == []

    @patch("levers.primitives.get_period_range_for_grain")
    def test_prepare_pacing_series_data(self, mock_get_range, evaluator_with_series, mock_forecasting_pattern):
        """Test _prepare_pacing_series_data method."""
        mock_get_range.return_value = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31"))
        pacing = mock_forecasting_pattern.pacing[0]

        result = evaluator_with_series._prepare_pacing_series_data(mock_forecasting_pattern, Granularity.DAY, pacing)

        assert isinstance(result, pd.DataFrame)
        assert "cumulative_value" in result.columns
        assert len(result) > 0

    @patch("levers.primitives.get_period_range_for_grain")
    def test_prepare_pacing_series_data_no_series(self, mock_get_range, evaluator, mock_forecasting_pattern):
        """Test _prepare_pacing_series_data with no series data."""
        mock_get_range.return_value = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31"))
        pacing = mock_forecasting_pattern.pacing[0]

        result = evaluator._prepare_pacing_series_data(mock_forecasting_pattern, Granularity.DAY, pacing)

        # Should return empty DataFrame with correct columns
        assert isinstance(result, pd.DataFrame)
        assert "date" in result.columns
        assert "value" in result.columns
        assert len(result) == 0

    def test_get_period_grain(self, evaluator):
        """Test _get_period_grain method."""
        assert evaluator._get_period_grain(PeriodType.END_OF_WEEK) == Granularity.WEEK
        assert evaluator._get_period_grain(PeriodType.END_OF_MONTH) == Granularity.MONTH
        assert evaluator._get_period_grain(PeriodType.END_OF_QUARTER) == Granularity.QUARTER
        assert evaluator._get_period_grain(PeriodType.END_OF_YEAR) == Granularity.YEAR

    @pytest.mark.asyncio
    async def test_evaluate_with_different_grains(self, evaluator_with_series, mock_metric):
        """Test evaluate with different granularities."""
        for grain in [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]:
            pattern = Forecasting(
                pattern="forecasting",
                metric_id="test_metric",
                analysis_window=AnalysisWindow(start_date="2024-01-01", end_date="2024-01-31", grain=grain),
                analysis_date=date(2024, 1, 31),
                forecast_vs_target_stats=[
                    ForecastVsTargetStats(
                        period=PeriodType.END_OF_MONTH,
                        forecasted_value=1000.0,
                        target_value=900.0,
                        gap_percent=11.1,
                        status=MetricGVAStatus.ON_TRACK,
                    )
                ],
                pacing=[],
                required_performance=[],
                forecast=[],
            )

            stories = await evaluator_with_series.evaluate(pattern, mock_metric)
            assert len(stories) == 1
            assert stories[0]["grain"] == grain

    @pytest.mark.asyncio
    async def test_evaluate_skips_invalid_required_performance(self, evaluator_with_series, mock_metric):
        """Test that evaluate skips invalid required performance data."""
        pattern = Forecasting(
            pattern="forecasting",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2024-01-01", end_date="2024-01-31", grain=Granularity.DAY),
            analysis_date=date(2024, 1, 31),
            forecast_vs_target_stats=[],
            pacing=[],
            required_performance=[
                RequiredPerformance(required_pop_growth_percent=None),  # Invalid: None value
                RequiredPerformance(remaining_periods=None),  # Invalid: None value
                RequiredPerformance(remaining_periods=0),  # Invalid: 0 periods
                RequiredPerformance(required_pop_growth_percent=15.0, remaining_periods=4, previous_periods=3),  # Valid
            ],
            forecast=[],
        )

        stories = await evaluator_with_series.evaluate(pattern, mock_metric)
        assert len(stories) == 1  # Only the valid required performance story
        assert stories[0]["story_type"] == StoryType.REQUIRED_PERFORMANCE

    @pytest.mark.asyncio
    async def test_evaluate_with_week_grain_pattern(self, evaluator_with_series, mock_metric):
        """Test evaluate with week grain analysis window."""
        pattern = Forecasting(
            pattern="forecasting",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(
                start_date="2024-01-01", end_date="2024-01-31", grain=Granularity.WEEK  # Week grain
            ),
            analysis_date=date(2024, 1, 31),
            forecast_vs_target_stats=[
                ForecastVsTargetStats(
                    period=PeriodType.END_OF_MONTH,
                    forecasted_value=1000.0,
                    target_value=900.0,
                    gap_percent=11.1,
                    status=MetricGVAStatus.ON_TRACK,
                )
            ],
            pacing=[
                PacingProjection(
                    period=PeriodType.END_OF_MONTH,
                    period_elapsed_percent=75.0,
                    projected_value=1100.0,
                    target_value=1000.0,
                    gap_percent=10.0,
                    status=MetricGVAStatus.ON_TRACK,
                )
            ],
            required_performance=[
                RequiredPerformance(
                    period=PeriodType.END_OF_MONTH,
                    remaining_periods=4,
                    required_pop_growth_percent=15.0,
                    previous_periods=3,
                )
            ],
            forecast=[
                Forecast(
                    date="2024-02-01",
                    forecasted_value=100.0,
                    lower_bound=90.0,
                    upper_bound=110.0,
                )
            ],
        )

        stories = await evaluator_with_series.evaluate(pattern, mock_metric)
        assert len(stories) == 3  # One of each story type
        for story in stories:
            assert story["grain"] == Granularity.WEEK

    def test_prepare_forecast_series_data_json_serialization(self, evaluator_with_series, mock_forecasting_pattern):
        """Test that _prepare_forecast_series_data produces JSON-serializable data."""
        with patch("levers.primitives.get_period_range_for_grain") as mock_get_range:
            mock_get_range.return_value = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31"))
            forecast_stats = mock_forecasting_pattern.forecast_vs_target_stats[0]

            series_data = evaluator_with_series._prepare_forecast_series_data(
                mock_forecasting_pattern, Granularity.DAY, forecast_stats
            )

            # Should be JSON serializable
            import json

            try:
                json.dumps(series_data)
            except (TypeError, ValueError):
                pytest.fail("Series data should be JSON serializable")

    @patch("story_manager.story_evaluator.evaluators.forecasting.render_story_text")
    @patch.object(ForecastingEvaluator, "prepare_story_model")
    def test_story_creation_methods_call_prepare_story_model(
        self, mock_prepare_story_model, mock_render, evaluator_with_series, mock_forecasting_pattern, mock_metric
    ):
        """Test that story creation methods call prepare_story_model."""
        mock_render.return_value = "Mock rendered text"
        mock_prepare_story_model.return_value = {"test": "story"}

        # Test forecasted on track story
        forecast_stats = mock_forecasting_pattern.forecast_vs_target_stats[0]
        result = evaluator_with_series._create_forecasted_on_track_story(
            mock_forecasting_pattern, "test_metric", mock_metric, Granularity.DAY, forecast_stats
        )
        assert result == {"test": "story"}
        mock_prepare_story_model.assert_called()

    def test_prepare_base_context_called_in_populate_template_context(
        self, evaluator, mock_forecasting_pattern, mock_metric
    ):
        """Test that prepare_base_context is called in _populate_template_context."""
        with patch.object(evaluator, "prepare_base_context", return_value={}) as mock_prepare_base:
            forecast_stats = mock_forecasting_pattern.forecast_vs_target_stats[0]
            evaluator._populate_template_context(
                mock_forecasting_pattern, mock_metric, Granularity.DAY, forecast_stats=forecast_stats
            )
            mock_prepare_base.assert_called_once_with(mock_metric, Granularity.DAY)

    def test_export_dataframe_as_story_series_called_in_pacing_story(
        self, evaluator_with_series, mock_forecasting_pattern, mock_metric
    ):
        """Test that export_dataframe_as_story_series is called in pacing story creation."""
        with patch.object(evaluator_with_series, "export_dataframe_as_story_series", return_value=[]) as mock_export:
            with patch(
                "story_manager.story_evaluator.evaluators.forecasting.render_story_text", return_value="Mock text"
            ):
                pacing = mock_forecasting_pattern.pacing[0]
                evaluator_with_series._create_pacing_on_track_story(
                    mock_forecasting_pattern, "test_metric", mock_metric, Granularity.DAY, pacing
                )
                mock_export.assert_called_once()

    def test_period_mapping_comprehensive(self, evaluator):
        """Test comprehensive period mapping in _populate_template_context."""
        test_cases = [
            (PeriodType.END_OF_WEEK, "week"),
            (PeriodType.END_OF_MONTH, "month"),
            (PeriodType.END_OF_QUARTER, "quarter"),
            (PeriodType.END_OF_YEAR, "year"),
        ]

        for period_type, expected_str in test_cases:
            forecast_stats = ForecastVsTargetStats(
                period=period_type,
                forecasted_value=1000.0,
                target_value=900.0,
            )

            context = evaluator._populate_template_context(
                Forecasting(
                    pattern="forecasting",
                    metric_id="test_metric",
                    analysis_window=AnalysisWindow(
                        start_date="2024-01-01", end_date="2024-01-31", grain=Granularity.DAY
                    ),
                    analysis_date=date(2024, 1, 31),
                ),
                {"metric_id": "test_metric", "label": "Test Metric"},
                Granularity.DAY,
                forecast_stats=forecast_stats,
            )

            assert context["period"] == expected_str

    # Additional edge case tests for higher coverage
    def test_populate_template_context_with_zero_target_value(self, evaluator, mock_forecasting_pattern, mock_metric):
        """Test _populate_template_context with zero target value in required performance."""
        required_perf = RequiredPerformance(
            period=PeriodType.END_OF_MONTH,
            remaining_periods=4,
            required_pop_growth_percent=15.0,
            previous_pop_growth_percent=10.0,
            growth_difference=5.0,
            previous_periods=3,
        )

        context = evaluator._populate_template_context(
            mock_forecasting_pattern, mock_metric, Granularity.DAY, required_perf=required_perf
        )

        assert "target_value" in context
        # The context should contain target_value from the forecasting pattern
        assert "target_value" in context

    def test_convert_daily_forecast_to_grain_empty_dataframe(self, evaluator):
        """Test _convert_daily_forecast_to_grain with empty DataFrame."""
        empty_df = pd.DataFrame(columns=["date", "value", "lower_bound", "upper_bound"])

        result = evaluator._convert_daily_forecast_to_grain(empty_df, Granularity.WEEK)

        # Should return empty DataFrame
        assert result.empty

    @patch("levers.primitives.get_period_range_for_grain")
    def test_prepare_forecast_series_data_empty_forecast(
        self, mock_get_range, evaluator_with_series, mock_forecasting_pattern
    ):
        """Test _prepare_forecast_series_data with empty forecast data."""
        mock_get_range.return_value = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-31"))

        # Create pattern with empty forecast
        pattern = Forecasting(
            pattern="forecasting",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2024-01-01", end_date="2024-01-31", grain=Granularity.DAY),
            analysis_date=date(2024, 1, 31),
            forecast=[],  # Empty forecast
        )

        forecast_stats = ForecastVsTargetStats(
            period=PeriodType.END_OF_MONTH, forecasted_value=1200.0, target_value=1000.0
        )

        series_data = evaluator_with_series._prepare_forecast_series_data(pattern, Granularity.DAY, forecast_stats)

        assert isinstance(series_data, list)
        assert len(series_data) == 1

    def test_calculate_cumulative_series_with_zero_values(self, evaluator):
        """Test _calculate_cumulative_series with zero values."""
        df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=3), "value": [0, 0, 0]})  # All zero values

        result = evaluator._calculate_cumulative_series(df, include_bounds=False)

        assert "cumulative_value" in result.columns
        assert result["cumulative_value"].tolist() == [0, 0, 0]

    @pytest.mark.asyncio
    async def test_evaluate_with_empty_forecast_vs_target_stats_but_valid_others(
        self, evaluator_with_series, mock_metric
    ):
        """Test evaluate with empty forecast stats but valid pacing and required performance."""
        pattern = Forecasting(
            pattern="forecasting",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2024-01-01", end_date="2024-01-31", grain=Granularity.DAY),
            analysis_date=date(2024, 1, 31),
            forecast_vs_target_stats=[],  # Empty
            pacing=[
                PacingProjection(
                    period=PeriodType.END_OF_MONTH,
                    period_elapsed_percent=75.0,
                    projected_value=1100.0,
                    target_value=1000.0,
                    gap_percent=10.0,
                    status=MetricGVAStatus.ON_TRACK,
                )
            ],
            required_performance=[
                RequiredPerformance(
                    period=PeriodType.END_OF_MONTH,
                    remaining_periods=4,
                    required_pop_growth_percent=15.0,
                    previous_periods=3,
                )
            ],
            forecast=[],
        )

        stories = await evaluator_with_series.evaluate(pattern, mock_metric)
        assert len(stories) == 2  # One pacing + one required performance story

    def test_get_period_grain_comprehensive(self, evaluator):
        """Test _get_period_grain with all supported period types."""
        period_mappings = [
            (PeriodType.END_OF_WEEK, Granularity.WEEK),
            (PeriodType.END_OF_MONTH, Granularity.MONTH),
            (PeriodType.END_OF_QUARTER, Granularity.QUARTER),
            (PeriodType.END_OF_YEAR, Granularity.YEAR),
            (PeriodType.END_OF_NEXT_MONTH, Granularity.DAY),  # Fallback case
        ]

        for period_type, expected_grain in period_mappings:
            result = evaluator._get_period_grain(period_type)
            assert result == expected_grain
