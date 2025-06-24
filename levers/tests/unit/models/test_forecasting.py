"""
Tests for forecasting models.
"""

from levers.models import AnalysisWindow, Granularity
from levers.models.forecasting import (
    DailyForecast,
    PacingProjection,
    RequiredPerformance,
    StatisticalForecast,
)
from levers.models.patterns.forecasting import Forecasting


class TestStatisticalForecast:
    """Test StatisticalForecast model."""

    def test_empty_statistical_forecast(self):
        """Test creating an empty statistical forecast."""
        forecast = StatisticalForecast()

        assert forecast.forecasted_value is None
        assert forecast.lower_bound is None
        assert forecast.upper_bound is None
        assert forecast.confidence_level is None
        assert forecast.target_value is None
        assert forecast.forecasted_gap_percent is None
        assert forecast.forecast_status is None

    def test_populated_statistical_forecast(self):
        """Test creating a populated statistical forecast."""
        forecast = StatisticalForecast(
            forecasted_value=150.5,
            lower_bound=140.0,
            upper_bound=160.0,
            confidence_level=0.95,
            target_value=155.0,
            forecasted_gap_percent=-3.0,
            forecast_status="off_track",
        )

        assert forecast.forecasted_value == 150.5
        assert forecast.lower_bound == 140.0
        assert forecast.upper_bound == 160.0
        assert forecast.confidence_level == 0.95
        assert forecast.target_value == 155.0
        assert forecast.forecasted_gap_percent == -3.0
        assert forecast.forecast_status == "off_track"


class TestPacingProjection:
    """Test PacingProjection model."""

    def test_empty_pacing_projection(self):
        """Test creating an empty pacing projection."""
        pacing = PacingProjection()

        assert pacing.percent_of_period_elapsed is None
        assert pacing.current_cumulative_value is None
        assert pacing.projected_value is None
        assert pacing.gap_percent is None
        assert pacing.status is None

    def test_populated_pacing_projection(self):
        """Test creating a populated pacing projection."""
        pacing = PacingProjection(
            percent_of_period_elapsed=75.0,
            current_cumulative_value=100.0,
            projected_value=133.33,
            gap_percent=5.0,
            status="on_track",
        )

        assert pacing.percent_of_period_elapsed == 75.0
        assert pacing.current_cumulative_value == 100.0
        assert pacing.projected_value == 133.33
        assert pacing.gap_percent == 5.0
        assert pacing.status == "on_track"


class TestRequiredPerformance:
    """Test RequiredPerformance model."""

    def test_empty_required_performance(self):
        """Test creating an empty required performance."""
        req_perf = RequiredPerformance()

        assert req_perf.remaining_grains_count is None
        assert req_perf.required_pop_growth_percent is None
        assert req_perf.past_pop_growth_percent is None
        assert req_perf.delta_from_historical_growth is None

    def test_populated_required_performance(self):
        """Test creating a populated required performance."""
        req_perf = RequiredPerformance(
            remaining_grains_count=15,
            required_pop_growth_percent=10.5,
            past_pop_growth_percent=8.0,
            delta_from_historical_growth=2.5,
        )

        assert req_perf.remaining_grains_count == 15
        assert req_perf.required_pop_growth_percent == 10.5
        assert req_perf.past_pop_growth_percent == 8.0
        assert req_perf.delta_from_historical_growth == 2.5


class TestDailyForecast:
    """Test DailyForecast model."""

    def test_minimal_daily_forecast(self):
        """Test creating a minimal daily forecast."""
        daily = DailyForecast(date="2023-12-31")

        assert daily.date == "2023-12-31"
        assert daily.forecasted_value is None
        assert daily.lower_bound is None
        assert daily.upper_bound is None
        assert daily.confidence_level is None

    def test_full_daily_forecast(self):
        """Test creating a full daily forecast."""
        daily = DailyForecast(
            date="2023-12-31", forecasted_value=125.5, lower_bound=120.0, upper_bound=131.0, confidence_level=0.95
        )

        assert daily.date == "2023-12-31"
        assert daily.forecasted_value == 125.5
        assert daily.lower_bound == 120.0
        assert daily.upper_bound == 131.0
        assert daily.confidence_level == 0.95


class TestForecasting:
    """Test Forecasting pattern model."""

    def test_minimal_forecasting(self):
        """Test creating a minimal forecasting result."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(metric_id="test_metric", analysis_window=analysis_window, period_name="endOfMonth")

        assert forecasting.pattern == "forecasting"
        assert forecasting.metric_id == "test_metric"
        assert forecasting.analysis_window == analysis_window
        assert forecasting.period_name == "endOfMonth"
        assert forecasting.statistical is None
        assert forecasting.pacing is None
        assert forecasting.required_performance is None
        assert len(forecasting.daily_forecast) == 0

    def test_full_forecasting(self):
        """Test creating a full forecasting result."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Create statistical forecast
        statistical = StatisticalForecast(forecasted_value=150.0, target_value=155.0, forecast_status="off_track")

        # Create pacing projection
        pacing = PacingProjection(
            percent_of_period_elapsed=80.0, current_cumulative_value=120.0, projected_value=150.0, status="on_track"
        )

        # Create required performance
        required_performance = RequiredPerformance(
            remaining_grains_count=10,
            required_pop_growth_percent=12.0,
            past_pop_growth_percent=8.5,
            delta_from_historical_growth=3.5,
        )

        # Create daily forecasts
        daily1 = DailyForecast(date="2023-02-01", forecasted_value=110.0)
        daily2 = DailyForecast(date="2023-02-02", forecasted_value=112.0)

        forecasting = Forecasting(
            metric_id="test_metric",
            analysis_window=analysis_window,
            period_name="endOfMonth",
            statistical=statistical,
            pacing=pacing,
            required_performance=required_performance,
            daily_forecast=[daily1, daily2],
        )

        assert forecasting.metric_id == "test_metric"
        assert forecasting.analysis_window == analysis_window
        assert forecasting.period_name == "endOfMonth"
        assert forecasting.statistical == statistical
        assert forecasting.pacing == pacing
        assert forecasting.required_performance == required_performance
        assert len(forecasting.daily_forecast) == 2

        # Verify nested objects
        assert forecasting.statistical.forecasted_value == 150.0
        assert forecasting.pacing.percent_of_period_elapsed == 80.0
        assert forecasting.required_performance.remaining_grains_count == 10
        assert forecasting.daily_forecast[0].date == "2023-02-01"
        assert forecasting.daily_forecast[0].forecasted_value == 110.0

    def test_forecasting_serialization(self):
        """Test that the forecasting model can be serialized properly."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(metric_id="test_metric", analysis_window=analysis_window, period_name="endOfMonth")

        # Test to_dict method
        result_dict = forecasting.to_dict()
        assert isinstance(result_dict, dict)
        assert result_dict["pattern"] == "forecasting"
        assert result_dict["metric_id"] == "test_metric"
        assert result_dict["period_name"] == "endOfMonth"

        # Test JSON serialization
        json_str = forecasting.model_dump_json()
        assert isinstance(json_str, str)
        assert "test_metric" in json_str
        assert "endOfMonth" in json_str
