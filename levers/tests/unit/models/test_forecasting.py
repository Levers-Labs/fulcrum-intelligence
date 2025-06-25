"""
Tests for forecasting models.
"""

from datetime import date, datetime

from levers.models import AnalysisWindow, Granularity
from levers.models.enums import ForecastMethod, MetricGVAStatus, PeriodType
from levers.models.forecasting import (
    Forecast,
    ForecastVsTargetStats,
    PacingProjection,
    RequiredPerformance,
)
from levers.models.patterns.forecasting import Forecasting


class TestForecastVsTargetStats:
    """Test ForecastVsTargetStats model."""

    def test_empty_forecast_vs_target_stats(self):
        """Test creating an empty forecast vs target stats."""
        stats = ForecastVsTargetStats()

        assert stats.forecasted_value is None
        assert stats.lower_bound is None
        assert stats.upper_bound is None
        assert stats.confidence_level is None
        assert stats.target_value is None
        assert stats.forecasted_gap_percent is None
        assert stats.forecast_status is None

    def test_populated_forecast_vs_target_stats(self):
        """Test creating a populated forecast vs target stats."""
        stats = ForecastVsTargetStats(
            forecasted_value=150.5,
            lower_bound=140.0,
            upper_bound=160.0,
            confidence_level=0.95,
            target_value=155.0,
            forecasted_gap_percent=-3.0,
            forecast_status=MetricGVAStatus.OFF_TRACK,
        )

        assert stats.forecasted_value == 150.5
        assert stats.lower_bound == 140.0
        assert stats.upper_bound == 160.0
        assert stats.confidence_level == 0.95
        assert stats.target_value == 155.0
        assert stats.forecasted_gap_percent == -3.0
        assert stats.forecast_status == MetricGVAStatus.OFF_TRACK

    def test_forecast_vs_target_stats_with_string_status(self):
        """Test creating forecast vs target stats with string status."""
        stats = ForecastVsTargetStats(
            forecasted_value=150.0,
            target_value=155.0,
            forecast_status="on_track",
        )

        assert stats.forecasted_value == 150.0
        assert stats.target_value == 155.0
        assert stats.forecast_status == "on_track"


class TestPacingProjection:
    """Test PacingProjection model."""

    def test_empty_pacing_projection(self):
        """Test creating an empty pacing projection."""
        pacing = PacingProjection()

        assert pacing.percent_of_period_elapsed is None
        assert pacing.cumulative_value is None
        assert pacing.projected_value is None
        assert pacing.gap_percent is None
        assert pacing.status is None

    def test_populated_pacing_projection(self):
        """Test creating a populated pacing projection."""
        pacing = PacingProjection(
            percent_of_period_elapsed=75.0,
            cumulative_value=100.0,
            projected_value=133.33,
            gap_percent=5.0,
            status="on_track",
        )

        assert pacing.percent_of_period_elapsed == 75.0
        assert pacing.cumulative_value == 100.0
        assert pacing.projected_value == 133.33
        assert pacing.gap_percent == 5.0
        assert pacing.status == "on_track"

    def test_pacing_projection_with_100_percent_elapsed(self):
        """Test pacing projection with 100% elapsed."""
        pacing = PacingProjection(
            percent_of_period_elapsed=100.0,
            cumulative_value=200.0,
            projected_value=200.0,
            gap_percent=0.0,
            status="on_track",
        )

        assert pacing.percent_of_period_elapsed == 100.0
        assert pacing.cumulative_value == 200.0
        assert pacing.projected_value == 200.0
        assert pacing.gap_percent == 0.0


class TestRequiredPerformance:
    """Test RequiredPerformance model."""

    def test_empty_required_performance(self):
        """Test creating an empty required performance."""
        req_perf = RequiredPerformance()

        assert req_perf.remaining_periods_count is None
        assert req_perf.required_pop_growth_percent is None
        assert req_perf.past_pop_growth_percent is None
        assert req_perf.delta_from_historical_growth is None

    def test_populated_required_performance(self):
        """Test creating a populated required performance."""
        req_perf = RequiredPerformance(
            remaining_periods_count=15,
            required_pop_growth_percent=10.5,
            past_pop_growth_percent=8.0,
            delta_from_historical_growth=2.5,
        )

        assert req_perf.remaining_periods_count == 15
        assert req_perf.required_pop_growth_percent == 10.5
        assert req_perf.past_pop_growth_percent == 8.0
        assert req_perf.delta_from_historical_growth == 2.5

    def test_required_performance_negative_delta(self):
        """Test required performance with negative delta."""
        req_perf = RequiredPerformance(
            remaining_periods_count=10,
            required_pop_growth_percent=5.0,
            past_pop_growth_percent=8.0,
            delta_from_historical_growth=-3.0,
        )

        assert req_perf.delta_from_historical_growth == -3.0


class TestForecast:
    """Test Forecast model."""

    def test_minimal_forecast(self):
        """Test creating a minimal forecast."""
        forecast = Forecast(date="2023-12-31")

        assert forecast.date == "2023-12-31"
        assert forecast.forecasted_value is None
        assert forecast.lower_bound is None
        assert forecast.upper_bound is None
        assert forecast.confidence_level is None

    def test_full_forecast(self):
        """Test creating a full forecast."""
        forecast = Forecast(
            date="2023-12-31", forecasted_value=125.5, lower_bound=120.0, upper_bound=131.0, confidence_level=0.95
        )

        assert forecast.date == "2023-12-31"
        assert forecast.forecasted_value == 125.5
        assert forecast.lower_bound == 120.0
        assert forecast.upper_bound == 131.0
        assert forecast.confidence_level == 0.95

    def test_forecast_with_zero_values(self):
        """Test forecast with zero values."""
        forecast = Forecast(
            date="2023-12-31", forecasted_value=0.0, lower_bound=0.0, upper_bound=0.0, confidence_level=0.95
        )

        assert forecast.forecasted_value == 0.0
        assert forecast.lower_bound == 0.0
        assert forecast.upper_bound == 0.0


class TestForecasting:
    """Test Forecasting pattern model."""

    def test_minimal_forecasting(self):
        """Test creating a minimal forecasting result."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(
            metric_id="test_metric", analysis_window=analysis_window, period_type=PeriodType.END_OF_MONTH
        )

        assert forecasting.pattern == "forecasting"
        assert forecasting.metric_id == "test_metric"
        assert forecasting.analysis_window == analysis_window
        assert forecasting.period_type == PeriodType.END_OF_MONTH
        assert forecasting.forecast_vs_target_stats is None
        assert forecasting.pacing is None
        assert forecasting.required_performance is None
        assert forecasting.period_forecast is None

    def test_full_forecasting(self):
        """Test creating a full forecasting result."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Create forecast vs target stats
        forecast_vs_target_stats = ForecastVsTargetStats(
            forecasted_value=150.0, target_value=155.0, forecast_status=MetricGVAStatus.OFF_TRACK
        )

        # Create pacing projection
        pacing = PacingProjection(
            percent_of_period_elapsed=80.0, cumulative_value=120.0, projected_value=150.0, status="on_track"
        )

        # Create required performance
        required_performance = RequiredPerformance(
            remaining_periods_count=10,
            required_pop_growth_percent=12.0,
            past_pop_growth_percent=8.5,
            delta_from_historical_growth=3.5,
        )

        # Create period forecasts
        forecast1 = Forecast(date="2023-02-01", forecasted_value=110.0)
        forecast2 = Forecast(date="2023-02-02", forecasted_value=112.0)

        forecasting = Forecasting(
            metric_id="test_metric",
            analysis_window=analysis_window,
            period_type=PeriodType.END_OF_MONTH,
            forecast_vs_target_stats=forecast_vs_target_stats,
            pacing=pacing,
            required_performance=required_performance,
            period_forecast=[forecast1, forecast2],
            analysis_date=date(2023, 1, 31),
            evaluation_time=datetime(2023, 1, 31, 12, 0, 0),
        )

        assert forecasting.metric_id == "test_metric"
        assert forecasting.analysis_window == analysis_window
        assert forecasting.period_type == PeriodType.END_OF_MONTH
        assert forecasting.forecast_vs_target_stats == forecast_vs_target_stats
        assert forecasting.pacing == pacing
        assert forecasting.required_performance == required_performance
        assert len(forecasting.period_forecast) == 2

        # Verify nested objects
        assert forecasting.forecast_vs_target_stats.forecasted_value == 150.0
        assert forecasting.pacing.percent_of_period_elapsed == 80.0
        assert forecasting.required_performance.remaining_periods_count == 10
        assert forecasting.period_forecast[0].date == "2023-02-01"
        assert forecasting.period_forecast[0].forecasted_value == 110.0

    def test_forecasting_serialization(self):
        """Test that the forecasting model can be serialized properly."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(
            metric_id="test_metric", analysis_window=analysis_window, period_type=PeriodType.END_OF_MONTH
        )

        # Test to_dict method
        result_dict = forecasting.to_dict()
        assert isinstance(result_dict, dict)
        assert result_dict["pattern"] == "forecasting"
        assert result_dict["metric_id"] == "test_metric"
        assert result_dict["period_type"] == "endOfMonth"

        # Test JSON serialization
        json_str = forecasting.model_dump_json()
        assert isinstance(json_str, str)
        assert "test_metric" in json_str
        assert "endOfMonth" in json_str

    def test_forecasting_with_string_period_type(self):
        """Test creating forecasting with string period type."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(metric_id="test_metric", analysis_window=analysis_window, period_type="endOfWeek")

        assert forecasting.period_type == "endOfWeek"


class TestPeriodTypeEnum:
    """Test PeriodType enum."""

    def test_period_type_values(self):
        """Test PeriodType enum values."""
        assert PeriodType.END_OF_WEEK.value == "endOfWeek"
        assert PeriodType.END_OF_MONTH.value == "endOfMonth"
        assert PeriodType.END_OF_QUARTER.value == "endOfQuarter"
        assert PeriodType.END_OF_YEAR.value == "endOfYear"
        assert PeriodType.END_OF_NEXT_MONTH.value == "endOfNextMonth"

    def test_period_type_string_representation(self):
        """Test PeriodType string representation."""
        assert str(PeriodType.END_OF_WEEK) == "PeriodType.END_OF_WEEK"
        assert str(PeriodType.END_OF_MONTH) == "PeriodType.END_OF_MONTH"
        assert str(PeriodType.END_OF_QUARTER) == "PeriodType.END_OF_QUARTER"

    def test_period_type_equality(self):
        """Test PeriodType equality with strings."""
        assert PeriodType.END_OF_WEEK == "endOfWeek"
        assert PeriodType.END_OF_MONTH == "endOfMonth"
        assert PeriodType.END_OF_QUARTER == "endOfQuarter"


class TestForecastMethodEnum:
    """Test ForecastMethod enum."""

    def test_forecast_method_values(self):
        """Test ForecastMethod enum values."""
        assert ForecastMethod.NAIVE.value == "naive"
        assert ForecastMethod.SES.value == "ses"
        assert ForecastMethod.HOLT_WINTERS.value == "holtwinters"
        assert ForecastMethod.AUTO_ARIMA.value == "auto_arima"

    def test_forecast_method_string_representation(self):
        """Test ForecastMethod string representation."""
        assert str(ForecastMethod.NAIVE) == "ForecastMethod.NAIVE"
        assert str(ForecastMethod.SES) == "ForecastMethod.SES"
        assert str(ForecastMethod.HOLT_WINTERS) == "ForecastMethod.HOLT_WINTERS"
        assert str(ForecastMethod.AUTO_ARIMA) == "ForecastMethod.AUTO_ARIMA"

    def test_forecast_method_equality(self):
        """Test ForecastMethod equality with strings."""
        assert ForecastMethod.NAIVE == "naive"
        assert ForecastMethod.SES == "ses"
        assert ForecastMethod.HOLT_WINTERS == "holtwinters"
        assert ForecastMethod.AUTO_ARIMA == "auto_arima"


class TestMetricGVAStatusEnum:
    """Test MetricGVAStatus enum."""

    def test_metric_gva_status_values(self):
        """Test MetricGVAStatus enum values."""
        assert MetricGVAStatus.ON_TRACK.value == "on_track"
        assert MetricGVAStatus.OFF_TRACK.value == "off_track"
        assert MetricGVAStatus.NO_TARGET.value == "no_target"

    def test_metric_gva_status_string_representation(self):
        """Test MetricGVAStatus string representation."""
        assert str(MetricGVAStatus.ON_TRACK) == "MetricGVAStatus.ON_TRACK"
        assert str(MetricGVAStatus.OFF_TRACK) == "MetricGVAStatus.OFF_TRACK"
        assert str(MetricGVAStatus.NO_TARGET) == "MetricGVAStatus.NO_TARGET"

    def test_metric_gva_status_equality(self):
        """Test MetricGVAStatus equality with strings."""
        assert MetricGVAStatus.ON_TRACK == "on_track"
        assert MetricGVAStatus.OFF_TRACK == "off_track"
        assert MetricGVAStatus.NO_TARGET == "no_target"
