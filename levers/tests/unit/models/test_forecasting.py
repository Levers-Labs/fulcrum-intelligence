"""
Tests for forecasting models.
"""

from datetime import datetime

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
        assert stats.target_date is None
        assert stats.target_value is None
        assert stats.gap_percent is None
        assert stats.status is None

    def test_populated_forecast_vs_target_stats(self):
        """Test creating a populated forecast vs target stats."""
        stats = ForecastVsTargetStats(
            forecasted_value=150.5,
            target_date="2023-01-31",
            target_value=155.0,
            gap_percent=-3.0,
            status=MetricGVAStatus.OFF_TRACK,
        )

        assert stats.forecasted_value == 150.5
        assert stats.target_date == "2023-01-31"
        assert stats.target_value == 155.0
        assert stats.gap_percent == -3.0
        assert stats.status == MetricGVAStatus.OFF_TRACK

    def test_forecast_vs_target_stats_with_string_status(self):
        """Test creating forecast vs target stats with string status."""
        stats = ForecastVsTargetStats(
            forecasted_value=150.0,
            target_value=155.0,
            status="on_track",
        )

        assert stats.forecasted_value == 150.0
        assert stats.target_value == 155.0
        assert stats.status == "on_track"


class TestPacingProjection:
    """Test PacingProjection model."""

    def test_empty_pacing_projection(self):
        """Test creating an empty pacing projection."""
        pacing = PacingProjection()

        assert pacing.period_elapsed_percent is None
        assert pacing.cumulative_value is None
        assert pacing.projected_value is None
        assert pacing.target_value is None
        assert pacing.gap_percent is None
        assert pacing.status is None

    def test_populated_pacing_projection(self):
        """Test creating a populated pacing projection."""
        pacing = PacingProjection(
            period_elapsed_percent=75.0,
            cumulative_value=100.0,
            projected_value=133.33,
            target_value=150.0,
            gap_percent=5.0,
            status="on_track",
        )

        assert pacing.period_elapsed_percent == 75.0
        assert pacing.cumulative_value == 100.0
        assert pacing.projected_value == 133.33
        assert pacing.target_value == 150.0
        assert pacing.gap_percent == 5.0
        assert pacing.status == "on_track"

    def test_pacing_projection_with_100_percent_elapsed(self):
        """Test pacing projection with 100% elapsed."""
        pacing = PacingProjection(
            period_elapsed_percent=100.0,
            cumulative_value=200.0,
            projected_value=200.0,
            target_value=200.0,
            gap_percent=0.0,
            status="on_track",
        )

        assert pacing.period_elapsed_percent == 100.0
        assert pacing.cumulative_value == 200.0
        assert pacing.projected_value == 200.0
        assert pacing.target_value == 200.0
        assert pacing.gap_percent == 0.0


class TestRequiredPerformance:
    """Test RequiredPerformance model."""

    def test_empty_required_performance(self):
        """Test creating an empty required performance."""
        req_perf = RequiredPerformance()

        assert req_perf.remaining_periods is None
        assert req_perf.required_pop_growth_percent is None
        assert req_perf.previous_pop_growth_percent is None
        assert req_perf.growth_difference is None

    def test_populated_required_performance(self):
        """Test creating a populated required performance."""
        req_perf = RequiredPerformance(
            remaining_periods=15,
            required_pop_growth_percent=10.5,
            previous_pop_growth_percent=8.0,
            growth_difference=2.5,
        )

        assert req_perf.remaining_periods == 15
        assert req_perf.required_pop_growth_percent == 10.5
        assert req_perf.previous_pop_growth_percent == 8.0
        assert req_perf.growth_difference == 2.5

    def test_required_performance_negative_delta(self):
        """Test required performance with negative delta."""
        req_perf = RequiredPerformance(
            remaining_periods=10,
            required_pop_growth_percent=5.0,
            previous_pop_growth_percent=8.0,
            growth_difference=-3.0,
        )

        assert req_perf.growth_difference == -3.0


class TestForecast:
    """Test Forecast model."""

    def test_minimal_forecast(self):
        """Test creating a minimal forecast."""
        forecast = Forecast(date="2023-12-31")

        assert forecast.date == "2023-12-31"
        assert forecast.forecasted_value is None
        assert forecast.lower_bound is None
        assert forecast.upper_bound is None

    def test_full_forecast(self):
        """Test creating a full forecast."""
        forecast = Forecast(date="2023-12-31", forecasted_value=125.5, lower_bound=120.0, upper_bound=131.0)

        assert forecast.date == "2023-12-31"
        assert forecast.forecasted_value == 125.5
        assert forecast.lower_bound == 120.0
        assert forecast.upper_bound == 131.0

    def test_forecast_with_zero_values(self):
        """Test forecast with zero values."""
        forecast = Forecast(date="2023-12-31", forecasted_value=0.0, lower_bound=0.0, upper_bound=0.0)

        assert forecast.forecasted_value == 0.0
        assert forecast.lower_bound == 0.0
        assert forecast.upper_bound == 0.0


class TestForecasting:
    """Test Forecasting pattern model."""

    def test_minimal_forecasting(self):
        """Test creating a minimal forecasting result."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(
            metric_id="test_metric", analysis_window=analysis_window, forecast_period_grain=Granularity.DAY
        )

        assert forecasting.pattern == "forecasting"
        assert forecasting.metric_id == "test_metric"
        assert forecasting.analysis_window == analysis_window
        assert forecasting.forecast_period_grain == Granularity.DAY
        assert forecasting.forecast_vs_target_stats is None

    def test_full_forecasting(self):
        """Test creating a full forecasting result."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(
            metric_id="test_metric",
            analysis_window=analysis_window,
            forecast_period_grain=Granularity.WEEK,
            forecast_vs_target_stats=ForecastVsTargetStats(
                forecasted_value=150.0,
                target_value=155.0,
                gap_percent=-3.2,
                status=MetricGVAStatus.OFF_TRACK,
            ),
            pacing=PacingProjection(
                period_elapsed_percent=75.0,
                cumulative_value=100.0,
                projected_value=133.33,
                gap_percent=5.0,
                status="on_track",
            ),
            required_performance=RequiredPerformance(
                remaining_periods=15,
                required_pop_growth_percent=10.5,
                previous_pop_growth_percent=8.0,
                growth_difference=2.5,
            ),
            period_forecast=[
                Forecast(date="2023-02-01", forecasted_value=130.0),
                Forecast(date="2023-02-02", forecasted_value=131.0),
            ],
            evaluation_time=datetime(2023, 1, 31, 12, 0),
        )

        assert forecasting.pattern == "forecasting"
        assert forecasting.metric_id == "test_metric"
        assert forecasting.forecast_period_grain == Granularity.WEEK
        assert forecasting.forecast_vs_target_stats is not None
        assert forecasting.pacing is not None
        assert forecasting.required_performance is not None
        assert len(forecasting.period_forecast) == 2

    def test_forecasting_serialization(self):
        """Test forecasting serialization and deserialization."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(
            metric_id="test_metric", analysis_window=analysis_window, forecast_period_grain=Granularity.MONTH
        )

        # Test serialization
        data = forecasting.model_dump()
        assert data["pattern"] == "forecasting"
        assert data["metric_id"] == "test_metric"
        assert data["forecast_period_grain"] == "month"

        # Test deserialization
        reconstructed = Forecasting(**data)
        assert reconstructed.pattern == "forecasting"
        assert reconstructed.metric_id == "test_metric"
        assert reconstructed.forecast_period_grain == Granularity.MONTH

    def test_forecasting_with_string_period_type(self):
        """Test forecasting with string period type."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(
            metric_id="test_metric", analysis_window=analysis_window, forecast_period_grain="week"
        )

        assert forecasting.forecast_period_grain == Granularity.WEEK


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
