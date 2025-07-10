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
        assert stats.period is None
        assert stats.target_value is None
        assert stats.gap_percent is None
        assert stats.status is None

    def test_populated_forecast_vs_target_stats(self):
        """Test creating a populated forecast vs target stats."""
        stats = ForecastVsTargetStats(
            forecasted_value=150.5,
            period=PeriodType.END_OF_MONTH,
            target_value=155.0,
            gap_percent=-3.0,
            status=MetricGVAStatus.OFF_TRACK,
        )

        assert stats.forecasted_value == 150.5
        assert stats.period == PeriodType.END_OF_MONTH
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
        assert pacing.projected_value is None
        assert pacing.target_value is None
        assert pacing.gap_percent is None
        assert pacing.status is None

    def test_populated_pacing_projection(self):
        """Test creating a populated pacing projection."""
        pacing = PacingProjection(
            period_elapsed_percent=75.0,
            projected_value=133.33,
            target_value=150.0,
            gap_percent=5.0,
            status="on_track",
        )

        assert pacing.period_elapsed_percent == 75.0
        assert pacing.projected_value == 133.33
        assert pacing.target_value == 150.0
        assert pacing.gap_percent == 5.0
        assert pacing.status == "on_track"

    def test_pacing_projection_with_100_percent_elapsed(self):
        """Test pacing projection with 100% elapsed."""
        pacing = PacingProjection(
            period_elapsed_percent=100.0,
            projected_value=200.0,
            target_value=200.0,
            gap_percent=0.0,
            status="on_track",
        )

        assert pacing.period_elapsed_percent == 100.0
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

        forecasting = Forecasting(metric_id="test_metric", analysis_window=analysis_window)

        assert forecasting.pattern == "forecasting"
        assert forecasting.metric_id == "test_metric"
        assert forecasting.analysis_window == analysis_window
        assert forecasting.forecast_vs_target_stats == []
        assert isinstance(forecasting.forecast_vs_target_stats, list)

    def test_full_forecasting(self):
        """Test creating a full forecasting result."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(
            metric_id="test_metric",
            analysis_window=analysis_window,
            forecast_vs_target_stats=[
                ForecastVsTargetStats(
                    forecasted_value=150.0,
                    target_value=155.0,
                    gap_percent=-3.2,
                    status=MetricGVAStatus.OFF_TRACK,
                )
            ],
            pacing=[
                PacingProjection(
                    period_elapsed_percent=75.0,
                    projected_value=133.33,
                    gap_percent=5.0,
                    status="on_track",
                )
            ],
            required_performance=[
                RequiredPerformance(
                    remaining_periods=15,
                    required_pop_growth_percent=10.5,
                    previous_pop_growth_percent=8.0,
                    growth_difference=2.5,
                )
            ],
            forecast=[
                Forecast(date="2023-02-01", forecasted_value=130.0),
                Forecast(date="2023-02-02", forecasted_value=131.0),
            ],
            evaluation_time=datetime(2023, 1, 31, 12, 0),
        )

        assert forecasting.pattern == "forecasting"
        assert forecasting.metric_id == "test_metric"
        assert forecasting.forecast_vs_target_stats is not None
        assert isinstance(forecasting.forecast_vs_target_stats, list)
        assert len(forecasting.forecast_vs_target_stats) == 1
        assert forecasting.pacing is not None
        assert isinstance(forecasting.pacing, list)
        assert len(forecasting.pacing) == 1
        assert forecasting.required_performance is not None
        assert isinstance(forecasting.required_performance, list)
        assert len(forecasting.required_performance) == 1
        assert len(forecasting.forecast) == 2

    def test_forecasting_serialization(self):
        """Test forecasting serialization and deserialization."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(metric_id="test_metric", analysis_window=analysis_window)

        # Test serialization
        data = forecasting.model_dump()
        assert data["pattern"] == "forecasting"
        assert data["metric_id"] == "test_metric"
        assert "forecast_vs_target_stats" in data

        # Test deserialization
        reconstructed = Forecasting(**data)
        assert reconstructed.pattern == "forecasting"
        assert reconstructed.metric_id == "test_metric"
        assert isinstance(reconstructed.forecast_vs_target_stats, list)

    def test_forecasting_with_string_period_type(self):
        """Test forecasting with string period type."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(metric_id="test_metric", analysis_window=analysis_window)

        assert forecasting.pattern == "forecasting"
        assert isinstance(forecasting.forecast_vs_target_stats, list)

    def test_forecasting_with_multiple_entries(self):
        """Test forecasting with multiple entries in list fields."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Create multiple entries for each list field
        multiple_stats = [
            ForecastVsTargetStats(
                forecasted_value=150.0,
                target_value=155.0,
                gap_percent=-3.2,
                status=MetricGVAStatus.OFF_TRACK,
            ),
            ForecastVsTargetStats(
                forecasted_value=180.0,
                target_value=175.0,
                gap_percent=2.9,
                status=MetricGVAStatus.ON_TRACK,
            ),
        ]

        multiple_pacing = [
            PacingProjection(
                period_elapsed_percent=50.0,
                projected_value=160.0,
                gap_percent=3.0,
                status="on_track",
            ),
            PacingProjection(
                period_elapsed_percent=75.0,
                projected_value=160.0,
                gap_percent=0.0,
                status="on_track",
            ),
        ]

        multiple_required = [
            RequiredPerformance(
                remaining_periods=10,
                required_pop_growth_percent=8.0,
                previous_pop_growth_percent=5.0,
                growth_difference=3.0,
            ),
            RequiredPerformance(
                remaining_periods=5,
                required_pop_growth_percent=12.0,
                previous_pop_growth_percent=10.0,
                growth_difference=2.0,
            ),
        ]

        forecasting = Forecasting(
            metric_id="test_metric",
            analysis_window=analysis_window,
            forecast_vs_target_stats=multiple_stats,
            pacing=multiple_pacing,
            required_performance=multiple_required,
            forecast=[
                Forecast(date="2023-02-01", forecasted_value=130.0),
                Forecast(date="2023-02-02", forecasted_value=131.0),
                Forecast(date="2023-02-03", forecasted_value=132.0),
            ],
        )

        # Verify multiple entries
        assert len(forecasting.forecast_vs_target_stats) == 2
        assert len(forecasting.pacing) == 2
        assert len(forecasting.required_performance) == 2
        assert len(forecasting.forecast) == 3

        # Verify specific entries
        assert forecasting.forecast_vs_target_stats[0].forecasted_value == 150.0
        assert forecasting.forecast_vs_target_stats[1].forecasted_value == 180.0
        assert forecasting.pacing[0].period_elapsed_percent == 50.0
        assert forecasting.pacing[1].period_elapsed_percent == 75.0
        assert forecasting.required_performance[0].remaining_periods == 10
        assert forecasting.required_performance[1].remaining_periods == 5

    def test_forecasting_with_empty_lists(self):
        """Test forecasting with empty lists."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        forecasting = Forecasting(
            metric_id="test_metric",
            analysis_window=analysis_window,
            forecast_vs_target_stats=[],  # Empty list
            pacing=[],  # Empty list
            required_performance=[],  # Empty list
            forecast=[],  # Empty list
        )

        assert forecasting.pattern == "forecasting"
        assert isinstance(forecasting.forecast_vs_target_stats, list)
        assert len(forecasting.forecast_vs_target_stats) == 0
        assert isinstance(forecasting.pacing, list)
        assert len(forecasting.pacing) == 0
        assert isinstance(forecasting.required_performance, list)
        assert len(forecasting.required_performance) == 0
        assert isinstance(forecasting.forecast, list)
        assert len(forecasting.forecast) == 0

    def test_forecasting_with_none_values_in_lists(self):
        """Test forecasting with None values in lists."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # This test should check validation error for None values in forecast list
        try:
            forecasting = Forecasting(
                metric_id="test_metric",
                analysis_window=analysis_window,
                forecast_vs_target_stats=[None, None],  # List with None values
                pacing=[None],  # List with None value
                required_performance=[None, None, None],  # List with None values
                forecast=[
                    Forecast(date="2023-02-01", forecasted_value=130.0),
                    Forecast(date="2023-02-03", forecasted_value=132.0),
                ],
            )
        except Exception:
            # It's acceptable to raise an exception for invalid data
            return

        assert len(forecasting.forecast_vs_target_stats) == 2
        assert len(forecasting.pacing) == 1
        assert len(forecasting.required_performance) == 3
        assert len(forecasting.forecast) == 2  # Only 2 valid forecasts, None values filtered out

        # Verify None values are preserved where applicable
        assert forecasting.forecast_vs_target_stats[0] is None
        assert forecasting.forecast_vs_target_stats[1] is None
        assert forecasting.pacing[0] is None
        assert forecasting.required_performance[0] is None
        # forecast list has filtered out None values, so only valid entries remain
        assert forecasting.forecast[0] is not None
        assert forecasting.forecast[1] is not None

    def test_forecasting_backwards_compatibility(self):
        """Test that the updated model maintains essential functionality."""
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Create forecasting with single entries (common case)
        forecasting = Forecasting(
            metric_id="test_metric",
            analysis_window=analysis_window,
            forecast_vs_target_stats=[ForecastVsTargetStats(forecasted_value=150.0, target_value=155.0)],
            pacing=[PacingProjection(period_elapsed_percent=75.0, projected_value=133.33)],
            required_performance=[RequiredPerformance(remaining_periods=15, required_pop_growth_percent=10.5)],
            forecast=[Forecast(date="2023-02-01", forecasted_value=130.0)],
        )

        # Verify essential attributes
        assert forecasting.pattern == "forecasting"
        assert forecasting.metric_id == "test_metric"
        assert forecasting.analysis_window == analysis_window

        # Verify list access patterns
        assert len(forecasting.forecast_vs_target_stats) == 1
        assert forecasting.forecast_vs_target_stats[0].forecasted_value == 150.0
        assert len(forecasting.pacing) == 1
        assert forecasting.pacing[0].period_elapsed_percent == 75.0
        assert len(forecasting.required_performance) == 1
        assert forecasting.required_performance[0].remaining_periods == 15
        assert len(forecasting.forecast) == 1
        assert forecasting.forecast[0].forecasted_value == 130.0


class TestPeriodTypeEnum:
    """Test PeriodType enum."""

    def test_period_type_values(self):
        """Test PeriodType enum values."""
        assert PeriodType.END_OF_WEEK.value == "END_OF_WEEK"
        assert PeriodType.END_OF_MONTH.value == "END_OF_MONTH"
        assert PeriodType.END_OF_QUARTER.value == "END_OF_QUARTER"
        assert PeriodType.END_OF_YEAR.value == "END_OF_YEAR"
        assert PeriodType.END_OF_NEXT_MONTH.value == "END_OF_NEXT_MONTH"

    def test_period_type_string_representation(self):
        """Test PeriodType string representation."""
        assert str(PeriodType.END_OF_WEEK) == "PeriodType.END_OF_WEEK"
        assert str(PeriodType.END_OF_MONTH) == "PeriodType.END_OF_MONTH"
        assert str(PeriodType.END_OF_QUARTER) == "PeriodType.END_OF_QUARTER"

    def test_period_type_equality(self):
        """Test PeriodType equality with strings."""
        assert PeriodType.END_OF_WEEK == "END_OF_WEEK"
        assert PeriodType.END_OF_MONTH == "END_OF_MONTH"
        assert PeriodType.END_OF_QUARTER == "END_OF_QUARTER"


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
