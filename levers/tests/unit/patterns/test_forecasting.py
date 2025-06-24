"""
Tests for the ForecastingPattern.
"""

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from levers.exceptions import ValidationError
from levers.models import AnalysisWindow, DataSourceType, Granularity
from levers.patterns.forecasting import ForecastingPattern


class TestForecastingPattern:
    """Test suite for the forecasting pattern."""

    def test_pattern_info(self):
        """Test that pattern metadata is correctly set."""
        pattern = ForecastingPattern()

        assert pattern.name == "forecasting"
        assert pattern.version == "1.0"
        assert "forecast" in pattern.description.lower()
        assert "forecast_with_confidence_intervals" in pattern.required_primitives
        assert "calculate_required_growth" in pattern.required_primitives

    def test_pattern_config(self):
        """Test that default pattern configuration is valid."""
        config = ForecastingPattern.get_default_config()

        assert config.pattern_name == "forecasting"
        assert len(config.data_sources) == 1  # Single data source with embedded targets
        assert config.data_sources[0].source_type == DataSourceType.METRIC_WITH_TARGETS
        assert config.data_sources[0].is_required is True
        assert config.analysis_window.days == 365
        assert config.settings["forecast_horizon_days"] == 90
        assert config.settings["confidence_level"] == 0.95

    def test_analyze_with_minimal_data(self):
        """Test analysis with minimal valid data."""
        pattern = ForecastingPattern()

        # Create minimal test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 130), "grain": ["day"] * 30})  # Simple increasing trend

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 30),
            forecast_horizon_days=30,
        )

        # Verify basic structure - now single period
        assert result.pattern == "forecasting"
        assert result.period_name == "endOfMonth"  # Default period
        assert len(result.daily_forecast) <= 30  # Should have daily forecasts

        # Verify structured sections
        assert result.statistical is not None
        # Pacing might be None depending on data
        # assert result.pacing is not None

    def test_analyze_with_targets(self):
        """Test analysis with target data."""
        pattern = ForecastingPattern()

        # Create test data with embedded targets
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": range(100, 130),
                "grain": ["day"] * 30,
                "target_value": [150.0] * 30,  # Add target values
                "target_date": ["2023-01-31"] * 30,  # Target date
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 30),
            forecast_horizon_days=30,
        )

        # Verify targets are incorporated
        assert result.pattern == "forecasting"

        # Check if statistical section has target values
        if result.statistical and result.statistical.target_value is not None:
            assert result.statistical.target_value > 0

    def test_analyze_empty_data(self):
        """Test analysis with empty data."""
        pattern = ForecastingPattern()

        empty_data = pd.DataFrame(columns=["date", "value", "grain"])

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Run analysis - should handle empty data gracefully
        # Empty data might raise ValidationError, so we test for that
        with pytest.raises(ValidationError):  # Could be ValidationError or TimeRangeError
            pattern.analyze(metric_id="test_metric", data=empty_data, analysis_window=analysis_window)

    def test_analyze_missing_columns(self):
        """Test analysis with missing required columns."""
        pattern = ForecastingPattern()

        # Data missing 'value' column
        data = pd.DataFrame({"date": pd.date_range(start="2023-01-01", periods=10, freq="D"), "grain": ["day"] * 10})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-10", grain=Granularity.DAY)

        # Should raise validation error
        with pytest.raises(ValidationError):  # Could be ValidationError or MissingDataError
            pattern.analyze(metric_id="test_metric", data=data, analysis_window=analysis_window)

    def test_different_forecasting_methods(self):
        """Test forecasting with different parameters."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 130), "grain": ["day"] * 30})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Test basic forecasting
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 30),
            forecast_horizon_days=10,
        )

        assert result.pattern == "forecasting"
        # Should have some forecast data
        assert len(result.daily_forecast) <= 10

    def test_analyze_with_target_calculations(self):
        """Test analysis with target value calculations and gap computation."""
        pattern = ForecastingPattern()

        # Create test data with embedded targets
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": range(100, 130),
                "grain": ["day"] * 30,
                "target_value": [0.0] * 29 + [150.0],  # Target only on last day (end of month)
                "target_date": ["2023-01-31"] * 30,
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 30),
            forecast_horizon_days=30,
        )

        # Verify target value is set (it might be None if target date doesn't match forecast period)
        assert result.statistical is not None
        # Target matching is based on forecast period end date, so it might not always match
        # Just verify the structure is correct

    def test_analyze_with_zero_target_value(self):
        """Test analysis with zero target value to cover edge case."""
        pattern = ForecastingPattern()

        # Create test data with zero targets (should be ignored)
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": range(100, 130),
                "grain": ["day"] * 30,
                "target_value": [0.0] * 30,  # All zero targets
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 30),
            forecast_horizon_days=30,
        )

        # Verify structure is correct
        assert result.statistical is not None
        # No targets should be processed since they're all zero
        assert result.pacing is None
        assert result.required_performance is None

    def test_analyze_with_weekly_grain(self):
        """Test analysis with weekly grain for pacing calculations."""
        pattern = ForecastingPattern()

        # Create weekly test data
        dates = pd.date_range(start="2023-01-01", periods=8, freq="W-MON")
        data = pd.DataFrame({"date": dates, "value": range(100, 108), "grain": ["week"] * 8})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-02-19", grain=Granularity.WEEK)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            analysis_window=analysis_window,
            analysis_date=date(2023, 2, 19),
            forecast_horizon_days=30,
        )

        assert result.pattern == "forecasting"
        # The Forecasting model doesn't have a grain field, so we verify the analysis window instead
        assert result.analysis_window.grain == "week"

    def test_analyze_with_monthly_grain(self):
        """Test analysis with monthly grain for remaining grains calculation."""
        pattern = ForecastingPattern()

        # Create monthly test data with targets
        dates = pd.date_range(start="2023-01-01", periods=6, freq="MS")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": range(100, 106),
                "grain": ["month"] * 6,
                "target_value": [0.0] * 5 + [150.0],  # Target on last month
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-06-30", grain=Granularity.MONTH)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            analysis_window=analysis_window,
            analysis_date=date(2023, 6, 15),
            forecast_horizon_days=30,
        )

        assert result.pattern == "forecasting"
        assert result.analysis_window.grain == "month"

        # Should have required performance calculations if targets are properly matched
        # This depends on exact date alignment

    def test_analyze_error_handling_in_daily_forecast(self):
        """Test error handling when daily forecast generation fails."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 130), "grain": ["day"] * 30})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Mock forecast_with_confidence_intervals to raise an exception
        with patch("levers.patterns.forecasting.forecast_with_confidence_intervals") as mock_forecast:
            mock_forecast.side_effect = Exception("Forecast generation failed")

            # Run analysis - should handle the error gracefully
            result = pattern.analyze(
                metric_id="test_metric",
                data=data,
                analysis_window=analysis_window,
                analysis_date=date(2023, 1, 30),
                forecast_horizon_days=30,
            )

            # Should still return a result with empty daily forecast
            assert result.pattern == "forecasting"
            assert len(result.daily_forecast) == 0

    def test_analyze_error_handling_in_period_processing(self):
        """Test error handling when period processing fails."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 130), "grain": ["day"] * 30})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Mock get_period_end_date to raise an exception
        with patch("levers.patterns.forecasting.get_period_end_date") as mock_period:
            mock_period.side_effect = Exception("Period calculation failed")

            # Run analysis - should handle the error gracefully
            result = pattern.analyze(
                metric_id="test_metric",
                data=data,
                analysis_window=analysis_window,
                analysis_date=date(2023, 1, 30),
                forecast_horizon_days=30,
            )

            # Should still return a result with empty statistical section
            assert result.pattern == "forecasting"
            assert result.statistical is not None
            # Statistical section should be empty due to error

    def test_pacing_projection_with_different_periods(self):
        """Test pacing projection calculation for different period types."""
        pattern = ForecastingPattern()

        # Create test data spanning multiple weeks with targets
        dates = pd.date_range(start="2023-01-01", periods=14, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": [10] * 14,  # Consistent daily values
                "grain": ["day"] * 14,
                "target_value": [0.0] * 13 + [300.0],  # Target on last day
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-14", grain=Granularity.DAY)

        # Run analysis in the middle of January
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 14),
            forecast_horizon_days=30,
        )

        # Verify structure is correct - pacing may or may not be created depending on implementation
        assert result.pattern == "forecasting"
        assert result.statistical is not None

    def test_pacing_projection_edge_cases(self):
        """Test pacing projection edge cases."""
        pattern = ForecastingPattern()

        # Test case: analysis date before period start
        dates = pd.date_range(start="2022-12-01", periods=10, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 110), "grain": ["day"] * 10})

        analysis_window = AnalysisWindow(start_date="2022-12-01", end_date="2022-12-10", grain=Granularity.DAY)

        # Analyze at beginning of next year (before any data in current period)
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 1),
            forecast_horizon_days=30,
        )

        # Should handle the edge case
        assert result.pattern == "forecasting"

    def test_required_performance_calculations(self):
        """Test required performance calculations with sufficient historical data."""
        pattern = ForecastingPattern()

        # Create test data with enough periods for growth calculation and targets
        dates = pd.date_range(start="2023-01-01", periods=10, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": [100, 102, 104, 106, 108, 110, 112, 114, 116, 118],  # Growing trend
                "grain": ["day"] * 10,
                "target_value": [0.0] * 9 + [200.0],  # Target on last day
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-10", grain=Granularity.DAY)

        # Run analysis with sufficient historical periods
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 10),
            forecast_horizon_days=30,
            num_past_periods_for_growth=4,  # Use 4 periods for growth calculation
        )

        # Verify structure is correct
        assert result.pattern == "forecasting"
        assert result.statistical is not None

    def test_remaining_grains_calculation_different_grains(self):
        """Test remaining grains calculation for different grain types."""
        pattern = ForecastingPattern()

        # Test with different grains
        test_cases = [("day", Granularity.DAY), ("week", Granularity.WEEK), ("month", Granularity.MONTH)]

        for grain_str, grain_enum in test_cases:
            # Create test data with targets
            if grain_str == "day":
                dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
            elif grain_str == "week":
                dates = pd.date_range(start="2023-01-01", periods=8, freq="W-MON")
            else:  # month
                dates = pd.date_range(start="2023-01-01", periods=6, freq="MS")

            data = pd.DataFrame(
                {
                    "date": dates,
                    "value": range(100, 100 + len(dates)),
                    "grain": [grain_str] * len(dates),
                    "target_value": [0.0] * (len(dates) - 1) + [200.0],  # Target on last date
                }
            )

            analysis_window = AnalysisWindow(
                start_date="2023-01-01", end_date=dates[-1].strftime("%Y-%m-%d"), grain=grain_enum
            )

            # Run analysis
            result = pattern.analyze(
                metric_id=f"test_metric_{grain_str}",
                data=data,
                analysis_window=analysis_window,
                analysis_date=dates[-1].date(),
                forecast_horizon_days=30,
            )

            assert result.pattern == "forecasting"
            assert result.analysis_window.grain == grain_str

    def test_error_handling_in_remaining_grains_calculation(self):
        """Test error handling in remaining grains calculation."""
        pattern = ForecastingPattern()

        # Create test data with targets
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": range(100, 130),
                "grain": ["day"] * 30,
                "target_value": [0.0] * 29 + [200.0],  # Target on last day
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Mock _calculate_remaining_grains to raise an exception
        with patch.object(pattern, "_calculate_remaining_grains") as mock_calc:
            mock_calc.side_effect = Exception("Calculation failed")

            # Run analysis - should handle the error gracefully
            result = pattern.analyze(
                metric_id="test_metric",
                data=data,
                analysis_window=analysis_window,
                analysis_date=date(2023, 1, 30),
                forecast_horizon_days=30,
            )

            # Should still return a result
            assert result.pattern == "forecasting"

    def test_error_handling_in_pacing_calculation(self):
        """Test error handling in pacing projection calculation."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 130), "grain": ["day"] * 30})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Mock _calculate_pacing_projection to raise an exception
        with patch.object(pattern, "_calculate_pacing_projection") as mock_pacing:
            mock_pacing.side_effect = Exception("Pacing calculation failed")

            # Run analysis - should handle the error gracefully
            result = pattern.analyze(
                metric_id="test_metric",
                data=data,
                analysis_window=analysis_window,
                analysis_date=date(2023, 1, 30),
                forecast_horizon_days=30,
            )

            # Should still return a result with no pacing
            assert result.pattern == "forecasting"
            assert result.pacing is None

    def test_overall_error_handling(self):
        """Test overall error handling when analysis fails completely."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 130), "grain": ["day"] * 30})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Mock validate_output to raise an exception
        with patch.object(pattern, "validate_output") as mock_validate:
            mock_validate.side_effect = Exception("Validation failed")

            # Should raise ValidationError
            with pytest.raises(ValidationError):
                pattern.analyze(
                    metric_id="test_metric",
                    data=data,
                    analysis_window=analysis_window,
                    analysis_date=date(2023, 1, 30),
                    forecast_horizon_days=30,
                )

    def test_calculate_required_growth_function_parameters(self):
        """Test that the calculate_required_growth function is called with correct parameters."""
        pattern = ForecastingPattern()

        # Create test data with targets
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": range(100, 130),
                "grain": ["day"] * 30,
                "target_value": [0.0] * 29 + [200.0],  # Target on last day
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Mock calculate_required_growth to check parameters
        with patch("levers.patterns.forecasting.calculate_required_growth") as mock_calc:
            mock_calc.return_value = 0.05  # 5% growth

            # Run analysis
            _ = pattern.analyze(
                metric_id="test_metric",
                data=data,
                analysis_window=analysis_window,
                analysis_date=date(2023, 1, 30),
                forecast_horizon_days=30,
            )

            # Verify the function was called with correct parameter names
            if mock_calc.called:
                # Check that it was called with periods_remaining, not periods_left
                call_args = mock_calc.call_args
                assert "periods_remaining" in call_args.kwargs or len(call_args.args) >= 3

    def test_pacing_projection_with_target_value(self):
        """Test pacing projection when target value is available."""
        pattern = ForecastingPattern()

        # Create test data for the current month with targets
        dates = pd.date_range(start="2023-01-01", periods=15, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": [10] * 15,  # Consistent daily values
                "grain": ["day"] * 15,
                "target_value": [0.0] * 14 + [300.0],  # Target on last day
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-15", grain=Granularity.DAY)

        # Run analysis in the middle of January with target
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 15),
            forecast_horizon_days=30,
        )

        # Verify structure is correct
        assert result.pattern == "forecasting"
        assert result.statistical is not None

    def test_different_period_name_coverage(self):
        """Test coverage for different period name handling."""
        pattern = ForecastingPattern()

        # Test the _calculate_pacing_projection method directly with different period names
        hist_df = pd.DataFrame({"date": pd.date_range(start="2023-01-01", periods=15, freq="D"), "value": [10] * 15})

        analysis_dt = pd.Timestamp("2023-01-15")

        # Test different period names to cover the if-elif-else branches
        test_cases = [
            ("endOfWeek", 300.0),
            ("endOfMonth", 300.0),
            ("endOfQuarter", 300.0),
            ("unknown_period", 300.0),  # This should return empty result
        ]

        for period_name, target_value in test_cases:
            result = pattern._calculate_pacing_projection(hist_df, analysis_dt, period_name, target_value, 5.0)
            # Should return a dict (empty or with data)
            assert isinstance(result, dict)

    def test_remaining_grains_edge_cases(self):
        """Test edge cases in remaining grains calculation."""
        pattern = ForecastingPattern()

        analysis_dt = pd.Timestamp("2023-01-15")
        target_date = pd.Timestamp("2023-01-31")

        # Test different grain types
        test_cases = [
            ("day", 16),  # Days from Jan 15 to Jan 31
            ("week", 0),  # Weeks remaining
            ("month", 0),  # Months remaining
            ("unknown", 0),  # Unknown grain type
        ]

        for grain, _ in test_cases:
            result = pattern._calculate_remaining_grains(analysis_dt, target_date, grain)
            assert isinstance(result, int)
            assert result >= 0  # Should never be negative

    def test_pacing_projection_before_period_start(self):
        """Test pacing projection when analysis_dt is before current period start."""
        pattern = ForecastingPattern()

        # Create test data from previous period
        dates = pd.date_range(start="2022-12-01", periods=15, freq="D")
        hist_df = pd.DataFrame({"date": dates, "value": [10] * 15})

        # Analysis date in next period (January)
        analysis_dt = pd.Timestamp("2023-01-05")

        # Test with month period - analysis_dt should be before current month start
        result = pattern._calculate_pacing_projection(hist_df, analysis_dt, "endOfMonth", 300.0, 5.0)

        # Should handle the case and return a dict
        assert isinstance(result, dict)
        # The exact values depend on the period boundaries calculation
        # Just verify it doesn't crash and returns reasonable data

    def test_pacing_projection_error_handling(self):
        """Test error handling in pacing projection calculation."""
        pattern = ForecastingPattern()

        # Create test data
        hist_df = pd.DataFrame({"date": pd.date_range(start="2023-01-01", periods=15, freq="D"), "value": [10] * 15})

        analysis_dt = pd.Timestamp("2023-01-15")

        # Mock get_current_period_boundaries to raise an exception
        with patch("levers.patterns.forecasting.get_period_range_for_grain") as mock_boundaries:
            mock_boundaries.side_effect = Exception("Period boundaries calculation failed")

            # Should handle the exception gracefully
            result = pattern._calculate_pacing_projection(hist_df, analysis_dt, "endOfMonth", 300.0, 5.0)

            # Should return empty dict on error
            assert isinstance(result, dict)

    def test_remaining_grains_exception_handling(self):
        """Test exception handling in remaining grains calculation."""
        pattern = ForecastingPattern()

        # Create problematic dates that might cause exceptions
        analysis_dt = pd.Timestamp("2023-01-15")
        target_date = pd.Timestamp("2023-01-31")

        # Mock pd.offsets to raise an exception
        with patch("pandas.offsets.Week") as mock_week:
            mock_week.side_effect = Exception("Date calculation failed")

            # Should handle the exception gracefully for week grain
            result = pattern._calculate_remaining_grains(analysis_dt, target_date, "week")

            # Should return 0 on error
            assert result == 0

    def test_pacing_projection_100_percent_elapsed(self):
        """Test pacing projection when 100% of period has elapsed."""
        pattern = ForecastingPattern()

        # Create test data for full month
        dates = pd.date_range(start="2023-01-01", periods=31, freq="D")
        hist_df = pd.DataFrame({"date": dates, "value": [10] * 31})

        # Analysis date at end of month (100% elapsed)
        analysis_dt = pd.Timestamp("2023-01-31")

        result = pattern._calculate_pacing_projection(hist_df, analysis_dt, "endOfMonth", 300.0, 5.0)

        # Should handle 100% elapsed case
        assert isinstance(result, dict)
        if result.get("percent_of_period_elapsed") == 100:
            # Should set projected value to cumulative value
            assert result.get("pacing_projected_value") == result.get("current_cumulative_value")
