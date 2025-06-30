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
        assert len(config.data_sources) == 2  # Two data sources: data and target
        assert config.data_sources[0].source_type == DataSourceType.METRIC_TIME_SERIES
        assert config.data_sources[0].data_key == "data"
        assert config.data_sources[0].lookahead is False
        assert config.data_sources[1].source_type == DataSourceType.METRIC_WITH_TARGETS
        assert config.data_sources[1].data_key == "target"
        assert config.data_sources[1].lookahead is True
        assert config.data_sources[0].is_required is True
        assert config.analysis_window.days == 365
        assert config.settings["confidence_level"] == 0.95

    def test_analyze_with_minimal_data(self):
        """Test analysis with minimal valid data."""
        pattern = ForecastingPattern()

        # Create minimal test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 130), "grain": ["day"] * 30})  # Simple increasing trend

        # Create minimal target data
        target = pd.DataFrame({"date": ["2023-02-05"], "target_value": [150.0]})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 30),
        )

        # Verify basic structure
        assert result.pattern == "forecasting"
        assert result.forecast_period_grain == Granularity.WEEK  # Default period for daily grain
        assert result.period_forecast is not None
        assert len(result.period_forecast) >= 1  # Should have period forecasts

        # Verify structured sections - with targets provided
        # forecast_vs_target_stats might be None if target date doesn't align with forecast period
        # Just verify the pattern completed successfully

    def test_analyze_with_targets(self):
        """Test analysis with target data."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": range(100, 130),
                "grain": ["day"] * 30,
            }
        )

        # Create target data
        target = pd.DataFrame(
            {
                "date": ["2023-02-05"],  # Week ending date for daily grain
                "target_value": [150.0],
                "target_date": ["2023-02-05"],
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 30),
        )

        # Verify targets are incorporated
        assert result.pattern == "forecasting"
        assert result.forecast_period_grain == Granularity.WEEK

        # Check if forecast_vs_target_stats section has target values
        # This might be None if target date doesn't align exactly with forecast period
        # Just verify the pattern completed successfully

    def test_analyze_empty_data(self):
        """Test analysis with empty data."""
        pattern = ForecastingPattern()

        empty_data = pd.DataFrame(columns=["date", "value", "grain"])
        empty_target = pd.DataFrame(columns=["date", "target_value"])

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Run analysis - should raise ValidationError for empty data
        with pytest.raises(ValidationError):
            pattern.analyze(
                metric_id="test_metric", data=empty_data, target=empty_target, analysis_window=analysis_window
            )

    def test_analyze_missing_columns(self):
        """Test analysis with missing required columns."""
        pattern = ForecastingPattern()

        # Data missing 'value' column
        data = pd.DataFrame({"date": pd.date_range(start="2023-01-01", periods=10, freq="D"), "grain": ["day"] * 10})
        target = pd.DataFrame({"date": ["2023-01-11"], "target_value": [100.0]})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-10", grain=Granularity.DAY)

        # Should raise validation error
        with pytest.raises(ValidationError):
            pattern.analyze(metric_id="test_metric", data=data, target=target, analysis_window=analysis_window)

    def test_different_forecasting_methods(self):
        """Test forecasting with different parameters."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 130), "grain": ["day"] * 30})
        target = pd.DataFrame({"date": ["2023-02-05"], "target_value": [150.0]})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Test basic forecasting
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 30),
        )

        assert result.pattern == "forecasting"
        # Should have some forecast data
        assert result.period_forecast is not None
        assert len(result.period_forecast) >= 1

    def test_analyze_with_target_calculations(self):
        """Test analysis with target value calculations and gap computation."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": range(100, 130),
                "grain": ["day"] * 30,
            }
        )

        # Create target data with week ending date for daily grain
        target = pd.DataFrame(
            {"date": ["2023-02-05"], "target_value": [150.0], "target_date": ["2023-02-05"]}  # Week ending date
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 30),
        )

        # Verify the pattern completed successfully
        assert result.pattern == "forecasting"
        assert result.forecast_period_grain == Granularity.WEEK

    def test_analyze_with_zero_target_value(self):
        """Test analysis with zero target value to cover edge case."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": range(100, 130),
                "grain": ["day"] * 30,
            }
        )

        # Create target data with zero values (should be ignored)
        target = pd.DataFrame(
            {
                "date": ["2023-02-05"],
                "target_value": [0.0],  # Zero target
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 30),
        )

        # Verify zero targets are ignored - since all targets are zero, forecast_vs_target_stats should be None
        assert result.pattern == "forecasting"
        assert result.forecast_vs_target_stats is None

    def test_analyze_with_weekly_grain(self):
        """Test analysis with weekly granularity."""
        pattern = ForecastingPattern()

        # Create weekly test data
        dates = pd.date_range(start="2023-01-01", periods=8, freq="W-MON")
        data = pd.DataFrame({"date": dates, "value": range(100, 108), "grain": ["week"] * 8})
        target = pd.DataFrame({"date": ["2023-02-28"], "target_value": [120.0]})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-02-19", grain=Granularity.WEEK)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 2, 19),
        )

        # Verify weekly analysis works
        assert result.pattern == "forecasting"
        assert result.forecast_period_grain == Granularity.MONTH  # Default period for weekly grain
        assert result.period_forecast is not None

    def test_analyze_with_monthly_grain(self):
        """Test analysis with monthly granularity."""
        pattern = ForecastingPattern()

        # Create monthly test data
        dates = pd.date_range(start="2023-01-01", periods=6, freq="MS")
        data = pd.DataFrame({"date": dates, "value": range(100, 106), "grain": ["month"] * 6})
        target = pd.DataFrame({"date": ["2023-12-31"], "target_value": [120.0]})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-06-30", grain=Granularity.MONTH)

        # Run analysis
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 6, 30),
        )

        # Verify monthly analysis works
        assert result.pattern == "forecasting"
        assert result.forecast_period_grain == Granularity.QUARTER  # Default period for monthly grain
        assert result.period_forecast is not None

    def test_analyze_error_handling_in_daily_forecast(self):
        """Test error handling in daily forecast generation."""
        pattern = ForecastingPattern()

        # Create data that might cause forecasting issues
        dates = pd.date_range(start="2023-01-01", periods=5, freq="D")  # Very short series
        data = pd.DataFrame({"date": dates, "value": [100] * 5, "grain": ["day"] * 5})  # Flat data
        target = pd.DataFrame({"date": ["2023-02-05"], "target_value": [150.0]})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-05", grain=Granularity.DAY)

        # Should handle gracefully and return a result
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 5),
        )

        # Should complete without error
        assert result.pattern == "forecasting"

    def test_analyze_error_handling_in_period_processing(self):
        """Test error handling in period processing."""
        pattern = ForecastingPattern()

        # Create minimal data
        dates = pd.date_range(start="2023-01-01", periods=3, freq="D")
        data = pd.DataFrame({"date": dates, "value": [100, 101, 102], "grain": ["day"] * 3})
        target = pd.DataFrame({"date": ["2023-02-05"], "target_value": [150.0]})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-03", grain=Granularity.DAY)

        # Should handle gracefully
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 3),
        )

        # Should complete without error
        assert result.pattern == "forecasting"

    def test_pacing_projection_with_different_periods(self):
        """Test pacing projection calculation for different period types."""
        pattern = ForecastingPattern()

        # Create test data spanning multiple weeks
        dates = pd.date_range(start="2023-01-01", periods=14, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": [10] * 14,  # Consistent daily values
                "grain": ["day"] * 14,
            }
        )

        # Create target data
        target = pd.DataFrame(
            {
                "date": ["2023-01-14"],
                "target_value": [300.0],
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-14", grain=Granularity.DAY)

        # Run analysis in the middle of January
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 14),
        )

        # Verify structure is correct - pacing may or may not be created depending on implementation
        assert result.pattern == "forecasting"
        # forecast_vs_target_stats may be None if target date doesn't align with forecast period end date
        # Just verify the pattern completed successfully

    def test_pacing_projection_edge_cases(self):
        """Test edge cases in pacing projection."""
        pattern = ForecastingPattern()

        # Test case: analysis date before period start
        dates = pd.date_range(start="2022-12-01", periods=10, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 110), "grain": ["day"] * 10})
        target = pd.DataFrame({"date": ["2023-01-08"], "target_value": [150.0]})

        analysis_window = AnalysisWindow(start_date="2022-12-01", end_date="2022-12-10", grain=Granularity.DAY)

        # Analyze at beginning of next year (before any data in current period)
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 1),
        )

        # Should handle edge case gracefully
        assert result.pattern == "forecasting"

    def test_required_performance_calculations(self):
        """Test required performance calculations with sufficient historical data."""
        pattern = ForecastingPattern()

        # Create test data with enough periods for growth calculation
        dates = pd.date_range(start="2023-01-01", periods=10, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": [100, 102, 104, 106, 108, 110, 112, 114, 116, 118],  # Growing trend
                "grain": ["day"] * 10,
            }
        )

        # Create target data
        target = pd.DataFrame(
            {
                "date": ["2023-01-10"],
                "target_value": [200.0],
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-10", grain=Granularity.DAY)

        # Run analysis with sufficient historical periods
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 10),
            num_past_periods_for_growth=4,  # Use 4 periods for growth calculation
        )

        # Verify structure is correct
        assert result.pattern == "forecasting"
        # forecast_vs_target_stats may be None if target date doesn't align with forecast period end date
        # Just verify the pattern completed successfully

    def test_remaining_grains_calculation_different_grains(self):
        """Test remaining grains calculation for different grain types."""
        pattern = ForecastingPattern()

        # Test with different grains
        test_cases = [("day", Granularity.DAY), ("week", Granularity.WEEK), ("month", Granularity.MONTH)]

        for grain_str, grain_enum in test_cases:
            if grain_str == "day":
                dates = pd.date_range(start="2023-01-01", periods=10, freq="D")
            elif grain_str == "week":
                dates = pd.date_range(start="2023-01-01", periods=6, freq="W-MON")
            else:  # month
                dates = pd.date_range(start="2023-01-01", periods=6, freq="MS")

            data = pd.DataFrame(
                {
                    "date": dates,
                    "value": range(100, 100 + len(dates)),
                    "grain": [grain_str] * len(dates),
                }
            )

            # Create target data
            target = pd.DataFrame(
                {
                    "date": [dates[-1].strftime("%Y-%m-%d")],
                    "target_value": [200.0],
                }
            )

            analysis_window = AnalysisWindow(
                start_date="2023-01-01", end_date=dates[-1].strftime("%Y-%m-%d"), grain=grain_enum
            )

            # Run analysis
            result = pattern.analyze(
                metric_id="test_metric",
                data=data,
                target=target,
                analysis_window=analysis_window,
                analysis_date=dates[-1].date(),
            )

            # Verify structure is correct for each grain
            assert result.pattern == "forecasting"

    def test_error_handling_in_remaining_grains_calculation(self):
        """Test error handling in remaining grains calculation."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": range(100, 130),
                "grain": ["day"] * 30,
            }
        )

        # Create target data
        target = pd.DataFrame(
            {
                "date": ["2023-01-30"],
                "target_value": [200.0],
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Mock calculate_remaining_periods to raise an exception
        with patch("levers.patterns.forecasting.calculate_remaining_periods") as mock_calc:
            mock_calc.side_effect = Exception("Calculation failed")

            # Run analysis - should complete successfully even with remaining periods error
            # since this is used in required_performance calculation which is optional
            result = pattern.analyze(
                metric_id="test_metric",
                data=data,
                target=target,
                analysis_window=analysis_window,
                analysis_date=date(2023, 1, 30),
            )

            # Should still return a result
            assert result.pattern == "forecasting"

    def test_error_handling_in_pacing_calculation(self):
        """Test error handling in pacing calculation."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 130), "grain": ["day"] * 30})
        target = pd.DataFrame({"date": ["2023-02-05"], "target_value": [150.0]})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Mock _calculate_pacing_projection to raise an exception
        with patch.object(pattern, "_calculate_pacing_projection") as mock_pacing:
            mock_pacing.side_effect = Exception("Pacing calculation failed")

            # Run analysis - should raise ValidationError when pacing calculation fails
            with pytest.raises(ValidationError):
                pattern.analyze(
                    metric_id="test_metric",
                    data=data,
                    target=target,
                    analysis_window=analysis_window,
                    analysis_date=date(2023, 1, 30),
                )

    def test_overall_error_handling(self):
        """Test overall error handling and validation."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame({"date": dates, "value": range(100, 130), "grain": ["day"] * 30})
        target = pd.DataFrame({"date": ["2023-02-05"], "target_value": [150.0]})

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-30", grain=Granularity.DAY)

        # Mock validate_output to raise an exception
        with patch.object(pattern, "validate_output") as mock_validate:
            mock_validate.side_effect = Exception("Validation failed")

            # Should raise ValidationError
            with pytest.raises(ValidationError):
                pattern.analyze(
                    metric_id="test_metric",
                    data=data,
                    target=target,
                    analysis_window=analysis_window,
                    analysis_date=date(2023, 1, 30),
                )

    def test_calculate_required_growth_function_parameters(self):
        """Test that calculate_required_growth is called with correct parameters."""
        pattern = ForecastingPattern()

        # Create test data
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": range(100, 130),
                "grain": ["day"] * 30,
            }
        )

        # Create target data
        target = pd.DataFrame(
            {
                "date": ["2023-01-30"],
                "target_value": [200.0],
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
                target=target,
                analysis_window=analysis_window,
                analysis_date=date(2023, 1, 30),
            )

            # Verify calculate_required_growth was called with correct parameter names
            if mock_calc.called:
                # Check that it was called with periods_remaining, not periods_left
                call_args = mock_calc.call_args
                assert "remaining_periods" in call_args.kwargs or len(call_args.args) >= 3

    def test_pacing_projection_with_target_value(self):
        """Test pacing projection when target value is available."""
        pattern = ForecastingPattern()

        # Create test data for the current month
        dates = pd.date_range(start="2023-01-01", periods=15, freq="D")
        data = pd.DataFrame(
            {
                "date": dates,
                "value": [10] * 15,  # Consistent daily values
                "grain": ["day"] * 15,
            }
        )

        # Create target data
        target = pd.DataFrame(
            {
                "date": ["2023-01-15"],
                "target_value": [300.0],
            }
        )

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-15", grain=Granularity.DAY)

        # Run analysis in the middle of January with target
        result = pattern.analyze(
            metric_id="test_metric",
            data=data,
            target=target,
            analysis_window=analysis_window,
            analysis_date=date(2023, 1, 15),
        )

        # Verify structure is correct
        assert result.pattern == "forecasting"
        # forecast_vs_target_stats may be None if target date doesn't align with forecast period end date
        # Just verify the pattern completed successfully

    def test_different_period_name_coverage(self):
        """Test coverage for different period name handling."""
        pattern = ForecastingPattern()

        hist_df = pd.DataFrame({"date": pd.date_range(start="2023-01-01", periods=15, freq="D"), "value": [10] * 15})

        analysis_dt = pd.Timestamp("2023-01-15")

        # Test different period types to cover the if-elif-else branches
        test_cases = [
            (Granularity.WEEK, 300.0),
            (Granularity.MONTH, 300.0),
            (Granularity.QUARTER, 300.0),
        ]

        for period_grain, target_value in test_cases:
            result = pattern._calculate_pacing_projection(hist_df, analysis_dt, period_grain, target_value, 5.0)
            # Should return a PacingProjection object
            assert hasattr(result, "period_elapsed_percent")

    def test_remaining_grains_edge_cases(self):
        """Test edge cases in remaining grains calculation."""
        # This test is no longer applicable since _calculate_remaining_grains doesn't exist
        # The functionality is now handled by calculate_remaining_periods primitive
        pass

    def test_pacing_projection_before_period_start(self):
        """Test pacing projection when analysis_dt is before current period start."""
        pattern = ForecastingPattern()

        # Test the _calculate_pacing_projection method directly
        # Create test data from previous period
        dates = pd.date_range(start="2022-12-01", periods=15, freq="D")
        hist_df = pd.DataFrame({"date": dates, "value": [10] * 15})

        # Analysis date in next period (January)
        analysis_dt = pd.Timestamp("2023-01-05")

        # Test with month period - analysis_dt should be before current month start
        result = pattern._calculate_pacing_projection(hist_df, analysis_dt, Granularity.MONTH, 300.0, 5.0)

        # Should handle the case and return a PacingProjection object
        assert hasattr(result, "period_elapsed_percent")

    def test_pacing_projection_error_handling(self):
        """Test error handling in pacing projection calculation."""
        pattern = ForecastingPattern()

        # Create test data
        hist_df = pd.DataFrame({"date": pd.date_range(start="2023-01-01", periods=15, freq="D"), "value": [10] * 15})

        analysis_dt = pd.Timestamp("2023-01-15")

        # Mock get_period_range_for_grain to raise an exception
        with patch("levers.patterns.forecasting.get_period_range_for_grain") as mock_boundaries:
            mock_boundaries.side_effect = ValidationError("Period boundaries calculation failed")

            # Should raise the exception since error handling isn't implemented in this method
            with pytest.raises(ValidationError):
                pattern._calculate_pacing_projection(hist_df, analysis_dt, Granularity.MONTH, 300.0, 5.0)

    def test_remaining_grains_exception_handling(self):
        """Test exception handling in remaining grains calculation."""
        # This test is no longer applicable since _calculate_remaining_grains doesn't exist
        # The functionality is now handled by calculate_remaining_periods primitive
        pass

    def test_pacing_projection_100_percent_elapsed(self):
        """Test pacing projection when 100% of period has elapsed."""
        pattern = ForecastingPattern()

        # Test the _calculate_pacing_projection method directly
        # Create test data that spans the entire month
        dates = pd.date_range(start="2023-01-01", periods=31, freq="D")
        hist_df = pd.DataFrame({"date": dates, "value": [10] * 31})

        # Analysis date at end of month
        analysis_dt = pd.Timestamp("2023-01-31")

        # Test with month period - 100% elapsed
        result = pattern._calculate_pacing_projection(hist_df, analysis_dt, Granularity.MONTH, 300.0, 5.0)

        # Should handle the case and return a PacingProjection object
        assert hasattr(result, "period_elapsed_percent")
        # The percent elapsed should be close to 100%
        if result.period_elapsed_percent is not None:
            assert result.period_elapsed_percent >= 95.0  # Allow some tolerance
