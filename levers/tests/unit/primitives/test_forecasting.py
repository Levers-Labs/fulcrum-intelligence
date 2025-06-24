"""
Tests for forecasting primitives.
"""

import numpy as np
import pandas as pd
import pytest

from levers.exceptions import PrimitiveError, ValidationError
from levers.primitives import (
    calculate_forecast_accuracy,
    forecast_with_confidence_intervals,
    generate_forecast_scenarios,
    simple_forecast,
)


class TestSimpleForecast:
    """Test simple_forecast primitive."""

    def create_sample_data(self, periods=30):
        """Create sample time series data."""
        dates = pd.date_range(start="2023-01-01", periods=periods, freq="D")
        values = np.random.normal(100, 10, periods) + np.arange(periods) * 0.5  # Trending upward
        return pd.DataFrame({"date": dates, "value": values})

    def test_simple_forecast_naive(self):
        """Test simple forecast with naive method."""
        df = self.create_sample_data()

        result = simple_forecast(df=df, value_col="value", periods=10, method="naive", date_col="date", grain="day")

        assert len(result) == 10
        assert "date" in result.columns
        assert "forecast" in result.columns

        # Naive method should forecast the last value
        last_value = df["value"].iloc[-1]
        assert all(result["forecast"] == last_value)

    def test_simple_forecast_ses(self):
        """Test simple forecast with SES method."""
        df = self.create_sample_data()

        result = simple_forecast(df=df, value_col="value", periods=10, method="ses", date_col="date", grain="day")

        assert len(result) == 10
        assert "date" in result.columns
        assert "forecast" in result.columns
        assert all(pd.notna(result["forecast"]))

    def test_simple_forecast_holtwinters(self):
        """Test simple forecast with Holt-Winters method."""
        df = self.create_sample_data(periods=60)  # Need more data for Holt-Winters

        result = simple_forecast(
            df=df, value_col="value", periods=10, method="holtwinters", date_col="date", grain="day"
        )

        assert len(result) == 10
        assert "date" in result.columns
        assert "forecast" in result.columns

    def test_simple_forecast_auto_arima(self):
        """Test simple forecast with auto ARIMA method."""
        df = self.create_sample_data()

        result = simple_forecast(
            df=df, value_col="value", periods=10, method="auto_arima", date_col="date", grain="day"
        )

        assert len(result) == 10
        assert "date" in result.columns
        assert "forecast" in result.columns

    def test_simple_forecast_invalid_method(self):
        """Test simple forecast with invalid method."""
        df = self.create_sample_data()

        with pytest.raises(PrimitiveError):  # Should raise PrimitiveError
            simple_forecast(df=df, value_col="value", periods=10, method="invalid_method", date_col="date", grain="day")

    def test_simple_forecast_missing_column(self):
        """Test simple forecast with missing value column."""
        df = pd.DataFrame({"date": pd.date_range("2023-01-01", periods=10), "wrong_col": range(10)})

        with pytest.raises(ValidationError):
            simple_forecast(df=df, value_col="value", periods=10, method="naive", date_col="date", grain="day")

    def test_simple_forecast_weekly_grain(self):
        """Test simple forecast with weekly grain."""
        dates = pd.date_range(start="2023-01-01", periods=20, freq="W")
        df = pd.DataFrame({"date": dates, "value": range(100, 120)})

        result = simple_forecast(df=df, value_col="value", periods=5, method="naive", date_col="date", grain="week")

        assert len(result) == 5
        # Check that dates are weekly
        date_diffs = pd.to_datetime(result["date"]).diff().dt.days[1:]
        assert all(date_diffs == 7)


class TestForecastWithConfidenceIntervals:
    """Test forecast_with_confidence_intervals primitive."""

    def create_sample_data(self, periods=30):
        """Create sample time series data."""
        dates = pd.date_range(start="2023-01-01", periods=periods, freq="D")
        values = np.random.normal(100, 10, periods) + np.arange(periods) * 0.5
        return pd.DataFrame({"date": dates, "value": values})

    def test_forecast_with_confidence_intervals_basic(self):
        """Test basic confidence interval forecasting."""
        df = self.create_sample_data()

        result = forecast_with_confidence_intervals(
            df=df, value_col="value", periods=10, confidence_level=0.95, date_col="date", grain="day"
        )

        assert len(result) == 10
        required_cols = ["date", "forecast", "lower_bound", "upper_bound"]
        for col in required_cols:
            assert col in result.columns

        # Lower bound should be less than forecast, forecast less than upper bound
        assert all(result["lower_bound"] <= result["forecast"])
        assert all(result["forecast"] <= result["upper_bound"])

    def test_forecast_with_confidence_intervals_different_levels(self):
        """Test confidence intervals with different confidence levels."""
        df = self.create_sample_data()

        result_95 = forecast_with_confidence_intervals(
            df=df, value_col="value", periods=10, confidence_level=0.95, date_col="date", grain="day"
        )

        result_90 = forecast_with_confidence_intervals(
            df=df, value_col="value", periods=10, confidence_level=0.90, date_col="date", grain="day"
        )

        # 90% intervals should be narrower than 95% intervals
        width_95 = result_95["upper_bound"] - result_95["lower_bound"]
        width_90 = result_90["upper_bound"] - result_90["lower_bound"]

        assert all(width_90 <= width_95)


class TestCalculateForecastAccuracy:
    """Test calculate_forecast_accuracy primitive."""

    def create_forecast_data(self):
        """Create sample actual and forecast data."""
        dates = pd.date_range(start="2023-01-01", periods=10, freq="D")
        actual = pd.DataFrame({"date": dates, "actual": [100, 102, 98, 105, 101, 99, 103, 107, 104, 106]})
        forecast = pd.DataFrame({"date": dates, "forecast": [99, 101, 100, 104, 102, 98, 102, 105, 103, 105]})
        return actual, forecast

    def test_calculate_forecast_accuracy_basic(self):
        """Test basic forecast accuracy calculation."""
        actual_df, forecast_df = self.create_forecast_data()

        result = calculate_forecast_accuracy(
            actual_df=actual_df, forecast_df=forecast_df, date_col="date", actual_col="actual", forecast_col="forecast"
        )

        # Check that all metrics are present
        expected_metrics = ["rmse", "mae", "mape", "bias"]
        for metric in expected_metrics:
            assert metric in result
            assert isinstance(result[metric], (int, float))
            assert not np.isnan(result[metric])

    def test_calculate_forecast_accuracy_perfect_forecast(self):
        """Test accuracy with perfect forecast."""
        dates = pd.date_range(start="2023-01-01", periods=5, freq="D")
        perfect_values = [100, 102, 104, 106, 108]

        actual_df = pd.DataFrame({"date": dates, "actual": perfect_values})
        forecast_df = pd.DataFrame({"date": dates, "forecast": perfect_values})

        result = calculate_forecast_accuracy(
            actual_df=actual_df, forecast_df=forecast_df, date_col="date", actual_col="actual", forecast_col="forecast"
        )

        # Perfect forecast should have zero error
        assert result["rmse"] == 0.0
        assert result["mae"] == 0.0
        assert result["mape"] == 0.0
        assert result["bias"] == 0.0

    def test_calculate_forecast_accuracy_missing_column(self):
        """Test accuracy calculation with missing column."""
        actual_df = pd.DataFrame({"date": pd.date_range("2023-01-01", periods=5), "actual": range(5)})
        forecast_df = pd.DataFrame({"date": pd.date_range("2023-01-01", periods=5), "wrong_col": range(5)})

        with pytest.raises(ValidationError):
            calculate_forecast_accuracy(
                actual_df=actual_df,
                forecast_df=forecast_df,
                date_col="date",
                actual_col="actual",
                forecast_col="forecast",
            )


class TestGenerateForecastScenarios:
    """Test generate_forecast_scenarios primitive."""

    def create_forecast_data(self):
        """Create sample forecast data."""
        dates = pd.date_range(start="2023-01-01", periods=10, freq="D")
        return pd.DataFrame({"date": dates, "forecast": [100, 102, 104, 106, 108, 110, 112, 114, 116, 118]})

    def test_generate_forecast_scenarios_basic(self):
        """Test basic scenario generation."""
        df = self.create_forecast_data()

        result = generate_forecast_scenarios(forecast_df=df, forecast_col="forecast", buffer_pct=10.0)

        assert len(result) == len(df)
        assert "best_case" in result.columns
        assert "worst_case" in result.columns

        # Best case should be higher than forecast, worst case should be lower
        assert all(result["best_case"] >= result["forecast"])
        assert all(result["worst_case"] <= result["forecast"])

    def test_generate_forecast_scenarios_multipliers(self):
        """Test scenario generation with different buffer percentages."""
        df = self.create_forecast_data()

        result = generate_forecast_scenarios(forecast_df=df, forecast_col="forecast", buffer_pct=20.0)

        # With 20% buffer, best case should be 20% higher, worst case 20% lower
        expected_best = df["forecast"] * 1.2
        expected_worst = df["forecast"] * 0.8

        pd.testing.assert_series_equal(result["best_case"], expected_best, check_names=False)
        pd.testing.assert_series_equal(result["worst_case"], expected_worst, check_names=False)

    def test_generate_forecast_scenarios_missing_column(self):
        """Test scenario generation with missing forecast column."""
        df = pd.DataFrame({"date": pd.date_range("2023-01-01", periods=5), "wrong_col": range(5)})

        with pytest.raises(ValidationError):
            generate_forecast_scenarios(forecast_df=df, forecast_col="forecast", buffer_pct=10.0)


class TestForecastingIntegration:
    """Integration tests for forecasting primitives."""

    def test_forecasting_pipeline(self):
        """Test a complete forecasting pipeline."""
        # Create sample data
        dates = pd.date_range(start="2023-01-01", periods=50, freq="D")
        values = 100 + np.cumsum(np.random.normal(0.5, 2, 50))  # Trending with noise
        df = pd.DataFrame({"date": dates, "value": values})

        # Generate forecast
        forecast_df = simple_forecast(df=df, value_col="value", periods=10, method="ses", date_col="date", grain="day")

        # Add confidence intervals
        forecast_with_ci = forecast_with_confidence_intervals(
            df=df, value_col="value", periods=10, confidence_level=0.95, date_col="date", grain="day"
        )

        # Generate scenarios
        scenarios = generate_forecast_scenarios(forecast_df=forecast_with_ci, forecast_col="forecast", buffer_pct=10.0)

        # Verify the pipeline worked
        assert len(forecast_df) == 10
        assert len(forecast_with_ci) == 10
        assert len(scenarios) == 10

        # Verify all required columns are present
        assert "forecast" in forecast_df.columns
        assert all(col in forecast_with_ci.columns for col in ["forecast", "lower_bound", "upper_bound"])
        assert all(col in scenarios.columns for col in ["forecast", "best_case", "worst_case"])

    def test_forecasting_with_gaps(self):
        """Test forecasting with gaps in data."""
        # Create data with gaps
        dates1 = pd.date_range(start="2023-01-01", periods=10, freq="D")
        dates2 = pd.date_range(start="2023-01-15", periods=10, freq="D")  # 4-day gap
        dates = list(dates1) + list(dates2)
        values = list(range(100, 110)) + list(range(110, 120))

        df = pd.DataFrame({"date": dates, "value": values})

        # Should still be able to forecast
        result = simple_forecast(df=df, value_col="value", periods=5, method="naive", date_col="date", grain="day")

        assert len(result) == 5
        assert "forecast" in result.columns
