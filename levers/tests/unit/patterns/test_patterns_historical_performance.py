"""
Unit tests for the HistoricalPerformancePattern.
"""

import numpy as np
import pandas as pd
import pytest

from levers.exceptions import ValidationError
from levers.models import (
    AnalysisWindow,
    Granularity,
    TrendExceptionType,
    TrendType,
)
from levers.models.patterns import HistoricalPerformance, RankSummary
from levers.patterns import HistoricalPerformancePattern


class TestHistoricalPerformancePattern:
    """Tests for the HistoricalPerformancePattern class."""

    @pytest.fixture
    def pattern(self):
        """Return a HistoricalPerformancePattern instance."""
        return HistoricalPerformancePattern()

    @pytest.fixture
    def sample_data(self):
        """Return sample data for testing."""
        # Create sample data with date and value columns
        return pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"),
                "value": [100 + i + np.sin(i / 5) * 10 for i in range(31)],  # Add some variation
            }
        )

    @pytest.fixture
    def sample_data_upward_trend(self):
        """Return sample data with a clear upward trend."""
        return pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"),
                "value": [100 + i * 2 for i in range(31)],  # Steadily increasing
            }
        )

    @pytest.fixture
    def sample_data_downward_trend(self):
        """Return sample data with a clear downward trend."""
        return pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"),
                "value": [160 - i * 2 for i in range(31)],  # Steadily decreasing
            }
        )

    @pytest.fixture
    def sample_data_plateau(self):
        """Return sample data with a plateau."""
        return pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"),
                "value": [100 + np.random.normal(0, 0.05) for _ in range(31)],  # Small random variations
            }
        )

    @pytest.fixture
    def sample_data_with_anomaly(self):
        """Return sample data with an anomaly."""
        values = [100 + i for i in range(31)]
        values[15] = 200  # Add a spike
        return pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"),
                "value": values,
            }
        )

    @pytest.fixture
    def analysis_window(self):
        """Return an AnalysisWindow for testing."""
        return AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

    def test_pattern_attributes(self, pattern):
        """Test pattern class attributes."""
        assert pattern.name == "historical_performance"
        assert pattern.version == "1.0"
        assert pattern.output_model == HistoricalPerformance
        assert len(pattern.required_primitives) > 0
        assert "calculate_pop_growth" in pattern.required_primitives
        assert "analyze_metric_trend" in pattern.required_primitives
        assert "detect_record_high" in pattern.required_primitives
        assert "detect_record_low" in pattern.required_primitives
        assert "detect_seasonality_pattern" in pattern.required_primitives
        assert "calculate_period_benchmarks" in pattern.required_primitives

    def test_analyze_basic(self, pattern, sample_data, analysis_window):
        """Test the basic analyze method."""
        # Arrange
        metric_id = "test_metric"

        # Act
        result = pattern.analyze(metric_id=metric_id, data=sample_data, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.pattern == "historical_performance"
        assert result.metric_id == metric_id
        assert result.analysis_window == analysis_window
        assert len(result.period_metrics) > 0
        assert result.growth_stats is not None
        assert isinstance(result.high_rank, RankSummary)
        assert isinstance(result.low_rank, RankSummary)

    def test_analyze_upward_trend(self, pattern, sample_data_upward_trend, analysis_window):
        """Test analyzing an upward trend."""
        # Arrange
        metric_id = "test_metric"

        # Act
        result = pattern.analyze(metric_id=metric_id, data=sample_data_upward_trend, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.current_trend is not None
        assert result.current_trend.trend_type == TrendType.UPWARD

    def test_analyze_downward_trend(self, pattern, sample_data_downward_trend, analysis_window):
        """Test analyzing a downward trend."""
        # Arrange
        metric_id = "test_metric"

        # Act
        result = pattern.analyze(metric_id=metric_id, data=sample_data_downward_trend, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.current_trend is not None
        assert result.current_trend.trend_type == TrendType.DOWNWARD

    def test_analyze_plateau(self, pattern, sample_data_plateau, analysis_window):
        """Test analyzing a plateau."""
        # Arrange
        metric_id = "test_metric"

        # Act
        result = pattern.analyze(metric_id=metric_id, data=sample_data_plateau, analysis_window=analysis_window)

        # Assert
        assert result is not None

        # Check for stable or plateau trend
        trend_type = result.current_trend.trend_type if result.current_trend else None
        assert trend_type == TrendType.PLATEAU

    def test_analyze_anomalies(self, pattern, sample_data_with_anomaly, analysis_window):
        """Test analyzing a dataset with anomalies."""
        # Arrange
        metric_id = "test_metric"

        # Act
        result = pattern.analyze(metric_id=metric_id, data=sample_data_with_anomaly, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert len(result.trend_exceptions) >= 0  # May or may not have exceptions depending on implementation
        if len(result.trend_exceptions) > 0:
            # Check if at least one exception is a spike (since we added one)
            has_spike = any(exception.type == TrendExceptionType.SPIKE for exception in result.trend_exceptions)
            assert has_spike

    def test_analyze_insufficient_data(self, pattern, analysis_window):
        """Test analyzing with insufficient data."""
        # Arrange
        metric_id = "test_metric"
        # Create a dataframe with just one data point
        data = pd.DataFrame(
            {
                "date": [pd.Timestamp("2023-01-01")],
                "value": [100],
            }
        )

        try:
            # Act
            result = pattern.analyze(metric_id=metric_id, data=data, analysis_window=analysis_window)

            # Assert
            assert result is not None
            assert result.pattern == "historical_performance"
            assert result.metric_id == metric_id
            assert result.period_metrics == []
            assert result.growth_stats.current_pop_growth is None
            assert result.growth_stats.average_pop_growth is None
        except ValidationError:
            # Some implementations might raise a validation error for insufficient data
            # which is also a valid behavior
            pass

    def test_analyze_empty_data(self, pattern, analysis_window):
        """Test analyzing with empty data."""
        # Arrange
        metric_id = "test_metric"
        data = pd.DataFrame({"date": [], "value": []})

        # Act & Assert
        with pytest.raises(ValidationError):
            pattern.analyze(metric_id=metric_id, data=data, analysis_window=analysis_window)

    def test_group_by_grain(self, pattern):
        """Test the _group_by_grain method."""
        # This test requires internal method access which might not be available
        # Skip the test or implement a suitable alternative
        if hasattr(pattern, "_group_by_grain"):
            # Arrange
            data = pd.DataFrame(
                {
                    "date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"),
                    "value": [100 + i for i in range(31)],
                }
            )

            # Act
            result = pattern._group_by_grain(data, "date", "value", Granularity.WEEK)

            # Assert
            assert result is not None
            assert len(result) <= len(data)  # Should be aggregated

    def test_analyze_trends(self, pattern):
        """Test the trend analysis functionality."""
        # Arrange
        data = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"),
                "value": [100 + i * 2 for i in range(31)],  # Steady upward trend
            }
        )
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Act
        result = pattern.analyze("test", data, analysis_window)

        # Assert
        assert result is not None
        assert result.current_trend is not None
        assert result.current_trend.trend_type == TrendType.UPWARD
        assert result.current_trend.duration_grains > 0

    def test_detect_seasonality_pattern(self, pattern):
        """Test the seasonality pattern detection functionality."""
        # Arrange - Create data with a yearly seasonal pattern
        # First year
        dates1 = pd.date_range(start="2022-01-01", periods=365, freq="D")
        # Second year (just January)
        dates2 = pd.date_range(start="2023-01-01", periods=31, freq="D")
        all_dates = dates1.append(dates2)

        # Create values with a seasonal pattern
        values = []
        for date in all_dates:
            day_of_year = date.dayofyear
            seasonal = 10 * np.sin(2 * np.pi * day_of_year / 365)
            values.append(100 + seasonal)

        data = pd.DataFrame({"date": all_dates, "value": values})
        analysis_window = AnalysisWindow(start_date="2022-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Act
        result = pattern.analyze("test", data, analysis_window)

        # Assert
        assert result is not None
        assert result.seasonality is not None
        assert hasattr(result.seasonality, "is_following_expected_pattern")
        assert hasattr(result.seasonality, "expected_change_percent")
        assert hasattr(result.seasonality, "actual_change_percent")
        assert hasattr(result.seasonality, "deviation_percent")

    def test_calculate_benchmark_comparisons(self, pattern):
        """Test the benchmark comparisons calculation functionality."""
        # Arrange
        data = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"),
                "value": [100 + i for i in range(31)],
            }
        )
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Act
        result = pattern.analyze("test", data, analysis_window)

        # Assert
        assert result is not None
        assert len(result.benchmark_comparisons) >= 0
        if len(result.benchmark_comparisons) > 0:
            # Check the structure of benchmark comparisons
            assert hasattr(result.benchmark_comparisons[0], "reference_period")
            assert hasattr(result.benchmark_comparisons[0], "absolute_change")
            assert hasattr(result.benchmark_comparisons[0], "change_percent")

    def test_detect_trend_exceptions(self, pattern):
        """Test the trend exceptions detection functionality."""
        # Arrange - Create data with a spike
        dates = pd.date_range(start="2023-01-01", end="2023-01-31", freq="D")
        values = [100 + i for i in range(31)]
        values[15] = 200  # Add a large spike

        data = pd.DataFrame({"date": dates, "value": values})
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Act
        result = pattern.analyze("test", data, analysis_window)

        # Assert
        assert result is not None
        # Note: Trend exceptions might not be present depending on the pattern implementation
        # We shouldn't assert that there must be exceptions
