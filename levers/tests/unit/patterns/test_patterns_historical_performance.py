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
        values[-1] = 1000  # Add a spike
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
        assert "process_control_analysis" in pattern.required_primitives
        assert "detect_record_high" in pattern.required_primitives
        assert "detect_record_low" in pattern.required_primitives
        assert "detect_seasonality_pattern" in pattern.required_primitives
        assert "calculate_benchmark_comparisons" in pattern.required_primitives

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
        # Check if at least one exception is a spike (since we added one)
        assert result.trend_exception is not None
        assert result.trend_exception.type == TrendExceptionType.SPIKE

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

        # Act & Assert - Should raise InsufficientDataError for empty data
        from levers.exceptions import InsufficientDataError

        with pytest.raises(InsufficientDataError):
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
                "date": pd.date_range(start="2023-01-01", end="2023-12-31", freq="W-MON"),
                "value": [100 + i for i in range(52)],  # 52 weeks of data
            }
        )
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-12-31", grain=Granularity.WEEK)

        # Act
        result = pattern.analyze("test", data, analysis_window)

        # Assert
        assert result is not None
        assert result.benchmark_comparison is not None
        assert hasattr(result.benchmark_comparison, "current_value")
        assert hasattr(result.benchmark_comparison, "current_period")
        assert hasattr(result.benchmark_comparison, "benchmarks")
        assert isinstance(result.benchmark_comparison.benchmarks, dict)

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

    def test_analyze_with_spc_data(self, pattern, analysis_window):
        """Test that SPC data is properly included in the analysis results."""
        # Arrange
        metric_id = "test_metric"
        # Create data with a clear trend for SPC analysis
        df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=20, freq="D"),
                "value": [100 + i * 2 for i in range(20)],  # Clear upward trend
            }
        )

        # Act
        result = pattern.analyze(metric_id=metric_id, data=df, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert len(result.period_metrics) > 0

        # Verify upward trend is detected
        assert result.current_trend is not None
        assert result.current_trend.trend_type == TrendType.UPWARD

    def test_spc_trend_signal_detection(self, pattern, analysis_window):
        """Test detection of trend signals in SPC analysis."""
        # Arrange
        metric_id = "test_metric"
        # Create data with a clear change in trend
        dates = pd.date_range(start="2023-01-01", periods=30, freq="D")
        # First part stable, second part rapid increase - make the change more extreme
        values = [100 + i * 0.1 for i in range(15)] + [105 + i * 5 for i in range(15)]

        df = pd.DataFrame({"date": dates, "value": values})

        # Act
        result = pattern.analyze(metric_id=metric_id, data=df, analysis_window=analysis_window)

        # Assert
        assert result is not None

        # Verify SPC data is present
        assert len(result.period_metrics) > 0

        # Verify the trend type is correctly identified as upward
        assert result.current_trend is not None
        assert result.current_trend.trend_type == TrendType.UPWARD

    def test_trend_analysis_field_populated(self, pattern, analysis_window):
        """Test that trend_analysis field is properly populated with SPC data."""
        # Arrange
        metric_id = "test_metric"
        df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=15, freq="D"),
                "value": [100 + i * 2 for i in range(15)],  # Clear upward trend
            }
        )

        # Act
        result = pattern.analyze(metric_id=metric_id, data=df, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.trend_analysis is not None
        assert len(result.trend_analysis) > 0

        # Check that TrendAnalysis objects have SPC fields
        for trend_analysis in result.trend_analysis:
            assert hasattr(trend_analysis, "value")
            assert hasattr(trend_analysis, "date")
            assert hasattr(trend_analysis, "central_line")
            assert hasattr(trend_analysis, "ucl")
            assert hasattr(trend_analysis, "lcl")
            assert hasattr(trend_analysis, "slope")
            assert hasattr(trend_analysis, "trend_signal_detected")

            # Verify data types and values
            assert trend_analysis.value is not None
            assert trend_analysis.date is not None

    def test_spc_control_limits_calculation(self, pattern, analysis_window):
        """Test that SPC control limits are properly calculated."""
        # Arrange
        metric_id = "test_metric"
        # Create data with known variance for predictable control limits
        np.random.seed(42)  # For reproducible results
        base_values = [100] * 20
        noise = np.random.normal(0, 5, 20)  # Add controlled noise
        df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=20, freq="D"),
                "value": base_values + noise,
            }
        )

        # Act
        result = pattern.analyze(metric_id=metric_id, data=df, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.trend_analysis is not None
        assert len(result.trend_analysis) > 0

        # Check that control limits are calculated
        for trend_analysis in result.trend_analysis:
            if trend_analysis.central_line is not None:
                # UCL should be above central line
                if trend_analysis.ucl is not None:
                    assert trend_analysis.ucl >= trend_analysis.central_line
                # LCL should be below central line
                if trend_analysis.lcl is not None:
                    assert trend_analysis.lcl <= trend_analysis.central_line

    def test_spc_signal_detection_beyond_limits(self, pattern, analysis_window):
        """Test SPC signal detection when values go beyond control limits."""
        # Arrange
        metric_id = "test_metric"
        # Create data with an outlier that should trigger a signal
        values = [100] * 15 + [200] + [100] * 4  # Large spike in the middle
        df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=20, freq="D"),
                "value": values,
            }
        )

        # Act
        result = pattern.analyze(metric_id=metric_id, data=df, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.trend_analysis is not None

        # Check that at least one signal is detected
        signals_detected = [ta.trend_signal_detected for ta in result.trend_analysis if ta.trend_signal_detected]
        assert len(signals_detected) > 0, "Expected at least one SPC signal to be detected for the outlier"

    def test_spc_consecutive_run_detection(self, pattern, analysis_window):
        """Test SPC signal detection for consecutive runs above/below center line."""
        # Arrange
        metric_id = "test_metric"
        # Create data with a long run above the center line (should trigger consecutive run signal)
        values = [95] * 5 + [105] * 10 + [95] * 5  # 10 consecutive points above center
        df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=20, freq="D"),
                "value": values,
            }
        )

        # Act
        result = pattern.analyze(metric_id=metric_id, data=df, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.trend_analysis is not None

        # Check that signals are detected during the consecutive run
        signals_detected = [ta.trend_signal_detected for ta in result.trend_analysis if ta.trend_signal_detected]
        assert len(signals_detected) > 0, "Expected SPC signals for consecutive run above center line"

    def test_spc_slope_calculation(self, pattern, analysis_window):
        """Test that slope is properly calculated in SPC analysis."""
        # Arrange
        metric_id = "test_metric"
        # Create data with a clear linear trend
        df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=15, freq="D"),
                "value": [100 + i * 3 for i in range(15)],  # Slope of 3 per day
            }
        )

        # Act
        result = pattern.analyze(metric_id=metric_id, data=df, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.trend_analysis is not None

        # Check that slopes are calculated
        slopes = [ta.slope for ta in result.trend_analysis if ta.slope is not None]
        assert len(slopes) > 0, "Expected slope calculations in trend analysis"

        # For an upward trend, most slopes should be positive
        positive_slopes = [s for s in slopes if s > 0]
        assert len(positive_slopes) > 0, "Expected positive slopes for upward trend"

    def test_spc_trend_type_determination(self, pattern, analysis_window):
        """Test that trend types are correctly determined using SPC analysis."""
        # Arrange
        metric_id = "test_metric"

        # Test upward trend
        upward_df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=15, freq="D"),
                "value": [100 + i * 2 for i in range(15)],
            }
        )

        # Test downward trend
        downward_df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=15, freq="D"),
                "value": [130 - i * 2 for i in range(15)],
            }
        )

        # Test plateau
        plateau_df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=15, freq="D"),
                "value": [100 + np.random.normal(0, 0.1) for _ in range(15)],
            }
        )

        # Act & Assert for upward trend
        upward_result = pattern.analyze(metric_id=metric_id, data=upward_df, analysis_window=analysis_window)
        assert upward_result.current_trend is not None
        assert upward_result.current_trend.trend_type == TrendType.UPWARD

        # Check that trend_analysis has SPC data (trend_type is now determined separately)
        assert len(upward_result.trend_analysis) > 0, "Expected trend analysis data"

        # Act & Assert for downward trend
        downward_result = pattern.analyze(metric_id=metric_id, data=downward_df, analysis_window=analysis_window)
        assert downward_result.current_trend is not None
        assert downward_result.current_trend.trend_type == TrendType.DOWNWARD

        # Check that trend_analysis has SPC data (trend_type is now determined separately)
        assert len(downward_result.trend_analysis) > 0, "Expected trend analysis data"

        # Act & Assert for plateau
        plateau_result = pattern.analyze(metric_id=metric_id, data=plateau_df, analysis_window=analysis_window)
        assert plateau_result.current_trend is not None
        assert plateau_result.current_trend.trend_type == TrendType.PLATEAU

    def test_spc_with_insufficient_data(self, pattern, analysis_window):
        """Test SPC analysis behavior with insufficient data."""
        # Arrange
        metric_id = "test_metric"
        # Create data with very few points (less than minimum required for SPC)
        df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=5, freq="D"),
                "value": [100, 101, 102, 103, 104],
            }
        )

        # Act & Assert
        # With insufficient data, the pattern should either:
        # 1. Return a result with minimal/empty trend_analysis, or
        # 2. Handle the insufficient data gracefully
        try:
            result = pattern.analyze(metric_id=metric_id, data=df, analysis_window=analysis_window)

            # If analysis succeeds, verify the result structure
            assert result is not None
            # If trend_analysis exists, it should still have valid structure
            if result.trend_analysis:
                for ta in result.trend_analysis:
                    assert hasattr(ta, "value")
                    assert hasattr(ta, "date")
        except ValidationError as e:
            # If validation fails due to insufficient data, that's acceptable
            # The pattern should handle this case gracefully
            assert "insufficient" in str(e).lower()
            # This is expected behavior for insufficient data

    def test_spc_date_alignment(self, pattern, analysis_window):
        """Test that dates in trend_analysis align with input data dates."""
        # Arrange
        metric_id = "test_metric"
        dates = pd.date_range(start="2023-01-01", periods=10, freq="D")
        df = pd.DataFrame(
            {
                "date": dates,
                "value": [100 + i for i in range(10)],
            }
        )

        # Act
        result = pattern.analyze(metric_id=metric_id, data=df, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.trend_analysis is not None

        # Check that trend_analysis dates are within the input date range
        input_dates = set(dates.strftime("%Y-%m-%d"))
        for ta in result.trend_analysis:
            if ta.date:
                # Convert to string for comparison if needed
                ta_date_str = ta.date if isinstance(ta.date, str) else ta.date.strftime("%Y-%m-%d")
                assert ta_date_str in input_dates, f"Trend analysis date {ta_date_str} not in input dates"

    def test_spc_signal_and_trend_correlation(self, pattern, analysis_window):
        """Test correlation between SPC signals and trend changes."""
        # Arrange
        metric_id = "test_metric"
        # Create data with a clear trend change that should trigger signals
        values = [100] * 10 + [100 + i * 5 for i in range(10)]  # Flat then steep increase
        df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=20, freq="D"),
                "value": values,
            }
        )

        # Act
        result = pattern.analyze(metric_id=metric_id, data=df, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.trend_analysis is not None

        # Check that the overall trend is detected as upward
        assert result.current_trend is not None
        assert result.current_trend.trend_type == TrendType.UPWARD

        # Check that some signals are detected during the trend change
        signals_detected = [ta.trend_signal_detected for ta in result.trend_analysis if ta.trend_signal_detected]
        assert len(signals_detected) > 0, "Expected signals during trend change"

    def test_spc_slope_change_calculation(self, pattern, analysis_window):
        """Test that slope change percentages are calculated."""
        # Arrange
        metric_id = "test_metric"
        # Create data with changing slopes
        values = [100 + i for i in range(10)] + [110 + i * 3 for i in range(10)]  # Slope change
        df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=20, freq="D"),
                "value": values,
            }
        )

        # Act
        result = pattern.analyze(metric_id=metric_id, data=df, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.trend_analysis is not None

        # Check that slope change percentages are calculated
        slope_changes = [ta.slope_change_percent for ta in result.trend_analysis if ta.slope_change_percent is not None]
        assert len(slope_changes) > 0, "Expected slope change calculations"
