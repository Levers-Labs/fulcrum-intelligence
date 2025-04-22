"""
Unit tests for the PerformanceStatusPattern.
"""

import pandas as pd
import pytest

from levers.models import AnalysisWindow, Granularity, MetricGVAStatus
from levers.models.patterns import (
    HoldSteady,
    MetricPerformance,
    StatusChange,
    Streak,
)
from levers.patterns import PerformanceStatusPattern


class TestPerformanceStatusPattern:
    """Tests for the PerformanceStatusPattern class."""

    @pytest.fixture
    def pattern(self):
        """Return a PerformanceStatusPattern instance."""
        return PerformanceStatusPattern()

    @pytest.fixture
    def sample_data(self):
        """Return sample data for testing."""
        # Create sample data with date, value, and target columns
        return pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"),
                "value": [100, 105, 110, 115, 120, 115, 110, 105, 100, 95],
                "target": [110, 110, 110, 110, 110, 110, 110, 110, 110, 110],
            }
        )

    @pytest.fixture
    def sample_data_with_status_change(self):
        """Return sample data with status changes for testing."""
        return pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"),
                "value": [110, 111, 112, 113, 114, 95, 96, 97, 98, 99],  # Initially above target, then below
                "target": [100, 100, 100, 100, 100, 100, 100, 100, 100, 100],  # Constant
            }
        )

    @pytest.fixture
    def sample_data_with_streak(self):
        """Return sample data with a consistent streak for testing."""
        return pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"),
                "value": [90, 91, 92, 93, 94, 95, 96, 97, 98, 99],  # Consistently increasing
                "target": [100, 100, 100, 100, 100, 100, 100, 100, 100, 100],  # Constant
            }
        )

    @pytest.fixture
    def sample_data_for_hold_steady(self):
        """Return sample data for hold steady tests."""
        return pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"),
                "value": [98, 99, 100, 101, 102, 102, 101, 101, 100, 102],  # Around/above target
                "target": [100, 100, 100, 100, 100, 100, 100, 100, 100, 100],  # Constant
            }
        )

    @pytest.fixture
    def analysis_window(self):
        """Return an AnalysisWindow for testing."""
        return AnalysisWindow(start_date="2023-01-01", end_date="2023-01-10", grain=Granularity.DAY)

    def test_pattern_attributes(self, pattern):
        """Test pattern class attributes."""
        assert pattern.name == "performance_status"
        assert pattern.version == "1.0"
        assert pattern.output_model == MetricPerformance
        assert len(pattern.required_primitives) > 0
        assert "classify_metric_status" in pattern.required_primitives
        assert "detect_status_changes" in pattern.required_primitives

    def test_analyze_with_target(self, pattern, sample_data, analysis_window, mocker):
        """Test analyze method with target data."""
        # Arrange
        metric_id = "test_metric"

        # Mock primitives
        mocker.patch(
            "levers.patterns.performance_status.classify_metric_status", return_value=MetricGVAStatus.OFF_TRACK
        )
        mocker.patch.object(pattern, "_calculate_streak_info", return_value=None)
        mocker.patch.object(pattern, "_calculate_status_change", return_value=None)
        mocker.patch.object(pattern, "_calculate_hold_steady", return_value=None)

        # Act
        result = pattern.analyze(metric_id=metric_id, data=sample_data, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.pattern == "performance_status"
        assert result.current_value == 95.0
        assert result.target_value == 110.0
        assert result.status == MetricGVAStatus.OFF_TRACK
        assert result.absolute_gap == 15.0
        assert result.analysis_window == analysis_window

    def test_analyze_without_target(self, pattern, analysis_window, mocker):
        """Test analyze method without target data."""
        # Arrange
        metric_id = "test_metric"
        data = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"),
                "value": [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
            }
        )

        # Act
        result = pattern.analyze(metric_id=metric_id, data=data, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.pattern == "performance_status"
        assert result.current_value == 109.0
        assert result.target_value is None
        assert result.status == MetricGVAStatus.NO_TARGET
        assert result.analysis_window == analysis_window

    def test_analyze_with_status_change(self, pattern, sample_data_with_status_change, analysis_window, mocker):
        """Test analyze method with status change."""
        # Arrange
        metric_id = "test_metric"
        status_change = StatusChange(
            has_flipped=True,
            old_status=MetricGVAStatus.ON_TRACK,
            new_status=MetricGVAStatus.OFF_TRACK,
            old_status_duration_grains=5,
        )

        # Mock primitives
        mocker.patch(
            "levers.patterns.performance_status.classify_metric_status", return_value=MetricGVAStatus.OFF_TRACK
        )
        mocker.patch.object(pattern, "_calculate_streak_info", return_value=None)
        mocker.patch.object(pattern, "_calculate_status_change", return_value=status_change)
        mocker.patch.object(pattern, "_calculate_hold_steady", return_value=None)

        # Act
        result = pattern.analyze(
            metric_id=metric_id, data=sample_data_with_status_change, analysis_window=analysis_window
        )

        # Assert
        assert result is not None
        assert result.pattern == "performance_status"
        assert result.status == MetricGVAStatus.OFF_TRACK
        assert result.status_change == status_change
        assert result.status_change.has_flipped is True
        assert result.status_change.old_status == MetricGVAStatus.ON_TRACK
        assert result.status_change.new_status == MetricGVAStatus.OFF_TRACK
        assert result.status_change.old_status_duration_grains == 5
        assert result.analysis_window == analysis_window

    def test_analyze_with_streak(self, pattern, sample_data_with_streak, analysis_window, mocker):
        """Test analyze method with value streak."""
        # Arrange
        metric_id = "test_metric"
        streak = Streak(
            length=9,
            status=MetricGVAStatus.OFF_TRACK,
            performance_change_percent_over_streak=10.0,
            absolute_change_over_streak=9.0,
            average_change_percent_per_grain=1.11,
            average_change_absolute_per_grain=1.0,
        )

        # Mock primitives
        mocker.patch(
            "levers.patterns.performance_status.classify_metric_status", return_value=MetricGVAStatus.OFF_TRACK
        )
        mocker.patch.object(pattern, "_calculate_streak_info", return_value=streak)
        mocker.patch.object(pattern, "_calculate_status_change", return_value=None)
        mocker.patch.object(pattern, "_calculate_hold_steady", return_value=None)

        # Act
        result = pattern.analyze(metric_id=metric_id, data=sample_data_with_streak, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.pattern == "performance_status"
        assert result.streak == streak
        assert result.streak.length == 9
        assert result.streak.status == MetricGVAStatus.OFF_TRACK
        assert result.streak.absolute_change_over_streak == 9.0
        assert result.streak.average_change_absolute_per_grain == 1.0
        assert result.analysis_window == analysis_window

    def test_analyze_with_hold_steady(self, pattern, sample_data_for_hold_steady, analysis_window, mocker):
        """Test analyze method with hold steady calculation."""
        # Arrange
        metric_id = "test_metric"
        hold_steady = HoldSteady(
            is_currently_at_or_above_target=True,
            time_to_maintain_grains=3,
            current_margin_percent=2.0,
        )

        # Mock primitives
        mocker.patch("levers.patterns.performance_status.classify_metric_status", return_value=MetricGVAStatus.ON_TRACK)
        mocker.patch.object(pattern, "_calculate_streak_info", return_value=None)
        mocker.patch.object(pattern, "_calculate_status_change", return_value=None)
        mocker.patch.object(pattern, "_calculate_hold_steady", return_value=hold_steady)

        # Act
        result = pattern.analyze(metric_id=metric_id, data=sample_data_for_hold_steady, analysis_window=analysis_window)

        # Assert
        assert result is not None
        assert result.pattern == "performance_status"
        assert result.hold_steady == hold_steady
        assert result.hold_steady.is_currently_at_or_above_target is True
        assert result.hold_steady.time_to_maintain_grains == 3
        assert result.hold_steady.current_margin_percent == 2.0
        assert result.analysis_window == analysis_window

    def test_calculate_status_change_method_direct(self, pattern, mocker):
        """Test _calculate_status_change method directly."""
        # Arrange
        data = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"),
                "status": [
                    MetricGVAStatus.ON_TRACK,
                    MetricGVAStatus.ON_TRACK,
                    MetricGVAStatus.ON_TRACK,
                    MetricGVAStatus.ON_TRACK,
                    MetricGVAStatus.ON_TRACK,
                    MetricGVAStatus.OFF_TRACK,
                    MetricGVAStatus.OFF_TRACK,
                    MetricGVAStatus.OFF_TRACK,
                    MetricGVAStatus.OFF_TRACK,
                    MetricGVAStatus.ON_TRACK,
                ],
            }
        )
        current_status = MetricGVAStatus.ON_TRACK

        # Act
        result = pattern._calculate_status_change(data, current_status)

        # Assert
        assert result is not None
        assert isinstance(result, StatusChange)
        assert result.has_flipped is True
        assert result.old_status == MetricGVAStatus.OFF_TRACK
        assert result.new_status == MetricGVAStatus.ON_TRACK
        assert result.old_status_duration_grains == 4

    def test_calculate_status_change_no_date_column(self, pattern, mocker):
        """Test _calculate_status_change method with no date column."""
        # Arrange
        data = pd.DataFrame(
            {
                "status": [
                    MetricGVAStatus.ON_TRACK,
                    MetricGVAStatus.ON_TRACK,
                    MetricGVAStatus.OFF_TRACK,  # Change here
                    MetricGVAStatus.OFF_TRACK,
                    MetricGVAStatus.ON_TRACK,
                ]
            }
        )
        current_status = MetricGVAStatus.ON_TRACK

        # Act
        result = pattern._calculate_status_change(data, current_status)

        # Assert
        assert result is not None
        assert isinstance(result, StatusChange)
        assert result.has_flipped is True
        assert result.old_status == MetricGVAStatus.OFF_TRACK
        assert result.new_status == MetricGVAStatus.ON_TRACK
        assert result.old_status_duration_grains == 2

    def test_calculate_streak_info_increasing(self, pattern, mocker):
        """Test _calculate_streak_info method with increasing values."""
        # Arrange
        data = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"),
                "value": [90, 91, 92, 93, 94, 95, 96, 97, 98, 99],  # Increasing streak
            }
        )
        current_status = MetricGVAStatus.OFF_TRACK

        # Act
        result = pattern._calculate_streak_info(data, current_status)

        # Assert
        assert result is not None
        assert isinstance(result, Streak)
        assert result.length == 10
        assert result.status == MetricGVAStatus.OFF_TRACK
        assert result.absolute_change_over_streak == 9
        assert result.average_change_absolute_per_grain == 0.9

    def test_calculate_streak_info_decreasing(self, pattern, mocker):
        """Test _calculate_streak_info method with decreasing values."""
        # Arrange
        data = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"),
                "value": [99, 98, 97, 96, 95, 94, 93, 92, 91, 90],  # Decreasing streak
            }
        )
        current_status = MetricGVAStatus.OFF_TRACK

        # Act
        result = pattern._calculate_streak_info(data, current_status)

        # Assert
        assert result is not None
        assert isinstance(result, Streak)
        assert result.length == 10
        assert result.status == MetricGVAStatus.OFF_TRACK
        assert result.absolute_change_over_streak == -9.0
        assert result.average_change_absolute_per_grain == -0.9

    def test_calculate_streak_info_stable(self, pattern, mocker):
        """Test _calculate_streak_info method with stable values."""
        # Arrange
        data = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"),
                "value": [95, 95, 95, 95, 95, 95, 95, 95, 95, 95],  # Stable values
            }
        )
        current_status = MetricGVAStatus.OFF_TRACK

        mocker.patch("levers.patterns.performance_status.calculate_difference", return_value=0.0)

        # Act
        result = pattern._calculate_streak_info(data, current_status)

        # Assert
        assert result is not None
        assert isinstance(result, Streak)
        assert result.length == 10
        assert result.status == MetricGVAStatus.OFF_TRACK
        assert result.absolute_change_over_streak == 0.0
        assert result.average_change_absolute_per_grain == 0.0

    def test_calculate_streak_info_mixed(self, pattern, mocker):
        """Test _calculate_streak_info method with mixed direction."""
        # Arrange
        data = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"),
                "value": [95, 96, 97, 96, 95, 94, 95, 96, 97, 98],  # Mixed directions
            }
        )
        current_status = MetricGVAStatus.OFF_TRACK

        mocker.patch("levers.patterns.performance_status.calculate_difference", return_value=3.0)

        # Act
        result = pattern._calculate_streak_info(data, current_status)

        # Assert
        assert result is not None
        assert isinstance(result, Streak)
        assert result.status == MetricGVAStatus.OFF_TRACK
        assert result.absolute_change_over_streak == 3.0

    def test_calculate_streak_info_one_point(self, pattern):
        """Test _calculate_streak_info method with only one data point."""
        # Arrange
        data = pd.DataFrame({"date": [pd.Timestamp("2023-01-01")], "value": [95]})  # Only one data point
        current_status = MetricGVAStatus.OFF_TRACK

        # Act
        result = pattern._calculate_streak_info(data, current_status)

        # Assert
        assert result is None  # Cannot detect streak with just one point

    def test_calculate_hold_steady_above_target(self, pattern, mocker):
        """Test _calculate_hold_steady method implementation when above target."""
        # Arrange
        current_value = 102
        target_value = 100

        mocker.patch("levers.patterns.performance_status.monitor_threshold_proximity", return_value=True)

        # Act
        result = pattern._calculate_hold_steady(current_value, target_value)

        # Assert
        assert result is not None
        assert isinstance(result, HoldSteady)
        assert result.is_currently_at_or_above_target is True
        assert result.time_to_maintain_grains == 3  # Close to target
        assert result.current_margin_percent == 2.0

    def test_calculate_hold_steady_not_above_target(self, pattern):
        """Test _calculate_hold_steady method when value is not above target."""
        # Arrange
        current_value = 98  # Below target
        target_value = 100

        # Act
        result = pattern._calculate_hold_steady(current_value, target_value)

        # Assert
        assert result is None  # Should return None when not above target

    def test_calculate_hold_steady_not_close(self, pattern, mocker):
        """Test _calculate_hold_steady method when value is not close to target."""
        # Arrange
        current_value = 120  # Well above target
        target_value = 100

        mocker.patch("levers.patterns.performance_status.monitor_threshold_proximity", return_value=False)

        # Act
        result = pattern._calculate_hold_steady(current_value, target_value)

        # Assert
        assert result is not None
        assert isinstance(result, HoldSteady)
        assert result.is_currently_at_or_above_target is True
        assert result.time_to_maintain_grains == 5  # Not close to target
        assert result.current_margin_percent == 20.0

    def test_calculate_hold_steady_calculation_error(self, pattern, mocker):
        """Test _calculate_hold_steady method when calculation throws an error."""
        # Arrange
        current_value = 150
        target_value = 100

        mocker.patch("levers.patterns.performance_status.monitor_threshold_proximity", return_value=True)
        mocker.patch(
            "levers.patterns.performance_status.calculate_gap_to_target",
            side_effect=Exception("Calculation error"),
        )

        # Act
        result = pattern._calculate_hold_steady(current_value, target_value)

        # Assert
        assert result is not None
        assert isinstance(result, HoldSteady)
        assert result.is_currently_at_or_above_target is True
        assert result.time_to_maintain_grains == 3  # Close to target
        assert result.current_margin_percent is None  # Should be None due to calculation error
