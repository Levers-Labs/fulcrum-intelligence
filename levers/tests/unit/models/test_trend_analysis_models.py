"""
Unit tests for trend analysis models.
"""

import pytest
from pydantic import ValidationError

from levers.models import (
    PerformancePlateau,
    RecordHigh,
    RecordLow,
    TrendAnalysis,
    TrendType,
)


class TestTrendAnalysis:
    """Tests for the TrendAnalysis class."""

    def test_valid_creation(self):
        """Test creating a valid TrendAnalysis object."""
        # Arrange & Act
        trend = TrendAnalysis(
            trend_type=TrendType.UPWARD,
            trend_slope=1.5,
            trend_confidence=0.95,
            normalized_slope=0.02,
            recent_trend_type=TrendType.UPWARD,
            is_accelerating=True,
            is_plateaued=False,
        )

        # Assert
        assert trend.trend_type == TrendType.UPWARD
        assert trend.trend_slope == 1.5
        assert trend.trend_confidence == 0.95
        assert trend.normalized_slope == 0.02
        assert trend.recent_trend_type == TrendType.UPWARD
        assert trend.is_accelerating is True
        assert trend.is_plateaued is False

    def test_minimal_creation(self):
        """Test creating a TrendAnalysis with minimal fields."""
        # Arrange & Act
        trend = TrendAnalysis(trend_type=TrendType.STABLE)

        # Assert
        assert trend.trend_type == TrendType.STABLE
        assert trend.trend_slope is None
        assert trend.trend_confidence is None
        assert trend.normalized_slope is None
        assert trend.recent_trend_type is None
        assert trend.is_accelerating is False
        assert trend.is_plateaued is False

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        trend = TrendAnalysis(trend_type=TrendType.UPWARD, trend_slope=1.5, trend_confidence=0.95)

        # Act
        result = trend.to_dict()

        # Assert
        assert result["trend_type"] == "upward"
        assert result["trend_slope"] == 1.5
        assert result["trend_confidence"] == 0.95
        assert result["is_accelerating"] is False
        assert result["is_plateaued"] is False

    def test_invalid_trend_type(self):
        """Test with invalid trend type."""
        # Act & Assert
        with pytest.raises(ValidationError):
            TrendAnalysis(trend_type="invalid_trend")


class TestPerformancePlateau:
    """Tests for the PerformancePlateau class."""

    def test_valid_creation(self):
        """Test creating a valid PerformancePlateau object."""
        # Arrange & Act
        plateau = PerformancePlateau(is_plateaued=True, plateau_duration=10, stability_score=0.95, mean_value=100.5)

        # Assert
        assert plateau.is_plateaued is True
        assert plateau.plateau_duration == 10
        assert plateau.stability_score == 0.95
        assert plateau.mean_value == 100.5

    def test_default_values(self):
        """Test default values."""
        # Arrange & Act
        plateau = PerformancePlateau()

        # Assert
        assert plateau.is_plateaued is False
        assert plateau.plateau_duration == 0
        assert plateau.stability_score == 0.0
        assert plateau.mean_value is None

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        plateau = PerformancePlateau(is_plateaued=True, plateau_duration=10, stability_score=0.95, mean_value=100.5)

        # Act
        result = plateau.to_dict()

        # Assert
        assert result["is_plateaued"] is True
        assert result["plateau_duration"] == 10
        assert result["stability_score"] == 0.95
        assert result["mean_value"] == 100.5


class TestRecordHigh:
    """Tests for the RecordHigh class."""

    def test_valid_creation(self):
        """Test creating a valid RecordHigh object."""
        # Arrange & Act
        record = RecordHigh(
            is_record_high=True,
            current_value=100.5,
            rank=1,
            periods_compared=30,
            absolute_delta=10.5,
            percentage_delta=10.5,
            prior_max=90.0,
            prior_max_index=5,
        )

        # Assert
        assert record.is_record_high is True
        assert record.current_value == 100.5
        assert record.rank == 1
        assert record.periods_compared == 30
        assert record.absolute_delta == 10.5
        assert record.percentage_delta == 10.5
        assert record.prior_max == 90.0
        assert record.prior_max_index == 5

    def test_default_is_record_high(self):
        """Test default is_record_high value."""
        # Arrange & Act
        record = RecordHigh(
            current_value=100.5,
            rank=1,
            periods_compared=30,
            absolute_delta=10.5,
            percentage_delta=10.5,
            prior_max=90.0,
            prior_max_index=5,
        )

        # Assert
        assert record.is_record_high is False

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        record = RecordHigh(
            is_record_high=True,
            current_value=100.5,
            rank=1,
            periods_compared=30,
            absolute_delta=10.5,
            percentage_delta=10.5,
            prior_max=90.0,
            prior_max_index=5,
        )

        # Act
        result = record.to_dict()

        # Assert
        assert result["is_record_high"] is True
        assert result["current_value"] == 100.5
        assert result["rank"] == 1
        assert result["periods_compared"] == 30
        assert result["absolute_delta"] == 10.5
        assert result["percentage_delta"] == 10.5
        assert result["prior_max"] == 90.0
        assert result["prior_max_index"] == 5

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            RecordHigh(
                is_record_high=True,
                current_value=100.5,
                rank=1,
                periods_compared=30,
                absolute_delta=10.5,
                percentage_delta=10.5,
                # Missing prior_max and prior_max_index
            )


class TestRecordLow:
    """Tests for the RecordLow class."""

    def test_valid_creation(self):
        """Test creating a valid RecordLow object."""
        # Arrange & Act
        record = RecordLow(
            is_record_low=True,
            current_value=90.0,
            rank=1,
            periods_compared=30,
            absolute_delta=-10.5,
            percentage_delta=-10.5,
            prior_min=100.5,
            prior_min_index=5,
        )

        # Assert
        assert record.is_record_low is True
        assert record.current_value == 90.0
        assert record.rank == 1
        assert record.periods_compared == 30
        assert record.absolute_delta == -10.5
        assert record.percentage_delta == -10.5
        assert record.prior_min == 100.5
        assert record.prior_min_index == 5

    def test_default_is_record_low(self):
        """Test default is_record_low value."""
        # Arrange & Act
        record = RecordLow(
            current_value=90.0,
            rank=1,
            periods_compared=30,
            absolute_delta=-10.5,
            percentage_delta=-10.5,
            prior_min=100.5,
            prior_min_index=5,
        )

        # Assert
        assert record.is_record_low is False

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        record = RecordLow(
            is_record_low=True,
            current_value=90.0,
            rank=1,
            periods_compared=30,
            absolute_delta=-10.5,
            percentage_delta=-10.5,
            prior_min=100.5,
            prior_min_index=5,
        )

        # Act
        result = record.to_dict()

        # Assert
        assert result["is_record_low"] is True
        assert result["current_value"] == 90.0
        assert result["rank"] == 1
        assert result["periods_compared"] == 30
        assert result["absolute_delta"] == -10.5
        assert result["percentage_delta"] == -10.5
        assert result["prior_min"] == 100.5
        assert result["prior_min_index"] == 5

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            RecordLow(
                is_record_low=True,
                current_value=90.0,
                rank=1,
                periods_compared=30,
                absolute_delta=-10.5,
                percentage_delta=-10.5,
                # Missing prior_min and prior_min_index
            )
