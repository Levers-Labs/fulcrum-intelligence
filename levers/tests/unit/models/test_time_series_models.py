"""
Unit tests for time series models.
"""

import pytest
from pydantic import ValidationError

from levers.models import AverageGrowth, TimeSeriesSlope, ToDateGrowth


class TestAverageGrowth:
    """Tests for the AverageGrowth class."""

    def test_valid_creation(self):
        """Test creating a valid AverageGrowth object."""
        # Arrange & Act
        avg_growth = AverageGrowth(average_growth=10.5, total_growth=50.0, periods=5)

        # Assert
        assert avg_growth.average_growth == 10.5
        assert avg_growth.total_growth == 50.0
        assert avg_growth.periods == 5

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        avg_growth = AverageGrowth(average_growth=None, total_growth=None, periods=5)

        # Assert
        assert avg_growth.average_growth is None
        assert avg_growth.total_growth is None
        assert avg_growth.periods == 5

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            AverageGrowth(
                average_growth=10.5,
                total_growth=50.0,
                # Missing periods
            )

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        avg_growth = AverageGrowth(average_growth=10.5, total_growth=50.0, periods=5)

        # Act
        result = avg_growth.to_dict()

        # Assert
        assert result["average_growth"] == 10.5
        assert result["total_growth"] == 50.0
        assert result["periods"] == 5


class TestToDateGrowth:
    """Tests for the ToDateGrowth class."""

    def test_valid_creation(self):
        """Test creating a valid ToDateGrowth object."""
        # Arrange & Act
        growth = ToDateGrowth(current_value=110.0, prior_value=100.0, abs_diff=10.0, growth_rate=10.0)

        # Assert
        assert growth.current_value == 110.0
        assert growth.prior_value == 100.0
        assert growth.abs_diff == 10.0
        assert growth.growth_rate == 10.0

    def test_optional_fields(self):
        """Test creating an object with optional fields as None."""
        # Arrange & Act
        growth = ToDateGrowth(current_value=110.0, prior_value=100.0, abs_diff=10.0, growth_rate=None)

        # Assert
        assert growth.current_value == 110.0
        assert growth.prior_value == 100.0
        assert growth.abs_diff == 10.0
        assert growth.growth_rate is None

    def test_required_fields(self):
        """Test required fields validation."""
        # Act & Assert
        with pytest.raises(ValidationError):
            ToDateGrowth(
                current_value=110.0,
                prior_value=100.0,
                # Missing abs_diff
            )

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        growth = ToDateGrowth(current_value=110.0, prior_value=100.0, abs_diff=10.0, growth_rate=10.0)

        # Act
        result = growth.to_dict()

        # Assert
        assert result["current_value"] == 110.0
        assert result["prior_value"] == 100.0
        assert result["abs_diff"] == 10.0
        assert result["growth_rate"] == 10.0


class TestTimeSeriesSlope:
    """Tests for the TimeSeriesSlope class."""

    def test_valid_creation(self):
        """Test creating a valid TimeSeriesSlope object."""
        # Arrange & Act
        slope = TimeSeriesSlope(
            slope=1.5,
            intercept=100.0,
            r_value=0.95,
            p_value=0.01,
            std_err=0.1,
            slope_per_day=1.5,
            slope_per_week=10.5,
            slope_per_month=45.0,
            slope_per_year=547.5,
        )

        # Assert
        assert slope.slope == 1.5
        assert slope.intercept == 100.0
        assert slope.r_value == 0.95
        assert slope.p_value == 0.01
        assert slope.std_err == 0.1
        assert slope.slope_per_day == 1.5
        assert slope.slope_per_week == 10.5
        assert slope.slope_per_month == 45.0
        assert slope.slope_per_year == 547.5

    def test_all_fields_optional(self):
        """Test creating an object with all fields as None."""
        # Arrange & Act
        slope = TimeSeriesSlope(
            slope=None,
            intercept=None,
            r_value=None,
            p_value=None,
            std_err=None,
            slope_per_day=None,
            slope_per_week=None,
            slope_per_month=None,
            slope_per_year=None,
        )

        # Assert
        assert slope.slope is None
        assert slope.intercept is None
        assert slope.r_value is None
        assert slope.p_value is None
        assert slope.std_err is None
        assert slope.slope_per_day is None
        assert slope.slope_per_week is None
        assert slope.slope_per_month is None
        assert slope.slope_per_year is None

    def test_some_fields_none(self):
        """Test creating an object with some fields as None."""
        # Arrange & Act
        slope = TimeSeriesSlope(
            slope=1.5,
            intercept=None,
            r_value=0.95,
            p_value=None,
            std_err=0.1,
            slope_per_day=1.5,
            slope_per_week=None,
            slope_per_month=45.0,
            slope_per_year=None,
        )

        # Assert
        assert slope.slope == 1.5
        assert slope.intercept is None
        assert slope.r_value == 0.95
        assert slope.p_value is None
        assert slope.std_err == 0.1
        assert slope.slope_per_day == 1.5
        assert slope.slope_per_week is None
        assert slope.slope_per_month == 45.0
        assert slope.slope_per_year is None

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        slope = TimeSeriesSlope(
            slope=1.5,
            intercept=100.0,
            r_value=0.95,
            p_value=0.01,
            std_err=0.1,
            slope_per_day=1.5,
            slope_per_week=10.5,
            slope_per_month=45.0,
            slope_per_year=547.5,
        )

        # Act
        result = slope.to_dict()

        # Assert
        assert result["slope"] == 1.5
        assert result["intercept"] == 100.0
        assert result["r_value"] == 0.95
        assert result["p_value"] == 0.01
        assert result["std_err"] == 0.1
        assert result["slope_per_day"] == 1.5
        assert result["slope_per_week"] == 10.5
        assert result["slope_per_month"] == 45.0
        assert result["slope_per_year"] == 547.5
