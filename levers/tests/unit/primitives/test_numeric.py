"""
Unit tests for the numeric primitives.
"""

import pytest

from levers.exceptions import CalculationError, ValidationError
from levers.primitives import (
    calculate_difference,
    calculate_gap_to_target,
    calculate_percentage_difference,
    round_to_precision,
    safe_divide,
)


class TestCalculateDifference:
    """Tests for the calculate_difference function."""

    def test_positive_values(self):
        """Test with positive values."""
        # Arrange
        value = 10
        reference_value = 5
        expected = 5

        # Act
        result = calculate_difference(value, reference_value)

        # Assert
        assert result == expected

    def test_negative_values(self):
        """Test with negative values."""
        # Arrange
        value = -10
        reference_value = -5
        expected = -5

        # Act
        result = calculate_difference(value, reference_value)

        # Assert
        assert result == expected

    def test_mixed_values(self):
        """Test with mixed positive and negative values."""
        # Arrange
        value = 10
        reference_value = -5
        expected = 15

        # Act
        result = calculate_difference(value, reference_value)

        # Assert
        assert result == expected

    def test_same_values(self):
        """Test with identical values."""
        # Arrange
        value = 10
        reference_value = 10
        expected = 0

        # Act
        result = calculate_difference(value, reference_value)

        # Assert
        assert result == expected

    def test_float_values(self):
        """Test with float values."""
        # Arrange
        value = 10.5
        reference_value = 5.2
        expected = 5.3

        # Act
        result = calculate_difference(value, reference_value)

        # Assert
        assert pytest.approx(result) == expected

    def test_non_numeric_values(self):
        """Test with non-numeric values."""
        # Arrange
        value = "string"
        reference_value = 5

        # Act & Assert
        with pytest.raises(ValidationError):
            calculate_difference(value, reference_value)

        # Arrange
        value = 10
        reference_value = "string"

        # Act & Assert
        with pytest.raises(ValidationError):
            calculate_difference(value, reference_value)


class TestCalculatePercentageDifference:
    """Tests for the calculate_percentage_difference function."""

    def test_positive_values(self):
        """Test with positive values."""
        # Arrange
        value = 110
        reference_value = 100
        expected = 10.0

        # Act
        result = calculate_percentage_difference(value, reference_value)

        # Assert
        assert result == expected

    def test_negative_values(self):
        """Test with negative values."""
        # Arrange
        value = -110
        reference_value = -100
        expected = -10.0

        # Act
        result = calculate_percentage_difference(value, reference_value)

        # Assert
        assert result == expected

    def test_mixed_values(self):
        """Test with mixed positive and negative values."""
        # Arrange
        value = -100
        reference_value = 100
        expected = -200.0

        # Act
        result = calculate_percentage_difference(value, reference_value)

        # Assert
        assert result == expected

    def test_zero_reference_with_handling(self):
        """Test with zero reference value and handling enabled."""
        # Arrange
        value = 100
        reference_value = 0
        expected = 0.0

        # Act
        result = calculate_percentage_difference(value, reference_value, handle_zero_reference=True)

        # Assert
        assert result == expected

    def test_zero_reference_without_handling(self):
        """Test with zero reference value and handling disabled."""
        # Arrange
        value = 100
        reference_value = 0

        # Act & Assert
        with pytest.raises(CalculationError):
            calculate_percentage_difference(value, reference_value)

    def test_non_numeric_values(self):
        """Test with non-numeric values."""
        # Arrange
        value = "string"
        reference_value = 100

        # Act & Assert
        with pytest.raises(ValidationError):
            calculate_percentage_difference(value, reference_value)


class TestSafeDivide:
    """Tests for the safe_divide function."""

    def test_normal_division(self):
        """Test normal division case."""
        # Arrange
        numerator = 10
        denominator = 2
        expected = 5.0

        # Act
        result = safe_divide(numerator, denominator)

        # Assert
        assert result == expected

    def test_zero_denominator_with_default(self):
        """Test division by zero with default value."""
        # Arrange
        numerator = 10
        denominator = 0
        default_value = 0.0
        expected = 0.0

        # Act
        result = safe_divide(numerator, denominator, default_value)

        # Assert
        assert result == expected

    def test_zero_denominator_without_default(self):
        """Test division by zero without default value."""
        # Arrange
        numerator = 10
        denominator = 0
        expected = None

        # Act
        result = safe_divide(numerator, denominator)

        # Assert
        assert result == expected

    def test_as_percentage(self):
        """Test division with percentage conversion."""
        # Arrange
        numerator = 10
        denominator = 20
        expected = 50.0

        # Act
        result = safe_divide(numerator, denominator, as_percentage=True)

        # Assert
        assert result == expected

    def test_non_numeric_values(self):
        """Test with non-numeric values."""
        # Arrange
        numerator = "string"
        denominator = 2

        # Act & Assert
        with pytest.raises(ValidationError):
            safe_divide(numerator, denominator)


class TestRoundToPrecision:
    """Tests for the round_to_precision function."""

    def test_round_positive_value(self):
        """Test rounding a positive value."""
        # Arrange
        value = 10.1234
        precision = 2
        expected = 10.12

        # Act
        result = round_to_precision(value, precision)

        # Assert
        assert result == expected

    def test_round_negative_value(self):
        """Test rounding a negative value."""
        # Arrange
        value = -10.1234
        precision = 2
        expected = -10.12

        # Act
        result = round_to_precision(value, precision)

        # Assert
        assert result == expected

    def test_round_to_zero_precision(self):
        """Test rounding to zero precision (integer)."""
        # Arrange
        value = 10.6
        precision = 0
        expected = 11

        # Act
        result = round_to_precision(value, precision)

        # Assert
        assert result == expected

    def test_round_with_default_precision(self):
        """Test rounding with default precision."""
        # Arrange
        value = 10.1234
        expected = 10.12

        # Act
        result = round_to_precision(value)

        # Assert
        assert result == expected

    def test_non_numeric_value(self):
        """Test with non-numeric value."""
        # Arrange
        value = "string"

        # Act & Assert
        with pytest.raises(ValidationError):
            round_to_precision(value)

    def test_non_integer_precision(self):
        """Test with non-integer precision."""
        # Arrange
        value = 10.1234
        precision = "test"

        # Act & Assert
        with pytest.raises(ValidationError):
            round_to_precision(value, precision)


class TestCalculateGapToTarget:
    """Tests for the calculate_gap_to_target function."""

    def test_value_below_target(self):
        """Test when value is below target (positive gap)."""
        # Arrange
        value = 80
        target = 100
        expected = 20.0  # (100 - 80) / 100 * 100 = 20%

        # Act
        result = calculate_gap_to_target(value, target)

        # Assert
        assert result == expected

    def test_value_above_target(self):
        """Test when value is above target (negative gap)."""
        # Arrange
        value = 120
        target = 100
        expected = 20.0  # (100 - 120) / 100 * 100 = -20%

        # Act
        result = calculate_gap_to_target(value, target)

        # Assert
        assert result == expected

    def test_value_equals_target(self):
        """Test when value equals target (no gap)."""
        # Arrange
        value = 100
        target = 100
        expected = 0.0

        # Act
        result = calculate_gap_to_target(value, target)

        # Assert
        assert result == expected

    def test_zero_target_raises_error(self):
        """Test that zero target raises CalculationError."""
        # Arrange
        value = 50
        target = 0

        # Act & Assert
        with pytest.raises(CalculationError):
            calculate_gap_to_target(value, target)

    def test_zero_target_with_handle_flag(self):
        """Test zero target with handle_zero_target flag."""
        # Arrange
        value = 50
        target = 0
        expected = 0.0

        # Act
        result = calculate_gap_to_target(value, target, handle_zero_target=True)

        # Assert
        assert result == expected

    def test_non_numeric_inputs(self):
        """Test with non-numeric inputs."""
        # Arrange
        value = "string"
        target = 100

        # Act & Assert
        with pytest.raises(ValidationError):
            calculate_gap_to_target(value, target)
