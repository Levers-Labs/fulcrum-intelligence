"""
Numeric operations primitives.
=============================================================================

This module provides fundamental numeric operations and calculations that can be
used across various domains. These are general-purpose mathematical functions
that serve as building blocks for more complex analyses.

Dependencies:
  - None (standard Python)
"""

from levers.exceptions import CalculationError, ValidationError


def calculate_difference(value: float, reference_value: float) -> float:
    """
    Calculate the absolute difference between two values.

    Family: numeric
    Version: 1.0

    Args:
        value: The value to compare
        reference_value: The reference value

    Returns:
        The absolute difference between the values

    Raises:
        ValidationError: If inputs are not numeric
    """
    try:
        value = float(value)
        reference_value = float(reference_value)
        return value - reference_value
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            "Both value and reference_value must be numeric",
            {"value": value, "reference_value": reference_value},
        ) from exc


def calculate_percentage_difference(value: float, reference_value: float, handle_zero_reference: bool = False) -> float:
    """
    Calculate the percentage difference between two values.

    Family: numeric
    Version: 1.0

    Args:
        value: The value to compare
        reference_value: The reference value
        handle_zero_reference: If True, returns 0 for zero reference value instead of raising an error

    Returns:
        The percentage difference between the values

    Raises:
        ValidationError: If inputs are not numeric
        CalculationError: If reference_value is zero and handle_zero_reference is False
    """
    try:
        value = float(value)
        reference_value = float(reference_value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            "Both value and reference_value must be numeric",
            {"value": value, "reference_value": reference_value},
        ) from exc

    # Check for division by zero
    if reference_value == 0:
        if handle_zero_reference:
            return 0.0
        raise CalculationError(
            "Cannot calculate percentage difference with zero reference value",
            {"value": value, "reference_value": reference_value},
        )

    return ((value - reference_value) / abs(reference_value)) * 100.0


def safe_divide(
    numerator: float, denominator: float, default_value: float | None = None, as_percentage: bool = False
) -> float | None:
    """
    Safely divide two numbers, handling zero denominator cases.

    Family: numeric
    Version: 1.0

    Args:
        numerator: The numerator value
        denominator: The denominator value
        default_value: Value to return if denominator is zero
        as_percentage: If True, multiply the result by 100

    Returns:
        The division result, or default_value if denominator is zero

    Raises:
        ValidationError: If inputs are not numeric
    """
    # Input validation
    if not isinstance(numerator, (int, float)) or not isinstance(denominator, (int, float)):
        raise ValidationError(
            "Both numerator and denominator must be numeric",
            {"numerator": numerator, "denominator": denominator},
        )

    if denominator == 0:
        return default_value

    result = numerator / denominator
    return result * 100.0 if as_percentage else result


def round_to_precision(value: float, precision: int = 2) -> float:
    """
    Round a value to the specified precision.

    Family: numeric
    Version: 1.0

    Args:
        value: The value to round
        precision: Number of decimal places

    Returns:
        The rounded value

    Raises:
        ValidationError: If value is not numeric or precision is not an integer
    """
    if not isinstance(value, (int, float)):
        raise ValidationError("Value must be numeric", {"value": value})

    if not isinstance(precision, int):
        raise ValidationError("Precision must be an integer", {"precision": precision})

    return round(value, precision)
