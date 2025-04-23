"""
Utility functions for the semantic manager module.
"""

from datetime import date
from typing import Any, Dict, List

from commons.models.enums import Granularity
from commons.utilities.grain_utils import GrainPeriodCalculator
from query_manager.semantic_manager.models import TargetCalculationType
from query_manager.semantic_manager.schemas import TargetCalculationResponse, TargetResponse


def calculate_targets(
    current_value: float,
    start_date: date,
    end_date: date,
    grain: Granularity,
    calculation_type: TargetCalculationType,
    target_value: float | None = None,
    growth_percentage: float | None = None,
    pop_growth_percentage: float | None = None,
) -> list[TargetCalculationResponse]:
    """
    Calculate target values between dates using different calculation methods.

    Args:
        current_value: Current value on which growth calculations are based
        start_date: Start date for the target period (inclusive)
        end_date: End date for the target period (inclusive)
        grain: Time granularity to use for date generation
        calculation_type: Method used for calculation
        target_value: Target value used for 'value' calculation type
        growth_percentage: Growth percentage used for 'growth' calculation type
        pop_growth_percentage: Period on period growth percentage for 'pop_growth' calculation type

    Returns:
        List of calculation entries for each date
    """
    # Get dates using the utility function
    dates = GrainPeriodCalculator.get_dates_for_range(grain, start_date, end_date)
    if not dates:
        return []

    # Initialize results list and get number of periods
    results = []
    num_periods = len(dates)

    # Calculate values based on calculation type
    if calculation_type == TargetCalculationType.VALUE and target_value is not None:
        # VALUE method: Distribute target values appropriately
        if num_periods == 1:
            # Single period - just use the target value
            results = [
                TargetCalculationResponse(
                    date=dates[0],
                    value=round(target_value, 2),
                    growth_percentage=round(((target_value / current_value - 1) * 100) if current_value != 0 else 0, 2),
                    pop_growth_percentage=0.0,
                )
            ]
        else:
            # Multiple periods - distribute evenly from first period to target
            step = (target_value - current_value) / num_periods

            for i, current_date in enumerate(dates):
                # Calculate value for this period - don't start at current_value
                # Instead, move immediately toward target from period 1
                value = current_value + step * (i + 1)

                # Calculate percentages
                prev_value = results[-1].value if i > 0 else current_value
                growth_pct = ((value / current_value - 1) * 100) if current_value != 0 else 0
                pop_growth_pct = ((value / prev_value - 1) * 100) if prev_value != 0 else 0

                # Add to results
                results.append(
                    TargetCalculationResponse(
                        date=current_date,
                        value=round(value, 2),
                        growth_percentage=round(growth_pct, 2),
                        pop_growth_percentage=round(pop_growth_pct, 2),
                    )
                )

    elif calculation_type == TargetCalculationType.GROWTH and growth_percentage is not None:
        # Handle negative current values specially for growth calculation
        is_negative = current_value < 0
        abs_current = abs(current_value)

        for i, current_date in enumerate(dates):
            if is_negative:
                # For negative values, we always want to grow toward positive
                # Compute position in growth curve (0 to 1)
                position = i / (num_periods - 1) if num_periods > 1 else 1

                # Calculate percentage of distance moved toward positive
                improvement_factor = 1 + (growth_percentage / 100)

                # Apply improvement to move toward zero/positive
                if improvement_factor > 1:  # positive growth
                    # Reduce the negative value
                    value = max(
                        -abs_current * (1 - position * (improvement_factor - 1)), current_value / improvement_factor
                    )
                else:  # negative growth (unusual for targets)
                    # Increase the negative value
                    value = -abs_current * (2 - improvement_factor**position)
            else:
                # For positive values, normal compound growth
                position = i / (num_periods - 1) if num_periods > 1 else 1
                growth_factor = (1 + growth_percentage / 100) ** position
                value = current_value * growth_factor

            # Calculate percentages
            prev_value = results[-1].value if i > 0 else current_value
            growth_pct = ((value / current_value - 1) * 100) if current_value != 0 else 0
            pop_growth_pct = ((value / prev_value - 1) * 100) if prev_value != 0 else 0

            # Add to results
            results.append(
                TargetCalculationResponse(
                    date=current_date,
                    value=round(value, 2),
                    growth_percentage=round(growth_pct, 2),
                    pop_growth_percentage=round(pop_growth_pct, 2),
                )
            )

    elif calculation_type == TargetCalculationType.POP_GROWTH and pop_growth_percentage is not None:
        # POP_GROWTH method: Apply consistent period-on-period growth
        pop_factor = 1 + (pop_growth_percentage / 100)

        # Handle negative values specially
        is_negative = current_value < 0

        for i, current_date in enumerate(dates):
            if i == 0:
                if is_negative:
                    # For negative values with positive growth, move toward zero
                    value = current_value / pop_factor if pop_factor > 1 else current_value * pop_factor
                else:
                    # For positive values or zero, regular growth
                    value = current_value * pop_factor
            else:
                prev_value = results[-1].value
                if prev_value < 0 and pop_factor > 1:
                    # Continue reducing the negative value
                    value = prev_value / pop_factor
                else:
                    # Normal period-on-period growth
                    value = prev_value * pop_factor

            # Calculate percentages
            prev_value = results[-1].value if i > 0 else current_value
            growth_pct = ((value / current_value - 1) * 100) if current_value != 0 else 0

            # Add to results
            results.append(
                TargetCalculationResponse(
                    date=current_date,
                    value=round(value, 2),
                    growth_percentage=round(growth_pct, 2),
                    pop_growth_percentage=round(pop_growth_percentage, 2),
                )
            )

    return results


def calculate_pop_growth_percentages(targets: list[TargetResponse]) -> dict[int, float]:
    """
    Calculate period-on-period growth percentages for a list of targets.

    Args:
        targets: List of target responses

    Returns:
        Dictionary mapping target IDs to their PoP growth percentages
    """
    if not targets:
        return {}

    # Sort targets by date to ensure correct calculations
    sorted_targets = sorted(targets, key=lambda t: t.target_date)

    # Calculate period-on-period growth for each target
    pop_growth = {}

    for i, target in enumerate(sorted_targets):
        if i == 0:
            # First target has no previous for comparison
            pop_growth[target.id] = 0.0
        else:
            # Calculate growth compared to previous target
            prev_value = sorted_targets[i - 1].target_value
            curr_value = target.target_value
            growth_pct = ((curr_value / prev_value - 1) * 100) if prev_value != 0 else 0
            pop_growth[target.id] = round(growth_pct, 1)

    return pop_growth


def calculate_growth_summary(targets: list[TargetResponse]) -> dict[str, float]:
    """
    Calculate growth summary metrics for a list of targets.

    Args:
        targets: List of target responses

    Returns:
        Dictionary with total and average growth percentages
    """
    if not targets or len(targets) < 2:
        return {"total_growth_percentage": 0.0, "avg_pop_growth_percentage": 0.0}

    # Sort targets by date
    sorted_targets = sorted(targets, key=lambda t: t.target_date)

    # Calculate total growth (first to last)
    first_value = sorted_targets[0].target_value
    last_value = sorted_targets[-1].target_value
    total_growth = ((last_value / first_value - 1) * 100) if first_value != 0 else 0

    # Calculate period-on-period growth percentages
    pop_values = []
    for i in range(1, len(sorted_targets)):
        prev_value = sorted_targets[i - 1].target_value
        curr_value = sorted_targets[i].target_value
        if prev_value != 0:
            pop_values.append((curr_value / prev_value - 1) * 100)

    # Calculate average pop growth
    avg_pop_growth = sum(pop_values) / len(pop_values) if pop_values else 0

    return {"total_growth_percentage": round(total_growth, 1), "avg_pop_growth_percentage": round(avg_pop_growth, 1)}


def add_growth_percentages(db_results: list[Any]) -> list[TargetResponse]:
    """
    Add growth_percentage and pop_growth_percentage to target results.

    Args:
        db_results: List of MetricTarget database objects

    Returns:
        List of TargetResponse objects with growth percentages added
    """
    if not db_results:
        return []

    # Group targets by metric_id and grain
    grouped_targets = {}
    for target in db_results:
        key = (target.metric_id, target.grain)
        if key not in grouped_targets:
            grouped_targets[key] = []
        grouped_targets[key].append(target)

    # Create response items with growth calculations
    response_items = []

    for (metric_id, grain), targets in grouped_targets.items():
        # Sort by date
        sorted_targets = sorted(targets, key=lambda t: t.target_date)

        # Find first non-zero value (or use first value if all are zero)
        first_non_zero_idx = 0
        first_non_zero_value = sorted_targets[0].target_value

        for i, t in enumerate(sorted_targets):
            if t.target_value != 0:
                first_non_zero_idx = i
                first_non_zero_value = t.target_value
                break

        for i, target in enumerate(sorted_targets):
            # Set default values
            growth_pct = 0.0
            pop_growth_pct = 0.0

            # Calculate growth from first non-zero target
            if i == first_non_zero_idx:
                # This is the first non-zero entry
                # If there were zeros before it, show this as special case
                if first_non_zero_idx > 0:
                    # There were zeros before this entry, consider it as infinite growth
                    growth_pct = 100.0  # Use 100% to indicate going from 0 to non-zero
            elif i > first_non_zero_idx and first_non_zero_value != 0:
                # Normal growth calculation for entries after the first non-zero
                try:
                    growth_pct = ((target.target_value / first_non_zero_value) - 1) * 100
                    # Ensure it's a valid float
                    if not (growth_pct == growth_pct and abs(growth_pct) < float("inf")):
                        growth_pct = 0.0
                except:
                    growth_pct = 0.0

            # Calculate period-on-period growth (if possible)
            if i > 0:
                prev_value = sorted_targets[i - 1].target_value
                curr_value = target.target_value

                if prev_value != 0 and curr_value != 0:
                    try:
                        pop_growth_pct = ((curr_value / prev_value) - 1) * 100
                        # Ensure it's a valid float
                        if not (pop_growth_pct == pop_growth_pct and abs(pop_growth_pct) < float("inf")):
                            pop_growth_pct = 0.0
                    except:
                        pop_growth_pct = 0.0
                elif prev_value == 0 and curr_value != 0:
                    # Special case: previous is zero, current is non-zero
                    pop_growth_pct = 100.0  # Represent as 100% growth

            # Create response with calculated values
            response_items.append(
                TargetResponse(
                    id=target.id,
                    metric_id=target.metric_id,
                    grain=target.grain,
                    target_date=target.target_date,
                    target_value=target.target_value,
                    target_upper_bound=target.target_upper_bound,
                    target_lower_bound=target.target_lower_bound,
                    yellow_buffer=target.yellow_buffer,
                    red_buffer=target.red_buffer,
                    growth_percentage=round(growth_pct, 1),
                    pop_growth_percentage=round(pop_growth_pct, 1),
                )
            )

    # Sort to match original order
    id_order = {t.id: i for i, t in enumerate(db_results)}
    response_items.sort(key=lambda t: id_order.get(t.id, 0))

    return response_items
