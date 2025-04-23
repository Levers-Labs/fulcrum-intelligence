"""
Utility classes for target calculations.
"""

from datetime import date
from typing import Any

from commons.models.enums import Granularity
from commons.utilities.grain_utils import GrainPeriodCalculator
from query_manager.semantic_manager.models import TargetCalculationType
from query_manager.semantic_manager.schemas import TargetCalculationResponse, TargetResponse


class TargetCalculator:
    """Handles all target calculation operations."""

    @classmethod
    def calculate_targets(
        cls,
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

        num_periods = len(dates)

        # Call the appropriate calculation method based on type
        if calculation_type == TargetCalculationType.VALUE and target_value is not None:
            return cls._calculate_by_value(dates, current_value, target_value, num_periods)
        elif calculation_type == TargetCalculationType.GROWTH and growth_percentage is not None:
            return cls._calculate_by_growth(dates, current_value, growth_percentage, num_periods)
        elif calculation_type == TargetCalculationType.POP_GROWTH and pop_growth_percentage is not None:
            return cls._calculate_by_pop_growth(dates, current_value, pop_growth_percentage)

        return []

    @classmethod
    def add_growth_stats_to_targets(cls, targets: list[Any]) -> list[TargetResponse]:
        """
        Add growth_percentage and pop_growth_percentage to target results.

        Args:
            targets: List of MetricTarget database objects

        Returns:
            List of TargetResponse objects with additional growth stats
        """
        if not targets:
            return []

        # Group targets by (metric_id, grain) and sort by date within groups
        grouped_targets = cls._group_targets_by_metric_and_grain(targets)

        # Create lookup for preserving original order
        id_order = {t.id: i for i, t in enumerate(targets)}
        updated_targets = []

        for targets in grouped_targets.values():
            sorted_targets = sorted(targets, key=lambda t: t.target_date)

            # Find first non-zero reference value
            initial_reference_index = next((i for i, t in enumerate(sorted_targets) if t.target_value != 0), 0)
            initial_reference_value = sorted_targets[initial_reference_index].target_value

            for i, entry in enumerate(sorted_targets):
                # Calculate growth percentages
                growth_pct = cls._calc_growth_pct(
                    i, initial_reference_index, entry.target_value, initial_reference_value
                )
                pop_growth_pct = cls._calc_pop_growth_pct(i, sorted_targets, entry.target_value)

                # Create response with growth stats
                updated_targets.append(
                    TargetResponse(
                        **entry.model_dump(exclude={"tenant_id", "created_at", "updated_at"}),
                        growth_percentage=round(growth_pct, 1),
                        pop_growth_percentage=round(pop_growth_pct, 1),
                    )
                )

        # Restore original order
        updated_targets.sort(key=lambda t: id_order[t.id])
        return updated_targets

    # Private calculation methods

    @classmethod
    def _calculate_by_value(
        cls, dates: list[date], current_value: float, target_value: float, num_periods: int
    ) -> list[TargetCalculationResponse]:
        """Calculate targets using the VALUE method."""
        results: list = []

        if num_periods == 1:
            # Single period - just use the target value
            results = [
                TargetCalculationResponse(
                    date=dates[0],
                    value=round(target_value, 2),
                    growth_percentage=round(cls._calc_relative_growth(target_value, current_value), 2),
                    pop_growth_percentage=0.0,
                )
            ]
        else:
            # Multiple periods - distribute evenly
            step = (target_value - current_value) / num_periods

            for i, current_date in enumerate(dates):
                value = current_value + step * (i + 1)
                prev_value = results[-1].value if i > 0 else current_value

                results.append(
                    TargetCalculationResponse(
                        date=current_date,
                        value=round(value, 2),
                        growth_percentage=round(cls._calc_relative_growth(value, current_value), 2),
                        pop_growth_percentage=round(cls._calc_relative_growth(value, prev_value), 2),
                    )
                )

        return results

    @classmethod
    def _calculate_by_growth(
        cls, dates: list[date], current_value: float, growth_percentage: float, num_periods: int
    ) -> list[TargetCalculationResponse]:
        """Calculate targets using the GROWTH method."""
        results: list = []
        is_negative = current_value < 0
        abs_current = abs(current_value)

        for i, current_date in enumerate(dates):
            if is_negative:
                value = cls._calculate_negative_growth(i, num_periods, current_value, abs_current, growth_percentage)
            else:
                position = i / (num_periods - 1) if num_periods > 1 else 1
                growth_factor = (1 + growth_percentage / 100) ** position
                value = current_value * growth_factor

            prev_value = results[-1].value if i > 0 else current_value

            results.append(
                TargetCalculationResponse(
                    date=current_date,
                    value=round(value, 2),
                    growth_percentage=round(cls._calc_relative_growth(value, current_value), 2),
                    pop_growth_percentage=round(cls._calc_relative_growth(value, prev_value), 2),
                )
            )

        return results

    @classmethod
    def _calculate_by_pop_growth(
        cls, dates: list[date], current_value: float, pop_growth_percentage: float
    ) -> list[TargetCalculationResponse]:
        """Calculate targets using the POP_GROWTH method."""
        results: list = []
        pop_factor = 1 + (pop_growth_percentage / 100)
        is_negative = current_value < 0

        for i, current_date in enumerate(dates):
            if i == 0:
                value = cls._apply_pop_growth_to_first(current_value, pop_factor, is_negative)
            else:
                prev_value = results[-1].value
                value = cls._apply_pop_growth_to_subsequent(prev_value, pop_factor)

            prev_value = results[-1].value if i > 0 else current_value

            results.append(
                TargetCalculationResponse(
                    date=current_date,
                    value=round(value, 2),
                    growth_percentage=round(cls._calc_relative_growth(value, current_value), 2),
                    pop_growth_percentage=round(pop_growth_percentage, 2),
                )
            )

        return results

    # Helper methods

    @staticmethod
    def _calculate_negative_growth(
        i: int, num_periods: int, current_value: float, abs_current: float, growth_percentage: float
    ) -> float:
        """Calculate growth for negative values."""
        position = i / (num_periods - 1) if num_periods > 1 else 1
        improvement_factor = 1 + (growth_percentage / 100)

        if improvement_factor > 1:  # positive growth
            # Reduce the negative value
            return max(-abs_current * (1 - position * (improvement_factor - 1)), current_value / improvement_factor)
        else:  # negative growth
            # Increase the negative value
            return -abs_current * (2 - improvement_factor**position)

    @staticmethod
    def _apply_pop_growth_to_first(current_value: float, pop_factor: float, is_negative: bool) -> float:
        """Apply period-on-period growth to the first value."""
        if is_negative:
            # For negative values with positive growth, move toward zero
            return current_value / pop_factor if pop_factor > 1 else current_value * pop_factor
        else:
            # For positive values or zero, regular growth
            return current_value * pop_factor

    @staticmethod
    def _apply_pop_growth_to_subsequent(prev_value: float, pop_factor: float) -> float:
        """Apply period-on-period growth to subsequent values."""
        if prev_value < 0 and pop_factor > 1:
            # Continue reducing the negative value
            return prev_value / pop_factor
        else:
            # Normal period-on-period growth
            return prev_value * pop_factor

    @staticmethod
    def _group_targets_by_metric_and_grain(targets: list[Any]) -> dict[tuple[str, Granularity], list[Any]]:
        """Group targets by metric_id and grain."""
        grouped_targets: dict = {}
        for target in targets:
            key = (target.metric_id, target.grain)
            grouped_targets.setdefault(key, []).append(target)
        return grouped_targets

    @classmethod
    def _calc_growth_pct(cls, i: int, ref_idx: int, curr_val: float, ref_val: float) -> float:
        """Calculate compounding growth percentage."""
        if i == ref_idx and ref_idx > 0:
            return 100.0
        if i > ref_idx and ref_val != 0:
            growth = cls._calc_relative_growth(curr_val, ref_val)
            return growth if cls._is_valid_float(growth) else 0.0
        return 0.0

    @classmethod
    def _calc_pop_growth_pct(cls, i: int, targets: list[Any], curr_val: float) -> float:
        """Calculate period-over-period growth percentage."""
        if i == 0:
            return 0.0
        prev_val = targets[i - 1].target_value
        if prev_val == 0:
            return 100.0 if curr_val != 0 else 0.0
        pop_growth = cls._calc_relative_growth(curr_val, prev_val)
        return pop_growth if cls._is_valid_float(pop_growth) else 0.0

    @staticmethod
    def _calc_relative_growth(new_val: float, reference_val: float) -> float:
        """Calculate relative growth percentage between two values."""
        if reference_val == 0:
            return 0.0
        return ((new_val / reference_val) - 1) * 100

    @staticmethod
    def _is_valid_float(value: float) -> bool:
        """Check if a float value is valid (not NaN, not infinite)."""
        return value == value and abs(value) < float("inf")
