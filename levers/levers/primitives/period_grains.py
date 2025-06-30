"""
Period and Grain Primitives
=============================================================================

This module provides functions for managing date periods across different granularities:
- Converting dates to period ranges
- Navigating between periods of different granularities
- Calculating date ranges for analysis windows

Dependencies:
  - pandas as pd
  - datetime
"""

from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from levers.exceptions import ValidationError
from levers.models import Granularity
from levers.models.enums import PeriodType

# Optional dictionary of grain metadata for reuse
GRAIN_META: dict[str, Any] = {
    Granularity.DAY: {"pop": "d/d", "label": "day", "delta": {"days": 1}, "eoi": "EOD", "interval": "daily"},
    Granularity.WEEK: {"pop": "w/w", "label": "week", "delta": {"weeks": 1}, "eoi": "EOW", "interval": "weekly"},
    Granularity.MONTH: {"pop": "m/m", "label": "month", "delta": {"months": 1}, "eoi": "EOM", "interval": "monthly"},
    Granularity.QUARTER: {
        "pop": "q/q",
        "label": "quarter",
        "delta": {"months": 3},
        "eoi": "EOQ",
        "interval": "quarterly",
    },
    Granularity.YEAR: {"pop": "y/y", "label": "year", "delta": {"years": 1}, "eoi": "EOY", "interval": "yearly"},
}


def get_analysis_end_date(grain: Granularity, include_today: bool = False) -> date:
    """
    Get the appropriate end date for analysis based on grain and include_today setting.

    Family: period_grains
    Version: 1.0

    Args:
        grain: The granularity level (day, week, month, etc.)
        include_today: Whether to include today in the analysis

    Returns:
        The appropriate end date for analysis

    Notes:
        - When include_today=True, returns today's date regardless of grain
        - When include_today=False, returns a date that excludes today's data:
          - For DAY: Returns yesterday
          - For WEEK: Returns the end of the previous week (Sunday)
          - For MONTH: Returns the end of the previous month
          - For QUARTER: Returns the end of the previous quarter
          - For YEAR: Returns the end of the previous year
    """
    today = date.today()

    if include_today:
        return today

    if grain == Granularity.DAY:
        # For daily grain, use yesterday
        return today - timedelta(days=1)
    elif grain == Granularity.WEEK:
        # For weekly grain, use the end of the previous week (Sunday)
        return today - timedelta(days=today.weekday() + 1)
    elif grain == Granularity.MONTH:
        # For monthly grain, use the last day of the previous month
        first_of_month = date(today.year, today.month, 1)
        return first_of_month - timedelta(days=1)
    elif grain == Granularity.QUARTER:
        # Calculate the current quarter's first month
        quarter_first_month = ((today.month - 1) // 3) * 3 + 1

        # If we're in the first month of a quarter and it's the first day, go to previous quarter
        if today.month == quarter_first_month and today.day == 1:
            # Last day of previous quarter
            if quarter_first_month == 1:  # Q1 (Jan-Mar)
                # End of Q4 previous year (Dec 31)
                return date(today.year - 1, 12, 31)
            else:
                # End of previous quarter
                prev_quarter_end_month = quarter_first_month - 1
                # Month lengths can vary, use the first of next month - 1 day
                next_month = date(today.year, prev_quarter_end_month + 1, 1)
                return next_month - timedelta(days=1)
        else:
            # We're not at the start of a quarter, so use the end of the previous quarter
            # First get the start of the current quarter
            quarter_start = date(today.year, quarter_first_month, 1)
            # Then go back one day to get the end of the previous quarter
            return quarter_start - timedelta(days=1)
    elif grain == Granularity.YEAR:
        # For yearly grain, if it's Jan 1, use previous year's end
        if today.month == 1 and today.day == 1:
            return date(today.year - 1, 12, 31)
        else:
            # Otherwise, use the end of the previous year
            return date(today.year - 1, 12, 31)


def get_period_range_for_grain(
    analysis_date: date, grain: Granularity, include_today: bool = False
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Get the period range (start,end) based on a date and grain.

    Family: period_grains
    Version: 1.0

    Args:
        analysis_date: Date to analyze
        grain: Time grain - one of DAY, WEEK, MONTH, QUARTER, or YEAR
        include_today: Whether to include today in the calculation (default False)

    Returns:
        Tuple of (start_date, end_date) as pandas Timestamps

    Raises:
        ValidationError: If analysis_date cannot be converted to a date or grain is invalid

    Notes:
        - For grain=DAY, returns a single day
        - For grain=WEEK, returns Monday-Sunday containing the analysis_date
        - For grain=MONTH, returns the full month containing the analysis_date
        - For grain=QUARTER, returns the full quarter containing the analysis_date
        - For grain=YEAR, returns the full year containing the analysis_date
        - When include_today=False, it adjusts the calculation to exclude today
    """
    try:
        dt = pd.to_datetime(analysis_date)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"Invalid analysis_date: {analysis_date}. Must be convertible to datetime.",
            invalid_fields={"analysis_date": analysis_date},
        ) from exc

    # Convert string grain to Granularity enum if needed
    if isinstance(grain, str):
        try:
            grain = Granularity(grain)
        except ValueError as exc:
            raise ValidationError(
                f"Unsupported grain '{grain}'",
                invalid_fields={"grain": grain, "valid_grains": list(Granularity)},
            ) from exc

    # Adjust for include_today=False when analysis_date is today
    today = pd.Timestamp.today().normalize()
    if not include_today and dt.normalize() == today:
        # Use our helper function to get the appropriate end date
        adjusted_date = get_analysis_end_date(grain, include_today=False)
        dt = pd.Timestamp(adjusted_date)

    # Now determine the period range based on the (potentially adjusted) date
    if grain == Granularity.DAY:
        start = dt.normalize()
        end = start
    elif grain == Granularity.WEEK:
        # Monday-based
        day_of_week = dt.isoweekday()  # Monday=1
        start = (dt - pd.Timedelta(days=(day_of_week - 1))).normalize()  # type: ignore
        end = start + pd.Timedelta(days=6)
    elif grain == Granularity.MONTH:
        start = dt.replace(day=1).normalize()
        next_month_start = start + pd.offsets.MonthBegin(1)
        end = (next_month_start - pd.Timedelta(days=1)).normalize()
    elif grain == Granularity.QUARTER:
        # Start of quarter
        quarter_month = ((dt.month - 1) // 3) * 3 + 1
        start = dt.replace(day=1, month=quarter_month).normalize()
        # End of quarter
        next_quarter = start + pd.DateOffset(months=3)
        end = (next_quarter - pd.Timedelta(days=1)).normalize()
    elif grain == Granularity.YEAR:
        start = dt.replace(day=1, month=1).normalize()
        end = dt.replace(day=31, month=12).normalize()
    else:
        raise ValidationError(
            f"Unsupported grain '{grain}'",
            invalid_fields={"grain": grain, "valid_grains": list(Granularity)},
        )

    return start, end


def get_prior_period_range(
    start_date: pd.Timestamp, end_date: pd.Timestamp, grain: Granularity | str
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Given current period start/end, return the immediately prior period.

    Family: period_grains
    Version: 1.0

    Args:
        start_date: Start date of the current period
        end_date: End date of the current period
        grain: Time grain - one of DAY, WEEK, MONTH, QUARTER, or YEAR

    Returns:
        Tuple of (prior_start_date, prior_end_date)

    Raises:
        ValidationError: If grain is not valid or dates are not Timestamps

    Notes:
        - For grain=DAY, shifts back by 1 day
        - For grain=WEEK, shifts back by 7 days
        - For grain=MONTH, shifts back by 1 month
        - For grain=QUARTER, shifts back by 3 months
        - For grain=YEAR, shifts back by 1 year
    """
    if not isinstance(start_date, pd.Timestamp) or not isinstance(end_date, pd.Timestamp):
        raise ValidationError(
            "start_date and end_date must be pandas Timestamps",
            invalid_fields={"start_date": start_date, "end_date": end_date},
        )

    # Convert string grain to Granularity enum if needed
    if isinstance(grain, str):
        try:
            grain = Granularity(grain)
        except ValueError as exc:
            raise ValidationError(
                f"Unsupported grain '{grain}'",
                invalid_fields={"grain": grain, "valid_grains": list(Granularity)},
            ) from exc

    valid_grains = {Granularity.DAY, Granularity.WEEK, Granularity.MONTH, Granularity.QUARTER, Granularity.YEAR}
    if grain not in valid_grains:
        raise ValidationError(
            f"Unsupported grain '{grain}' for prior-period calculation",
            invalid_fields={"grain": grain, "valid_grains": list(valid_grains)},
        )

    if grain == Granularity.DAY:
        prior_start = start_date - pd.Timedelta(days=1)
        prior_end = end_date - pd.Timedelta(days=1)
    elif grain == Granularity.WEEK:
        prior_start = start_date - pd.Timedelta(days=7)
        prior_end = end_date - pd.Timedelta(days=7)
    elif grain == Granularity.MONTH:
        prior_start = start_date - pd.offsets.MonthBegin(1)
        prior_end = prior_start + pd.offsets.MonthEnd(0)
    elif grain == Granularity.QUARTER:
        prior_start = start_date - pd.DateOffset(months=3)
        prior_end = prior_start + pd.DateOffset(months=3) - pd.Timedelta(days=1)
    elif grain == Granularity.YEAR:
        prior_start = start_date - pd.DateOffset(years=1)
        prior_end = prior_start + pd.DateOffset(years=1) - pd.Timedelta(days=1)

    return prior_start, prior_end


def get_prev_period_start_date(
    grain: Granularity, period_count: int, latest_start_date: date | datetime | pd.Timestamp
) -> date:
    """
    Calculate the start date of a period that is a specified number of periods before the latest start date.

    Family: period_grains
    Version: 1.0

    Args:
        grain: The granularity of the period (e.g., day, week, month, etc.)
        period_count: The number of periods to go back from the latest start date
        latest_start_date: The start date of the latest period

    Returns:
        The start date of the period that is `period_count` periods before the `latest_start_date`

    Raises:
        ValidationError: If grain is not supported or date conversion fails

    Notes:
        Uses GRAIN_META to determine the appropriate delta for each granularity
    """
    if grain not in GRAIN_META:
        raise ValidationError(
            f"Unsupported grain '{grain}'",
            invalid_fields={"grain": grain, "valid_grains": list(GRAIN_META.keys())},
        )

    try:
        if not isinstance(latest_start_date, pd.Timestamp):
            start_date_ts = pd.Timestamp(latest_start_date)
        else:
            start_date_ts = latest_start_date
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"Invalid latest_start_date: {latest_start_date}. Must be convertible to date.",
            invalid_fields={"latest_start_date": latest_start_date},
        ) from exc

    # Retrieve the delta for the specified grain from the GRAIN_META dictionary
    delta_eq = GRAIN_META[grain]["delta"]

    # Convert the delta dictionary into a pandas DateOffset object
    delta = pd.DateOffset(**delta_eq)

    # Calculate the start date of the period that is `period_count` periods before the `latest_start_date`
    result_date = (start_date_ts - period_count * delta).date()

    return result_date


def get_date_range_from_window(
    window_days: int,
    end_date: date | datetime | pd.Timestamp | str | None = None,
    include_today: bool = False,
    grain: Granularity | None = None,
) -> tuple[date, date]:
    """
    Calculate a date range based on a window size in days.

    Family: period_grains
    Version: 1.0

    Args:
        window_days: Number of days in the window
        end_date: End date for the window (defaults to calculated end date based on include_today and grain)
        include_today: Whether to include today in the window calculation
        grain: Optional granularity to use for end date calculation when include_today is False

    Returns:
        Tuple of (start_date, end_date) as date objects

    Raises:
        ValidationError: If window_days is invalid or date conversion fails
    """
    if window_days <= 0:
        raise ValidationError(
            "window_days must be positive",
            invalid_fields={"window_days": window_days, "valid_range": "greater than 0"},
        )

    # Determine end date
    if end_date is None:
        if grain is not None:
            # Use our helper function for consistent logic
            end = get_analysis_end_date(grain, include_today)
        else:
            # Default behavior if no grain specified
            end = date.today() if include_today else date.today() - pd.Timedelta(days=1)
    else:
        try:
            end = pd.Timestamp(end_date).date()
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                f"Invalid end_date: {end_date}. Must be convertible to date.",
                invalid_fields={"end_date": end_date},
            ) from exc

    # Calculate start date
    start = end - pd.Timedelta(days=window_days - 1)  # -1 because the window includes the end date

    return start, end


def get_period_length_for_grain(grain: Granularity | str) -> int:
    """
    Get the appropriate period length in days for a given grain.

    Args:
        grain: The time grain

    Returns:
        The length of the period in days

    Raises:
        ValidationError: If grain is not supported
    """
    # Convert string grain to Granularity enum if needed
    if isinstance(grain, str):
        try:
            grain = Granularity(grain)
        except ValueError as exc:
            raise ValidationError(
                f"Unsupported grain '{grain}'",
                invalid_fields={"grain": grain, "valid_grains": list(Granularity)},
            ) from exc

    if grain == Granularity.DAY:
        return 1
    elif grain == Granularity.WEEK:
        return 7
    elif grain == Granularity.MONTH:
        return 30
    elif grain == Granularity.QUARTER:
        return 90
    elif grain == Granularity.YEAR:
        return 365
    else:
        raise ValidationError(
            f"Unsupported grain '{grain}'",
            invalid_fields={"grain": grain, "valid_grains": list(Granularity)},
        )


def get_period_end_date(analysis_dt: pd.Timestamp, period: str | PeriodType) -> pd.Timestamp:
    """
    Calculate the end date for named periods relative to analysis_dt.

    Args:
        analysis_dt: The analysis timestamp
        period_name: The period name (string or PeriodType enum)

    Returns:
        The end date of the specified period as a pandas Timestamp

    Raises:
        ValueError: If period_name is not recognized
    """
    # Convert string to enum if needed
    if isinstance(period, str):
        try:
            period = PeriodType(period)
        except ValueError as err:
            raise ValueError(f"Unknown period: {period}") from err

    if period == PeriodType.END_OF_WEEK:
        return (analysis_dt + pd.offsets.Week(weekday=6)).normalize()  # Sunday
    elif period == PeriodType.END_OF_MONTH:
        return (analysis_dt + pd.offsets.MonthEnd(0)).normalize()
    elif period == PeriodType.END_OF_QUARTER:
        return (analysis_dt + pd.offsets.QuarterEnd(0)).normalize()
    elif period == PeriodType.END_OF_YEAR:
        return (analysis_dt + pd.offsets.YearEnd(0)).normalize()
    elif period == PeriodType.END_OF_NEXT_MONTH:
        return (analysis_dt + pd.offsets.MonthEnd(1)).normalize()
    else:
        raise ValueError(f"Unknown period: {period}")


def calculate_remaining_periods(current_date: pd.Timestamp, end_date: pd.Timestamp, grain: Granularity) -> int:
    """
    Calculate remaining periods count based on the grain.

    Args:
        current_date: The current date
        end_date: The end date
        grain: The grain to use for the calculation

    Returns:
        The number of remaining periods

    Raises:
        ValidationError: If the calculation fails
    """
    remaining_periods_count = 0

    try:
        if grain == Granularity.DAY:
            remaining_periods_count = (end_date - current_date).days
        elif grain == Granularity.WEEK:
            # Count weeks from next week start to target date's week
            next_week_start = (current_date + pd.offsets.Week(weekday=0) + pd.Timedelta(weeks=1)).normalize()
            if next_week_start <= end_date:
                temp_date = next_week_start
                while temp_date <= end_date:
                    remaining_periods_count += 1
                    temp_date += pd.Timedelta(weeks=1)
        elif grain == Granularity.MONTH:
            # Count months from next month start to target date's month
            next_month_start = (current_date.replace(day=1) + pd.offsets.MonthBegin(1)).normalize()
            if next_month_start <= end_date:
                temp_date = next_month_start
                while temp_date <= end_date:
                    remaining_periods_count += 1
                    temp_date = (temp_date.replace(day=1) + pd.offsets.MonthBegin(1)).normalize()
        elif grain == Granularity.QUARTER:
            # Count quarters from next quarter start to target date's quarter
            next_quarter_start = (
                current_date.replace(day=1, month=((current_date.month - 1) // 3) * 3 + 1)
            ).normalize()
            if next_quarter_start <= end_date:
                temp_date = next_quarter_start
                while temp_date <= end_date:
                    remaining_periods_count += 1
                    temp_date = (temp_date.replace(day=1, month=((temp_date.month - 1) // 3) * 3 + 1)).normalize()
        elif grain == Granularity.YEAR:
            # Count years from next year start to target date's year
            next_year_start = (current_date.replace(day=1, month=1)).normalize()
            if next_year_start <= end_date:
                temp_date = next_year_start
                while temp_date <= end_date:
                    remaining_periods_count += 1
                    temp_date = (temp_date.replace(day=1, month=1)).normalize()
    except Exception as e:
        raise ValidationError(
            f"Error calculating remaining periods count: {str(e)}",
            invalid_fields={"current_date": current_date, "end_date": end_date, "grain": grain},
        ) from e

    return max(0, remaining_periods_count)
