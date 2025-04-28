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

from datetime import date, datetime
from typing import Any

import pandas as pd

from levers.exceptions import ValidationError
from levers.models import Granularity

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


def get_period_range_for_grain(
    analysis_date: str | pd.Timestamp, grain: Granularity
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Convert an analysis_date plus a grain into (start_date, end_date).

    Family: period_grains
    Version: 1.0

    Args:
        analysis_date: The focal date for analysis
        grain: Time grain - one of DAY, WEEK, or MONTH

    Returns:
        Tuple of (start_date, end_date) for the specified grain

    Raises:
        ValidationError: If grain is not supported

    Notes:
        - For grain=DAY, returns (analysis_date, analysis_date)
        - For grain=WEEK, uses Monday-Sunday
        - For grain=MONTH, uses the full calendar month
    """
    try:
        dt = pd.to_datetime(analysis_date)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"Invalid analysis_date: {analysis_date}. Must be convertible to datetime.",
            invalid_fields={"analysis_date": analysis_date},
        ) from exc

    if grain not in Granularity:
        raise ValidationError(
            f"Unsupported grain '{grain}'",
            invalid_fields={"grain": grain, "valid_grains": list(Granularity)},
        )

    if grain == Granularity.DAY:
        start = dt.normalize()
        end = start
    elif grain == Granularity.WEEK:
        # Monday-based
        day_of_week = dt.isoweekday()  # Monday=1
        start = (dt - pd.Timedelta(days=(day_of_week - 1))).normalize()
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

    return start, end


def get_prior_period_range(
    start_date: pd.Timestamp, end_date: pd.Timestamp, grain: Granularity
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
    window_days: int, end_date: date | datetime | pd.Timestamp | str | None = None, include_today: bool = False
) -> tuple[date, date]:
    """
    Calculate a date range based on a window size in days.

    Family: period_grains
    Version: 1.0

    Args:
        window_days: Number of days in the window
        end_date: End date for the window (defaults to yesterday or today based on include_today)
        include_today: Whether to include today in the window calculation

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


def get_period_length_for_grain(grain: Granularity) -> int:
    """
    Get the appropriate period length in days for a given grain.

    Args:
        grain: The time grain

    Returns:
        Number of days in the period
    """
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
