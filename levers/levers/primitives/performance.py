"""
Performance-related primitives.
=============================================================================

This module provides functions for analyzing metric performance against targets,
including GvA calculations, status classification, status change detection,
duration tracking, threshold proximity checking, and required growth calculation.

Dependencies:
  - pandas as pd
  - numpy as np
"""

import numpy as np
import pandas as pd

from levers.exceptions import CalculationError, ValidationError
from levers.models.common import GrowthTrend
from levers.models.patterns import MetricGVAStatus, SmoothingMethod
from levers.primitives import calculate_difference, calculate_gap_to_target


def calculate_metric_gva(
    actual_value: float, target_value: float, allow_negative_target: bool = False
) -> dict[str, float | None]:
    """
    Compute the Goal vs. Actual difference between an actual metric value and its target.

    Family: performance
    Version: 1.0

    Args:
        actual_value: The actual observed value
        target_value: The target value
        allow_negative_target: If True, negative or zero targets are allowed

    Returns:
        Dictionary with absolute difference and percentage difference
    """
    # Input validation
    if not isinstance(actual_value, (int, float)) or not isinstance(target_value, (int, float)):
        raise ValidationError(
            "Both actual_value and target_value must be numeric",
            {"actual_value": actual_value, "target_value": target_value},
        )

    abs_diff = calculate_difference(actual_value, target_value)

    # When target is not valid for percentage calculations:
    if (target_value <= 0) and not allow_negative_target:
        return {"difference": abs_diff, "percentage_difference": None}

    # Handle target == 0 (allowed) separately to avoid division by zero
    if target_value == 0:
        if actual_value == 0:
            pct = 0.0
        elif actual_value > 0:
            pct = float("inf")
        else:
            pct = float("-inf")
    else:
        try:
            pct = calculate_gap_to_target(actual_value, target_value)
        except CalculationError:
            pct = None

    return {"difference": abs_diff, "percentage_difference": pct}


def calculate_historical_gva(
    df_actual: pd.DataFrame,
    df_target: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "value",
    allow_negative_target: bool = False,
) -> pd.DataFrame:
    """
    Compute historical Goal vs. Actual differences over a time series.

    Family: performance
    Version: 1.0

    Args:
        df_actual: DataFrame with columns [date_col, value_col] for actuals
        df_target: DataFrame with columns [date_col, value_col] for targets
        date_col: Name of the datetime column used for merging
        value_col: Name of the numeric column representing metric values
        allow_negative_target: If True, negative or zero targets are allowed

    Returns:
        Merged DataFrame with columns [date, value_actual, value_target, difference, percentage_difference]
    """
    # Validate inputs
    if date_col not in df_actual.columns or value_col not in df_actual.columns:
        raise ValidationError(f"df_actual must contain columns '{date_col}' and '{value_col}'", {"field": "df_actual"})
    if date_col not in df_target.columns or value_col not in df_target.columns:
        raise ValidationError(f"df_target must contain columns '{date_col}' and '{value_col}'", {"field": "df_target"})

    # Convert date columns to datetime for proper joining
    df_actual_copy = df_actual.copy()
    df_target_copy = df_target.copy()

    df_actual_copy[date_col] = pd.to_datetime(df_actual_copy[date_col])
    df_target_copy[date_col] = pd.to_datetime(df_target_copy[date_col])

    merged = pd.merge(
        df_actual_copy[[date_col, value_col]],
        df_target_copy[[date_col, value_col]],
        on=date_col,
        how="left",
        suffixes=("_actual", "_target"),
    )

    # Compute absolute difference
    merged["difference"] = merged[f"{value_col}_actual"] - merged[f"{value_col}_target"]

    # Use vectorized computation for percentage_difference:
    tgt = merged[f"{value_col}_target"]
    act = merged[f"{value_col}_actual"]

    # Define a mask where percentage_difference is undefined:
    invalid = (tgt <= 0) & (~allow_negative_target)

    # Avoid division by zero if allowed:
    safe_tgt = tgt.replace({0: np.nan})

    merged["percentage_difference"] = np.where(
        invalid,
        np.nan,
        np.where(
            tgt == 0,
            np.where(act == 0, 0.0, np.where(act > 0, float("inf"), float("-inf"))),
            (merged["difference"] / safe_tgt.abs()) * 100.0,
        ),
    )

    # Classify status
    merged["status"] = merged.apply(
        lambda row: classify_metric_status(
            row[f"{value_col}_actual"], row[f"{value_col}_target"], allow_negative_target=allow_negative_target
        ),
        axis=1,
    )

    return merged


def classify_metric_status(
    actual_value: float,
    target_value: float,
    threshold_ratio: float = 0.05,
    allow_negative_target: bool = False,
    status_if_no_target: MetricGVAStatus = MetricGVAStatus.NO_TARGET,
) -> MetricGVAStatus:
    """
    Classify a metric as 'on_track' or 'off_track' given a threshold ratio.

    Family: performance
    Version: 1.0

    Args:
        actual_value: The actual observed value
        target_value: The target value
        threshold_ratio: Allowable deviation fraction (e.g., 0.05 for 5%)
        allow_negative_target: Whether to handle negative targets
        status_if_no_target: Status to return when no valid target exists

    Returns:
        Status classification (on_track, off_track, or no_target)

    Notes:
        For positive targets:
            on_track if actual_value >= target_value * (1 - threshold_ratio)
        For negative targets (when allowed):
            on_track if actual_value <= target_value * (1 + threshold_ratio)
            (i.e. less deviation in the negative direction)
    """
    # Input validation
    if not isinstance(actual_value, (int, float)) or pd.isna(actual_value):
        return status_if_no_target

    if target_value is None or np.isnan(target_value):
        return status_if_no_target

    if (target_value <= 0) and (not allow_negative_target):
        return status_if_no_target

    if target_value > 0:
        # For positive targets, we're on track if we're within threshold_ratio below target
        cutoff = target_value * (1.0 - threshold_ratio)
        return MetricGVAStatus.ON_TRACK if actual_value >= cutoff else MetricGVAStatus.OFF_TRACK
    else:
        # For negative targets, we're on track if we're within threshold_ratio above target
        # e.g. if target is -100, threshold_ratio=0.05 => cutoff=-100*(1+0.05)=-105
        cutoff = target_value * (1.0 + threshold_ratio)
        return MetricGVAStatus.ON_TRACK if actual_value <= cutoff else MetricGVAStatus.OFF_TRACK


def detect_status_changes(
    df: pd.DataFrame, status_col: str = "status", sort_by_date: str | None = None
) -> pd.DataFrame:
    """
    Identify rows where the status value changes from the previous row.

    Family: performance
    Version: 1.0

    Args:
        df: DataFrame containing a column with status values
        status_col: Name of the status column
        sort_by_date: If provided, the DataFrame is sorted by this column before detection

    Returns:
        DataFrame with two new columns: 'prev_status' and 'status_flip'
    """
    # Input validation
    if status_col not in df.columns:
        raise ValidationError(f"Column '{status_col}' not found in DataFrame", {"field": status_col})

    out_df = df.copy()

    if sort_by_date:
        if sort_by_date not in out_df.columns:
            raise ValidationError(f"Column '{sort_by_date}' not found in DataFrame", {"field": sort_by_date})
        out_df[sort_by_date] = pd.to_datetime(out_df[sort_by_date])
        out_df.sort_values(sort_by_date, inplace=True)

    out_df["prev_status"] = out_df[status_col].shift(1)
    out_df["status_flip"] = (out_df[status_col] != out_df["prev_status"]) & out_df["prev_status"].notna()

    return out_df


def track_status_durations(df: pd.DataFrame, status_col: str = "status", date_col: str | None = None) -> pd.DataFrame:
    """
    Compute consecutive runs of identical statuses.

    Family: performance
    Version: 1.0

    Args:
        df: DataFrame with a status column and optionally a date column
        status_col: Column name holding status values
        date_col: If provided, must be datetime-like; durations are computed in days

    Returns:
        DataFrame with one row per consecutive status run
    """
    # Input validation
    if status_col not in df.columns:
        raise ValidationError(f"Column '{status_col}' not found in DataFrame", {"field": status_col})
    if date_col and date_col not in df.columns:
        raise ValidationError(f"Column '{date_col}' not found in DataFrame", {"field": date_col})

    # Handle empty dataframe
    if df.empty:
        cols = ["status", "start_index", "end_index", "run_length"]
        if date_col:
            cols.extend(["start_date", "end_date", "duration_days"])
        return pd.DataFrame(columns=cols)

    # Ensure a clean, zero-indexed DataFrame
    df_clean = df.reset_index().copy()

    # Identify runs by grouping on changes in status
    df_clean["group"] = (df_clean[status_col] != df_clean[status_col].shift(1)).cumsum()

    runs = df_clean.groupby("group", as_index=False).agg(
        status=(status_col, "first"),
        start_index=("index", "first"),
        end_index=("index", "last"),
        run_length=("group", "count"),
    )

    if date_col:
        # Ensure date_col is datetime
        df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors="coerce")
        date_groups = df_clean.groupby("group")[date_col]
        runs["start_date"] = date_groups.first().values
        runs["end_date"] = date_groups.last().values
        runs["duration_days"] = (runs["end_date"] - runs["start_date"]).dt.days + 1

    return runs.drop(columns="group")


def monitor_threshold_proximity(
    val: float, target: float, margin: float = 0.05, allow_negative_target: bool = False
) -> bool:
    """
    Check if 'val' is within +/- margin fraction of 'target'.

    Family: performance
    Version: 1.0

    Args:
        val: The observed value
        target: The target value
        margin: Fractional margin (e.g., 0.05 for 5%)
        allow_negative_target: If False and target <= 0, returns False

    Returns:
        True if value is within margin of target, False otherwise
    """
    # Input validation
    if not isinstance(val, (int, float)) or pd.isna(val):
        return False

    if target is None or np.isnan(target):
        return False

    if (target <= 0) and not allow_negative_target:
        return False

    # Special-case when target is zero and allowed
    if target == 0:
        return val == 0

    return abs(val - target) / abs(target) <= margin


def calculate_required_growth(
    current_value: float, target_value: float, periods_remaining: int, allow_negative: bool = False
) -> float:
    """
    Determine the compound per-period growth rate needed to reach target from current value.

    Family: performance
    Version: 1.0

    Args:
        current_value: Current metric value
        target_value: Target metric value
        periods_remaining: Number of periods over which growth occurs
        allow_negative: Whether to allow negative or zero values in the computation

    Returns:
        The per-period compound growth rate (e.g., 0.02 for 2% growth)

    Notes:
        For positive current and target values:
            rate = (target_value / current_value)^(1/periods_left) - 1.
        When negative values are allowed, the function attempts a ratio-based approach with absolute values.

    Raises:
        ValidationError: If periods_remaining is not positive, or if current_value or target_value are not valid numbers
        CalculationError: If the growth rate cannot be calculated due to incompatible values
    """
    # Input validation
    if periods_remaining <= 0:
        raise ValidationError("Periods remaining must be positive", {"field": "periods_remaining"})

    if not isinstance(current_value, (int, float)) or pd.isna(current_value):
        raise ValidationError("Current value must be a valid number", {"field": "current_value"})

    if not isinstance(target_value, (int, float)) or pd.isna(target_value):
        raise ValidationError("Target value must be a valid number", {"field": "target_value"})

    # Standard domain checks for positive values
    if not allow_negative:
        if current_value <= 0 or target_value <= 0:
            raise ValidationError(
                "Values must be positive for growth calculation",
                {"current_value": current_value, "target_value": target_value},
            )
    else:
        # For negative-to-negative growth, work in absolute terms
        if current_value < 0 and target_value < 0:
            current_value, target_value = abs(current_value), abs(target_value)
        # If signs differ or one value is zero, the calculation is undefined
        elif current_value == 0 or target_value == 0 or (current_value * target_value < 0):
            raise CalculationError(
                "Cannot calculate growth rate with incompatible values",
                {"current_value": current_value, "target_value": target_value},
            )

    ratio = target_value / current_value
    if ratio <= 0:
        raise CalculationError(
            "Cannot calculate growth rate with negative ratio",
            {"current_value": current_value, "target_value": target_value},
        )

    rate = ratio ** (1.0 / periods_remaining) - 1.0
    return rate


def classify_growth_trend(
    growth_rates: list[float], stability_threshold: float = 0.01, acceleration_threshold: float = 0.02
) -> str:
    """
    Classify the growth pattern as stable, accelerating, or decelerating.

    Family: performance
    Version: 1.0

    Args:
        growth_rates: List of sequential growth rates
        stability_threshold: Maximum deviation for "stable" classification (1%)
        acceleration_threshold: Minimum change for "accelerating" or "decelerating" (2%)

    Returns:
        Classification from GrowthTrend enum

    Raises:
        ValidationError: If growth_rates is empty or has fewer than 2 elements
    """
    # Handle empty or too short lists
    if not growth_rates or len(growth_rates) < 2:
        raise ValidationError(
            "Insufficient data to classify growth trend",
            {"field": "growth_rates", "min_length": 2, "actual_length": len(growth_rates) if growth_rates else 0},
        )

    # Calculate changes between consecutive growth rates
    changes = [growth_rates[i] - growth_rates[i - 1] for i in range(1, len(growth_rates))]

    # Check if all changes are small (stable)
    if all(abs(change) <= stability_threshold for change in changes):
        return GrowthTrend.STABLE

    # Check if all significant changes are positive (accelerating)
    if all(change >= -stability_threshold for change in changes) and any(
        change >= acceleration_threshold for change in changes
    ):
        return GrowthTrend.ACCELERATING

    # Check if all significant changes are negative (decelerating)
    if all(change <= stability_threshold for change in changes) and any(
        change <= -acceleration_threshold for change in changes
    ):
        return GrowthTrend.DECELERATING

    # Otherwise, pattern is volatile
    return GrowthTrend.VOLATILE


def calculate_moving_target(
    current_value: float,
    final_target: float,
    periods_total: int,
    periods_elapsed: int,
    smoothing_method: str = SmoothingMethod.LINEAR,
) -> float:
    """
    Calculate interim targets along a trajectory from current value to final target.

    Family: performance
    Version: 1.0

    Args:
        current_value: Starting value
        final_target: Final target value to reach
        periods_total: Total number of periods to reach target
        periods_elapsed: Number of periods already elapsed
        smoothing_method: Method for calculating interim targets (from SmoothingMethod enum)

    Returns:
        Target value for the current period
    """
    # Input validation
    if periods_total <= 0:
        raise ValidationError("Total periods must be positive", {"field": "periods_total"})
    if periods_elapsed < 0 or periods_elapsed >= periods_total:
        raise ValidationError("Periods elapsed must be between 0 and periods_total-1", {"field": "periods_elapsed"})

    # Calculate total change needed
    total_change = final_target - current_value

    if smoothing_method == SmoothingMethod.LINEAR:
        # Linear: equal changes each period
        change_per_period = total_change / periods_total
        interim_target = current_value + (change_per_period * periods_elapsed)

    elif smoothing_method == SmoothingMethod.FRONT_LOADED:
        # Front-loaded: changes decrease over time
        # Use a simple exponential decay formula
        if periods_elapsed == 0:
            interim_target = current_value
        else:
            progress = 1 - (0.8**periods_elapsed)  # Adjust 0.8 to control front-loading
            interim_target = current_value + (total_change * progress)

    elif smoothing_method == SmoothingMethod.BACK_LOADED:
        # Back-loaded: changes increase over time
        # Use a simple exponential growth formula
        if periods_elapsed == 0:
            interim_target = current_value
        else:
            progress = (periods_elapsed / periods_total) ** 2  # Square to back-load
            interim_target = current_value + (total_change * progress)

    else:
        raise ValidationError(f"Unknown smoothing method: {smoothing_method}", {"field": "smoothing_method"})

    return interim_target
