"""
Trend Analysis Primitives
=============================================================================

This module provides functions for analyzing trends in time series data:
- Trend detection and classification
- Record high/low detection 
- Anomaly/exception detection
- Performance plateau identification

Dependencies:
- pandas as pd
- numpy as np
- scipy.stats for linear regression
"""

from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import linregress

from levers.exceptions import InsufficientDataError, ValidationError
from levers.models.patterns import AnomalyDetectionMethod, TrendExceptionType, TrendType
from levers.primitives.numeric import calculate_difference, calculate_percentage_difference
from levers.primitives.time_series import validate_date_sorted


def analyze_metric_trend(
    df: pd.DataFrame, value_col: str = "value", date_col: str = "date", window_size: int = 7
) -> dict[str, Any]:
    """
    Analyze the trend direction in a time series.

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        value_col: Column name containing values
        date_col: Column name containing dates
        window_size: Number of most recent periods to consider for trend

    Returns:
        Dictionary with trend_direction, trend_slope, trend_confidence, recent_direction, etc.
    """
    # Input validation
    if date_col not in df.columns:
        raise ValidationError(f"Column '{date_col}' not found in DataFrame", invalid_fields={"date_col": date_col})
    if value_col not in df.columns:
        raise ValidationError(f"Column '{value_col}' not found in DataFrame", invalid_fields={"value_col": value_col})

    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    if len(df_sorted) < 2:
        return {
            "trend_direction": None,
            "trend_slope": None,
            "trend_confidence": None,
            "normalized_slope": None,
            "recent_direction": None,
            "is_accelerating": False,
            "is_plateaued": False,
        }

    # Full dataset trend analysis
    y = df_sorted[value_col].values
    x = np.arange(len(y))

    # Handle missing values
    mask = ~np.isnan(y)
    if sum(mask) < 2:
        return {
            "trend_direction": None,
            "trend_slope": None,
            "trend_confidence": None,
            "normalized_slope": None,
            "recent_direction": None,
            "is_accelerating": False,
            "is_plateaued": False,
        }

    # Calculate trend using linregress (matching reference exactly)
    slope, intercept, r_value, p_value, std_err = linregress(x[mask], y[mask])
    trend_confidence = r_value**2  # R-squared

    # Check for plateau using the dedicated function, same window as trend analysis
    plateau_window = min(window_size, len(df_sorted))
    plateau_result = detect_performance_plateau(df_sorted, value_col=value_col, tolerance=0.01, window=plateau_window)
    is_plateaued = plateau_result["is_plateaued"]

    # todo: check with abhi if this logic is correct over original
    # Calculate normalized slope as percentage
    mean_val = np.mean(y[mask])
    if mean_val != 0:
        norm_slope = (slope / abs(mean_val)) * 100  # as percentage
    else:
        norm_slope = 0 if slope == 0 else (100 if slope > 0 else -100)  # Arbitrary large value

    # Determine trend direction
    if is_plateaued or abs(norm_slope) < 0.5:  # Less than 0.5% change per period
        trend_direction = TrendType.STABLE
    elif slope > 0:
        trend_direction = TrendType.UPWARD
    else:
        trend_direction = TrendType.DOWNWARD

    # Check for acceleration and recent trend
    is_accelerating = False
    recent_direction = None

    # Get recent window for analysis if enough data points
    if len(df_sorted) >= window_size:
        recent_df = df_sorted.tail(window_size)
        recent_y = recent_df[value_col].values
        recent_x = np.arange(len(recent_y))

        mask_recent = ~np.isnan(recent_y)
        if sum(mask_recent) >= 2:
            # Calculate recent trend using linregress (matching reference)
            try:
                recent_slope, _, recent_r, _, _ = linregress(recent_x[mask_recent], recent_y[mask_recent])

                # Determine recent direction
                recent_mean = np.mean(recent_y[mask_recent])
                if recent_mean != 0:
                    recent_norm_slope = (recent_slope / abs(recent_mean)) * 100
                else:
                    recent_norm_slope = 0 if recent_slope == 0 else (100 if recent_slope > 0 else -100)

                if abs(recent_norm_slope) < 0.5:
                    recent_direction = TrendType.STABLE
                elif recent_slope > 0:
                    recent_direction = TrendType.UPWARD
                else:
                    recent_direction = TrendType.DOWNWARD

                # Check for acceleration
                is_accelerating = abs(recent_slope) > abs(slope)
            except:
                # If regression fails, skip recent direction and acceleration
                is_accelerating = False
                recent_direction = None

    # Return results
    return {
        "trend_direction": TrendType.PLATEAU if is_plateaued else trend_direction,
        "trend_slope": float(slope),
        "trend_confidence": float(trend_confidence),
        "normalized_slope": float(norm_slope),
        "recent_direction": recent_direction,
        "is_accelerating": is_accelerating,
        "is_plateaued": is_plateaued,
    }


def detect_record_high(df: pd.DataFrame, value_col: str = "value") -> dict[str, Any]:
    """
    Detect if the latest value is a record high.

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        value_col: Column name containing values

    Returns:
        Dictionary with is_record_high, prior_max, rank, etc.
    """
    if value_col not in df.columns:
        raise ValidationError(f"Column '{value_col}' not found in DataFrame", invalid_fields={"value_col": value_col})

    if len(df) < 2:
        raise InsufficientDataError("Insufficient data to detect record high", data_details={"value_col": value_col})

    # Get the current value (last row)
    current_value = df[value_col].iloc[-1]

    # Get all previous values (exclude the last one)
    previous_values = df[value_col].iloc[:-1]

    # Find the maximum of previous values
    prior_max = previous_values.max()
    prior_max_idx = previous_values.idxmax()

    # Calculate rank (1 = highest, 2 = second highest, etc.)
    sorted_values = sorted(df[value_col].unique(), reverse=True)
    try:
        rank = sorted_values.index(current_value) + 1
    except ValueError:
        rank = len(sorted_values) + 1  # Should not happen

    # Return results
    return {
        "is_record_high": current_value > prior_max,
        "current_value": current_value,
        "prior_max": prior_max,
        "prior_max_index": prior_max_idx,
        "rank": rank,
        "periods_compared": len(df),
        "absolute_delta": calculate_difference(current_value, prior_max),
        "percentage_delta": calculate_percentage_difference(current_value, prior_max, handle_zero_reference=True),
    }


def detect_record_low(df: pd.DataFrame, value_col: str = "value") -> dict[str, Any]:
    """
    Detect if the latest value is a record low.

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        value_col: Column name containing values

    Returns:
        Dictionary with is_record_low, prior_min, rank, etc.
    """
    if value_col not in df.columns:
        raise ValidationError(f"Column '{value_col}' not found in DataFrame", invalid_fields={"value_col": value_col})

    if len(df) < 2:
        raise InsufficientDataError("Insufficient data to detect record low", data_details={"value_col": value_col})

    # Get the current value (last row)
    current_value = df[value_col].iloc[-1]

    # Get all previous values (exclude the last one)
    previous_values = df[value_col].iloc[:-1]

    # Find the minimum of previous values
    prior_min = previous_values.min()
    prior_min_idx = previous_values.idxmin()

    # Calculate rank (1 = lowest, 2 = second lowest, etc.)
    sorted_values = sorted(df[value_col].unique())
    try:
        rank = sorted_values.index(current_value) + 1
    except ValueError:
        rank = len(sorted_values) + 1  # Should not happen

    # Return results
    return {
        "is_record_low": current_value < prior_min,
        "current_value": current_value,
        "prior_min": prior_min,
        "prior_min_index": prior_min_idx,
        "rank": rank,
        "periods_compared": len(df),
        "absolute_delta": calculate_difference(current_value, prior_min),
        "percentage_delta": calculate_percentage_difference(current_value, prior_min, handle_zero_reference=True),
    }


# todo: check with abhi if this logic is correct over original
# logic is quite different from original
def detect_trend_exceptions(
    df: pd.DataFrame, date_col: str = "date", value_col: str = "value", window_size: int = 5, z_threshold: float = 2.0
) -> list[dict[str, Any]]:
    """
    Detect spikes and drops in a time series relative to recent values.

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        date_col: Column name containing dates
        value_col: Column name containing values
        window_size: Number of periods to include in normal range calculation
        z_threshold: Number of standard deviations to consider exceptional

    Returns:
        List of dictionaries with exception details
    """
    # Input validation
    if date_col not in df.columns:
        raise ValidationError(f"Column '{date_col}' not found in DataFrame", invalid_fields={"date_col": date_col})
    if value_col not in df.columns:
        raise ValidationError(f"Column '{value_col}' not found in DataFrame", invalid_fields={"value_col": value_col})

    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    # Check if we have enough data to calculate z-score
    if len(df_sorted) < window_size + 1:
        return []

    # Get the recent window and current value
    recent_window = df_sorted[value_col].iloc[-(window_size + 1) : -1]
    current_value = df_sorted[value_col].iloc[-1]

    # Calculate mean and standard deviation of recent values
    mean_val = recent_window.mean()
    std_val = recent_window.std()

    if std_val == 0 or pd.isna(std_val):
        # Cannot calculate z-score with zero std dev
        return []

    # Calculate z-score of current value
    z_score = (current_value - mean_val) / std_val

    # Check if current value is exceptional
    exceptions = []
    if z_score > z_threshold:
        # Spike detected
        upper_bound = mean_val + z_threshold * std_val
        delta = current_value - upper_bound
        exceptions.append(
            {
                "type": TrendExceptionType.SPIKE,
                "current_value": current_value,
                "normal_range_low": mean_val - z_threshold * std_val,
                "normal_range_high": upper_bound,
                "absolute_delta_from_normal_range": delta,
                "magnitude_percent": (delta / upper_bound * 100) if upper_bound != 0 else None,
                "z_score": z_score,
            }
        )
    elif z_score < -z_threshold:
        # Drop detected
        lower_bound = mean_val - z_threshold * std_val
        delta = lower_bound - current_value
        exceptions.append(
            {
                "type": TrendExceptionType.DROP,
                "current_value": current_value,
                "normal_range_low": lower_bound,
                "normal_range_high": mean_val + z_threshold * std_val,
                "absolute_delta_from_normal_range": -delta,  # Keep consistent with fulcrum implementation
                "magnitude_percent": (delta / abs(lower_bound) * 100) if lower_bound != 0 else None,
                "z_score": z_score,
            }
        )

    return exceptions


def detect_performance_plateau(
    df: pd.DataFrame, value_col: str = "value", tolerance: float = 0.01, window: int = 7
) -> dict[str, Any]:
    """
    Detect if a time series has plateaued (minimal changes within a threshold).

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        value_col: Column name containing values
        tolerance: Relative change threshold to consider a plateau (as a fraction)
        window: Number of periods to analyze for plateau detection

    Returns:
        Dictionary with plateau information
    """
    if value_col not in df.columns:
        raise ValidationError(f"Column '{value_col}' not found in DataFrame", invalid_fields={"value_col": value_col})

    if len(df) < window:
        return {
            "is_plateaued": False,
            "plateau_duration": 0,
            "stability_score": 0.0,
            "mean_value": df[value_col].mean() if not df.empty else None,
        }

    # Get the most recent window of values
    recent_values = df[value_col].iloc[-window:].values

    # Calculate mean and standard deviation
    mean_val = np.mean(recent_values)
    if mean_val == 0:
        # Can't calculate relative stability with zero mean
        return {"is_plateaued": False, "plateau_duration": 0, "stability_score": 0.0, "mean_value": mean_val}

    # Calculate coefficient of variation
    std_val = np.std(recent_values)
    cv = std_val / abs(mean_val)  # Coefficient of variation

    # Determine if plateaued based on CV compared to tolerance
    is_plateaued = cv <= tolerance

    # If plateaued, estimate how long it's been stable
    plateau_duration = 0
    if is_plateaued:
        # Count how many periods back the values stay within the tolerance
        full_values = df[value_col].values
        if len(full_values) > window:
            # Start from the last window points and work backward
            for i in range(len(full_values) - window - 1, -1, -1):
                test_window = full_values[i : i + window]
                test_mean = np.mean(test_window)
                if test_mean == 0:
                    break
                test_cv = np.std(test_window) / abs(test_mean)
                if test_cv <= tolerance:
                    plateau_duration += 1
                else:
                    break

    # Calculate stability score (1.0 = perfectly stable, 0.0 = highly unstable)
    stability_score = max(0.0, 1.0 - (cv / tolerance)) if tolerance > 0 else 0.0

    return {
        "is_plateaued": is_plateaued,
        "plateau_duration": plateau_duration + window if is_plateaued else 0,
        "stability_score": stability_score,
        "mean_value": mean_val,
    }


def _average_moving_range(values: pd.Series) -> float:
    """
    Calculate the average moving range for a series.

    The moving range is the absolute difference between consecutive points.
    This is used in SPC to calculate control limits.

    Args:
        values: Series of values

    Returns:
        Average moving range or 0.0 if not enough data
    """
    # Calculate absolute differences between consecutive points
    diffs = values.diff().abs().dropna()

    if len(diffs) == 0:
        return 0.0

    return diffs.mean()


def _compute_segment_center_line(
    df: pd.DataFrame, start_idx: int, end_idx: int, half_average_point: int, value_col: str
) -> tuple[list[float], float]:
    """
    Calculate the center line and slope for a segment of data.

    This helper function creates a local trend line by:
    1. Computing averages of first and last sections in the segment
    2. Computing the slope between these averages
    3. Generating a complete center line based on this slope

    Args:
        df: Input DataFrame
        start_idx: Starting index for this segment
        end_idx: Ending index for this segment
        half_average_point: Number of points to use for averaging at each end
        value_col: Name of the value column

    Returns:
        (center_line array, slope value)
    """
    # Extract segment values
    seg = df[value_col].iloc[start_idx:end_idx].reset_index(drop=True)
    n = len(seg)

    if n < 2:
        return ([None] * n, 0.0)

    # Adjust half point based on available data
    half_pt = min(half_average_point, n // 2)

    # Calculate averages of first and last sections
    first_avg = seg.iloc[:half_pt].mean()
    second_avg = seg.iloc[-half_pt:].mean()

    # Calculate slope between these averages
    slope = (second_avg - first_avg) / float(half_pt) if half_pt > 0 else 0.0

    # Generate center line based on slope
    center_line = [None] * n
    mid_idx = half_pt // 2 if half_pt > 0 else 0

    if mid_idx >= n:
        # If segment is too small, use flat center line
        center_line = [seg.mean()] * n
        slope = 0.0
        return (center_line, slope)

    # Set middle point and extend in both directions
    center_line[mid_idx] = first_avg

    # Forward projection
    for i in range(mid_idx + 1, n):
        center_line[i] = center_line[i - 1] + slope

    # Backward projection
    for i in range(mid_idx - 1, -1, -1):
        center_line[i] = center_line[i + 1] - slope

    return (center_line, slope)


def _detect_spc_signals(
    df_segment: pd.DataFrame,
    offset: int,
    central_line_array: list[float | None],
    ucl_array: list[float | None],
    lcl_array: list[float | None],
    value_col: str,
    consecutive_run_length: int,
) -> list[int]:
    """
    Detect SPC rule violations indicating a process signal.

    This function checks for:
    1. Points beyond control limits
    2. Consecutive points above/below center line

    Args:
        df_segment: Segment of data to check
        offset: Index offset for global array position
        central_line_array, ucl_array, lcl_array: Arrays containing control values
        value_col: Name of the value column
        consecutive_run_length: Number of consecutive points for run detection

    Returns:
        List of indices where signals were detected
    """
    n = len(df_segment)
    idx_start = offset
    signals_idx = []

    # 1. Points outside control limits
    for i in range(n):
        idx = idx_start + i
        if idx >= len(central_line_array):
            continue

        val = df_segment[value_col].iloc[i]

        if (ucl_array[idx] is not None and val > ucl_array[idx]) or (
            lcl_array[idx] is not None and val < lcl_array[idx]
        ):
            signals_idx.append(idx)

    # 2. Consecutive points above/below center line
    if n >= consecutive_run_length:
        for i in range(n - consecutive_run_length + 1):
            all_above = True
            all_below = True

            for j in range(consecutive_run_length):
                check_idx = idx_start + i + j
                if check_idx >= len(central_line_array) or central_line_array[check_idx] is None:
                    all_above = all_below = False
                    break

                val = df_segment[value_col].iloc[i + j]
                cl = central_line_array[check_idx]

                if val <= cl:
                    all_above = False
                if val >= cl:
                    all_below = False

            if all_above or all_below:
                end_run_idx = idx_start + i + consecutive_run_length - 1
                signals_idx.append(end_run_idx)

    return signals_idx


def _check_consecutive_signals(signal_idxes: list[int], threshold: int) -> int | None:
    """
    Check if there are enough consecutive signals to trigger recalculation.

    Args:
        signal_idxes: List of indices where signals were detected
        threshold: Number of consecutive signals required to trigger recalculation

    Returns:
        Starting index for recalculation or None if not needed
    """
    if not signal_idxes:
        return None

    s = sorted(signal_idxes)
    consecutive_count = 1

    for i in range(1, len(s)):
        if s[i] == s[i - 1] + 1:
            consecutive_count += 1
            if consecutive_count >= threshold:
                return s[i - threshold + 1]
        else:
            consecutive_count = 1

    return None


def process_control_analysis(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "value",
    min_data_points: int = 10,
    control_limit_multiplier: float = 2.66,
    consecutive_run_length: int = 7,
    half_average_point: int = 9,
    consecutive_signal_threshold: int = 5,
) -> pd.DataFrame:
    """
    Perform statistical process control (SPC) analysis on time series data.

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        date_col: Column name containing dates
        value_col: Column name containing values
        min_data_points: Minimum number of data points required for analysis
        control_limit_multiplier: Multiplier for control limits (standard is 2.66 for moving ranges)
        consecutive_run_length: Number of consecutive points in same direction to detect a trend
        half_average_point: Half-width of window used for central line calculation
        consecutive_signal_threshold: Number of consecutive signals that triggers recalculation

    Returns:
        DataFrame with SPC analysis results, including control limits and signals
    """
    # Input validation
    if date_col not in df.columns:
        raise ValidationError(f"Column '{date_col}' not found in DataFrame", invalid_fields={"date_col": date_col})
    if value_col not in df.columns:
        raise ValidationError(f"Column '{value_col}' not found in DataFrame", invalid_fields={"value_col": value_col})

    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    # Check if we have enough data
    if len(df_sorted) < min_data_points:
        result_df = df_sorted.copy()
        result_df["central_line"] = np.nan
        result_df["ucl"] = np.nan
        result_df["lcl"] = np.nan
        result_df["slope"] = np.nan
        result_df["slope_change"] = np.nan
        result_df["trend_signal_detected"] = False
        return result_df

    # Create a copy for SPC analysis
    dff = df_sorted.copy()
    n_points = len(dff)

    # Initialize arrays for dynamic calculation
    central_line_array = [None] * n_points
    ucl_array = [None] * n_points
    lcl_array = [None] * n_points
    signal_array = [False] * n_points
    slope_array = [None] * n_points

    # Process data in segments for a more dynamic approach
    start_idx = 0
    while start_idx < n_points:
        # Define segment end
        end_idx = min(start_idx + half_average_point * 2, n_points)
        seg_length = end_idx - start_idx

        if seg_length < 2:
            break

        # Compute segment center line and slope
        center_line, segment_slope = _compute_segment_center_line(
            dff, start_idx, end_idx, half_average_point, value_col
        )

        # Store center line and slope values
        for i in range(seg_length):
            idx = start_idx + i
            if idx < n_points:
                central_line_array[idx] = center_line[i]
                slope_array[idx] = segment_slope

        # Calculate control limits from moving ranges
        segment_values = dff[value_col].iloc[start_idx:end_idx].reset_index(drop=True)
        avg_range = _average_moving_range(segment_values)

        for i in range(seg_length):
            idx = start_idx + i
            if idx < n_points:
                cl_val = central_line_array[idx]
                if cl_val is not None and not np.isnan(cl_val):
                    ucl_array[idx] = cl_val + avg_range * control_limit_multiplier
                    lcl_array[idx] = cl_val - avg_range * control_limit_multiplier
                else:
                    ucl_array[idx] = np.nan
                    lcl_array[idx] = np.nan

        # Detect signals
        signals_idx = _detect_spc_signals(
            df_segment=dff.iloc[start_idx:end_idx],
            offset=start_idx,
            central_line_array=central_line_array,
            ucl_array=ucl_array,
            lcl_array=lcl_array,
            value_col=value_col,
            consecutive_run_length=consecutive_run_length,
        )

        # Mark signals
        for idx in signals_idx:
            if idx < n_points:
                signal_array[idx] = True

        # Check for consecutive signals to trigger recalculation
        recalc_idx = _check_consecutive_signals(signals_idx, consecutive_signal_threshold)

        if recalc_idx is not None and recalc_idx < n_points:
            start_idx = recalc_idx
        else:
            start_idx = end_idx

    # Prepare result DataFrame
    dff["central_line"] = central_line_array
    dff["ucl"] = ucl_array
    dff["lcl"] = lcl_array
    dff["slope"] = slope_array
    dff["trend_signal_detected"] = signal_array

    # Calculate slope changes
    dff["slope_change"] = np.nan
    for i in range(1, len(dff)):
        s_now = slope_array[i]
        s_prev = slope_array[i - 1]
        if s_now is not None and s_prev is not None and abs(s_prev) > 1e-9:
            dff.loc[dff.index[i], "slope_change"] = (s_now - s_prev) / abs(s_prev) * 100.0

    return dff


def _apply_anomaly_detection_methods(
    df: pd.DataFrame, value_col: str, z_threshold: float, method: AnomalyDetectionMethod
) -> pd.DataFrame:
    """
    Apply the selected anomaly detection methods to the dataframe.

    Args:
        df: DataFrame with rolling statistics calculated
        value_col: Column name containing values
        z_threshold: Z-score threshold for the variance method
        method: Detection method: 'variance', 'spc', or 'combined'

    Returns:
        DataFrame with anomaly flags added
    """
    # Variance method: z-score approach
    if method in [AnomalyDetectionMethod.VARIANCE, AnomalyDetectionMethod.COMBINED]:
        # Calculate z-scores
        df["z_score"] = np.nan
        mask = ~df["rolling_std"].isna() & (df["rolling_std"] > 0)
        if mask.any():
            df.loc[mask, "z_score"] = (df.loc[mask, value_col] - df.loc[mask, "rolling_mean"]) / df.loc[
                mask, "rolling_std"
            ]

        # Flag anomalies based on z-score threshold
        df["is_anomaly_variance"] = np.abs(df["z_score"]) > z_threshold
        df["is_anomaly_variance"] = df["is_anomaly_variance"].fillna(False)
    else:
        df["is_anomaly_variance"] = False

    # SPC method: control limits approach
    if method in [AnomalyDetectionMethod.SPC, AnomalyDetectionMethod.COMBINED]:
        # Add SPC control limits
        df["ucl"] = df["rolling_mean"] + z_threshold * df["rolling_std"]
        df["lcl"] = df["rolling_mean"] - z_threshold * df["rolling_std"]

        # Flag points outside control limits
        df["is_anomaly_spc"] = (df[value_col] > df["ucl"]) | (df[value_col] < df["lcl"])
        df["is_anomaly_spc"] = df["is_anomaly_spc"].fillna(False)
    else:
        df["is_anomaly_spc"] = False

    # Combined anomaly flag
    df["is_anomaly"] = df["is_anomaly_variance"] | df["is_anomaly_spc"]

    return df


def detect_anomalies(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "value",
    window_size: int = 7,
    z_threshold: float = 3.0,
    method: str = "combined",
) -> pd.DataFrame:
    """
    Detect anomalies in a time series using multiple methods.

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        date_col: Column name containing dates
        value_col: Column name containing values
        window_size: Size of the rolling window for SPC methods
        z_threshold: Z-score threshold for the variance method
        method: Detection method: 'variance', 'spc', or 'combined'

    Returns:
        DataFrame with added columns for anomaly detection
    """
    # Input validation
    if date_col not in df.columns:
        raise ValidationError(f"Column '{date_col}' not found in DataFrame", invalid_fields={"date_col": date_col})
    if value_col not in df.columns:
        raise ValidationError(f"Column '{value_col}' not found in DataFrame", invalid_fields={"value_col": value_col})

    if method not in ["variance", "spc", "combined"]:
        raise ValidationError(
            f"Invalid method: {method}. Must be 'variance', 'spc', or 'combined'", invalid_fields={"method": method}
        )

    # Ensure data is sorted by date
    dff = validate_date_sorted(df, date_col)

    if len(dff) < 2:
        # Not enough data for analysis
        dff = dff.copy()
        dff["is_anomaly"] = False
        return dff

    # Calculate rolling statistics
    dff = dff.copy()
    dff["rolling_mean"] = dff[value_col].rolling(window=window_size, min_periods=2).mean()
    dff["rolling_std"] = dff[value_col].rolling(window=window_size, min_periods=2).std()

    # Apply selected detection methods
    dff = _apply_anomaly_detection_methods(dff, value_col, z_threshold, method)

    return dff
