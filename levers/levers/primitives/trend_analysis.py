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

import logging

import numpy as np
import pandas as pd
from scipy.stats import linregress

from levers.exceptions import InsufficientDataError, ValidationError
from levers.models import (
    AnomalyDetectionMethod,
    PerformancePlateau,
    RecordHigh,
    RecordLow,
    TrendAnalysis,
    TrendExceptionType,
    TrendType,
)
from levers.models.patterns import Seasonality, TrendException
from levers.primitives import calculate_difference, calculate_percentage_difference, validate_date_sorted

logger = logging.getLogger(__name__)


def analyze_metric_trend(
    df: pd.DataFrame, value_col: str = "value", date_col: str = "date", window_size: int = 7
) -> TrendAnalysis | None:
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
        None or TrendAnalysis object containing trend analysis details,
        - trend_type: str, the type of trend
        - trend_slope: float, the slope of the trend
        - trend_confidence: float, the confidence in the trend
        - recent_trend_type: str, the type of trend in the most recent period
        - is_accelerating: bool, whether the trend is accelerating
        - is_plateaued: bool, whether the trend is plateaued
    """
    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    if len(df_sorted) < 2:
        logger.warning("Not enough data points to analyze trend")
        return None

    # Full dataset trend analysis
    y = df_sorted[value_col].values
    x = np.arange(len(y))

    # Handle missing values
    mask = ~np.isnan(y)  # type: ignore
    if sum(mask) < 2:
        return None

    # Calculate trend using linregress
    slope, _, r_value, _, _ = linregress(x[mask], y[mask])
    trend_confidence = r_value**2  # R-squared

    # Calculate normalized slope
    mean_val = np.mean(y[mask])
    if mean_val != 0:
        norm_slope = (slope / abs(mean_val)) * 100  # as percentage
    else:
        norm_slope = 0 if slope == 0 else (100 if slope > 0 else -100)  # Arbitrary large value

    # Determine trend direction using absolute slope threshold
    if abs(norm_slope) < 0.5:
        trend_type = TrendType.STABLE
    elif slope > 0:
        trend_type = TrendType.UPWARD
    else:
        trend_type = TrendType.DOWNWARD

    # Check for acceleration and recent trend
    is_accelerating = False
    recent_trend_type = None

    # Get recent window for analysis if enough data points
    if len(df_sorted) >= window_size:
        recent_df = df_sorted.tail(window_size)
        recent_y = recent_df[value_col].values
        recent_x = np.arange(len(recent_y))

        mask_recent = ~np.isnan(recent_y)  # type: ignore
        if sum(mask_recent) >= 2:
            # Calculate recent trend using linregress
            try:
                recent_slope, _, _, _, _ = linregress(recent_x[mask_recent], recent_y[mask_recent])

                recent_mean = np.mean(recent_y[mask_recent])
                if recent_mean != 0:
                    recent_norm_slope = (recent_slope / abs(recent_mean)) * 100
                else:
                    recent_norm_slope = 0 if recent_slope == 0 else (100 if recent_slope > 0 else -100)

                if abs(recent_norm_slope) < 0.5:
                    # Determine recent direction using absolute slope threshold
                    recent_trend_type = TrendType.STABLE
                elif recent_slope > 0:
                    recent_trend_type = TrendType.UPWARD
                else:
                    recent_trend_type = TrendType.DOWNWARD

                # Check for acceleration
                is_accelerating = abs(recent_slope) > abs(slope)
            except Exception:
                # If regression fails, skip recent direction and acceleration
                is_accelerating = False
                recent_trend_type = None

    # Check for plateau after determining basic trend
    plateau_window = min(window_size, len(df_sorted))
    plateau_result = detect_performance_plateau(df_sorted, value_col=value_col, tolerance=0.01, window=plateau_window)
    is_plateaued = plateau_result.is_plateaued

    # If it's a plateau, override the trend type
    if is_plateaued:
        trend_type = TrendType.PLATEAU

    return TrendAnalysis(
        trend_type=trend_type,
        trend_slope=slope,
        trend_confidence=trend_confidence,
        normalized_slope=norm_slope,
        recent_trend_type=recent_trend_type,
        is_accelerating=is_accelerating,
        is_plateaued=is_plateaued,
    )


def detect_record_high(df: pd.DataFrame, value_col: str = "value") -> RecordHigh:
    """
    Detect if the latest value is a record high.

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        value_col: Column name containing values

    Returns:
        RecordHigh object containing record high details,
        - is_record_high: bool, whether the current value is a record high
        - current_value: float, the current value
        - prior_max: float, the maximum value in the previous periods
        - prior_max_index: int, the index of the maximum value in the previous periods
        - rank: int, the rank of the current value
        - periods_compared: int, the number of periods compared
        - absolute_delta: float, the absolute difference between current and prior max
        - percentage_delta: float, the percentage difference between current and prior max
    """
    if len(df) < 2:
        raise InsufficientDataError("Insufficient data to detect record high", data_details={"value_col": value_col})

    # Get the current value (last row)
    current_value = df[value_col].iloc[-1]

    # Get all previous values (exclude the last one)
    previous_values = df[value_col].iloc[:-1]

    # Find the maximum of previous values
    prior_max = previous_values.max()
    prior_max_idx = int(previous_values.idxmax())

    # Calculate rank (1 = highest, 2 = second highest, etc.)
    rank = df[value_col].rank(method="min", ascending=False).iloc[-1]

    return RecordHigh(
        is_record_high=current_value > prior_max,
        current_value=current_value,
        prior_max=prior_max,
        prior_max_index=prior_max_idx,
        rank=int(rank),
        periods_compared=len(df),
        absolute_delta=calculate_difference(current_value, prior_max),
        percentage_delta=calculate_percentage_difference(current_value, prior_max, handle_zero_reference=True),
    )


def detect_record_low(df: pd.DataFrame, value_col: str = "value") -> RecordLow:
    """
    Detect if the latest value is a record low.

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        value_col: Column name containing values

    Returns:
        RecordLow object containing record low details,
        - is_record_low: bool, whether the current value is a record low
        - current_value: float, the current value
        - prior_min: float, the minimum value in the previous periods
        - prior_min_index: int, the index of the minimum value in the previous periods
        - rank: int, the rank of the current value
        - periods_compared: int, the number of periods compared
        - absolute_delta: float, the absolute difference between current and prior min
        - percentage_delta: float, the percentage difference between current and prior min
    """
    if len(df) < 2:
        raise InsufficientDataError("Insufficient data to detect record low", data_details={"value_col": value_col})

    # Get the current value (last row)
    current_value = df[value_col].iloc[-1]

    # Get all previous values (exclude the last one)
    previous_values = df[value_col].iloc[:-1]

    # Find the minimum of previous values
    prior_min = previous_values.min()
    prior_min_idx = int(previous_values.idxmin())

    # Calculate rank (1 = lowest, 2 = second lowest, etc.)
    rank = df[value_col].rank(method="min", ascending=True).iloc[-1]

    return RecordLow(
        is_record_low=current_value < prior_min,
        current_value=current_value,
        prior_min=prior_min,
        prior_min_index=prior_min_idx,
        rank=int(rank),
        periods_compared=len(df),
        absolute_delta=calculate_difference(current_value, prior_min),
        percentage_delta=calculate_percentage_difference(current_value, prior_min, handle_zero_reference=True),
    )


def detect_trend_exceptions(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "value",
    window_size: int = 5,
    z_threshold: float = 2.0,
) -> TrendException | None:
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
        TrendException object containing exception details,
        - type: str, the type of exception
        - current_value: float, the current value
        - normal_range_low: float, the lower bound of the normal range
        - normal_range_high: float, the upper bound of the normal range
        - absolute_delta_from_normal_range: float, the absolute difference between current and normal range
        - magnitude_percent: float, the percentage difference between current and normal range
        or None if no exceptions are detected
    """

    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    # Check if we have enough data
    if len(df_sorted) < window_size:
        logger.warning("Insufficient data for trend exceptions detection")
        return None

    # Get the recent window for analysis
    recent_subset = df_sorted[value_col].iloc[-window_size:-1]

    # Calculate mean and standard deviation of recent values
    mean_val = recent_subset.mean()
    std_val = recent_subset.std()

    # Cannot calculate bounds with invalid std dev
    if std_val is None or pd.isna(std_val) or std_val == 0:
        logger.warning("Cannot calculate bounds for trend exceptions (invalid std dev)")
        return None

    # Calculate upper and lower bounds based on threshold
    upper_bound = mean_val + z_threshold * std_val
    lower_bound = mean_val - z_threshold * std_val

    # Get the current value (last value in the dataframe)
    last_val = df_sorted[value_col].iloc[-1]

    # Check if the current value is outside the bounds
    if last_val > upper_bound:
        # Spike detected
        delta_from_range = last_val - upper_bound
        magnitude_percent = (delta_from_range / upper_bound * 100.0) if upper_bound != 0 else None

        return TrendException(
            type=TrendExceptionType.SPIKE,
            current_value=last_val,
            normal_range_low=lower_bound,
            normal_range_high=upper_bound,
            absolute_delta_from_normal_range=delta_from_range,
            magnitude_percent=magnitude_percent,
        )
    elif last_val < lower_bound:
        # Drop detected
        delta_from_range = lower_bound - last_val
        magnitude_percent = (delta_from_range / abs(lower_bound) * 100.0) if lower_bound != 0 else None

        return TrendException(
            type=TrendExceptionType.DROP,
            current_value=last_val,
            normal_range_low=lower_bound,
            normal_range_high=upper_bound,
            absolute_delta_from_normal_range=-delta_from_range,  # Negative value for drops
            magnitude_percent=magnitude_percent,
        )
    return None


def detect_performance_plateau(
    df: pd.DataFrame, value_col: str = "value", tolerance: float = 0.01, window: int = 7
) -> PerformancePlateau:
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
        PerformancePlateau object containing plateau information,
        - is_plateaued: bool, whether the time series has plateaued
        - plateau_duration: int, the number of periods the time series has plateaued
        - stability_score: float, the stability score of the time series
        - mean_value: float, the mean value of the time series
    """

    if len(df) < window:
        return PerformancePlateau(
            is_plateaued=False,
            plateau_duration=0,
            stability_score=0.0,
            mean_value=df[value_col].mean() if not df.empty else None,
        )

    # Get the most recent window of values - similar to original
    sub = df[value_col].tail(window).dropna()

    if len(sub) < window:
        return PerformancePlateau(
            is_plateaued=False,
            plateau_duration=0,
            stability_score=0.0,
            mean_value=df[value_col].mean() if not df.empty else None,
        )

    # Calculate mean for use in relative range and result
    mean_val = sub.mean()

    # Avoid division by zero - similar to original
    if abs(mean_val) < 1e-12:
        return PerformancePlateau(
            is_plateaued=False,
            plateau_duration=0,
            stability_score=0.0,
            mean_value=None,
        )

    # Calculate relative range - exactly like original implementation
    relative_range = (sub.max() - sub.min()) / abs(mean_val)

    # Determine if plateaued based on relative range compared to tolerance
    is_plateaued = relative_range < tolerance

    # Calculate stability score (1.0 = perfectly stable, 0.0 = highly unstable)
    stability_score = max(0.0, 1.0 - (relative_range / tolerance)) if tolerance > 0 else 0.0

    # Basic plateau duration calculation - simplified from original
    plateau_duration = window if is_plateaued else 0

    return PerformancePlateau(
        is_plateaued=is_plateaued,
        plateau_duration=plateau_duration,
        stability_score=float(stability_score),
        mean_value=float(mean_val),
    )


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
) -> tuple[list[float | None], float]:
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
        return [None] * n, 0.0

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
        center_line = [seg.mean()] * n  # type: ignore
        slope = 0.0
        return center_line, slope  # type: ignore

    # Set middle point and extend in both directions
    center_line[mid_idx] = first_avg  # type: ignore

    # Forward projection
    for i in range(mid_idx + 1, n):
        center_line[i] = center_line[i - 1] + slope  # type: ignore

    # Backward projection
    for i in range(mid_idx - 1, -1, -1):
        center_line[i] = center_line[i + 1] - slope  # type: ignore

    return center_line, slope  # type: ignore


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
        List of signal indices
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
        if ucl_array[idx] is not None and val > ucl_array[idx]:
            signals_idx.append(idx)
        elif lcl_array[idx] is not None and val < lcl_array[idx]:
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
        DataFrame with SPC analysis results, including control limits, signals, and trend types
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
        result_df["central_line"] = None
        result_df["ucl"] = None
        result_df["lcl"] = None
        result_df["slope"] = None
        result_df["slope_change"] = None
        result_df["trend_signal_detected"] = False
        result_df["trend_type"] = None
        return result_df

    # Create a copy for SPC analysis
    dff = df_sorted.copy()
    n_points = len(dff)

    # Initialize arrays for dynamic calculation
    central_line_array = [None] * n_points
    ucl_array = [None] * n_points
    lcl_array = [None] * n_points
    signal_array = [False] * n_points
    slope_array: list[float | None] = [None] * n_points

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
                central_line_array[idx] = center_line[i]  # type: ignore
                slope_array[idx] = segment_slope  # type: ignore

        # Calculate control limits from moving ranges
        segment_values = dff[value_col].iloc[start_idx:end_idx].reset_index(drop=True)
        avg_range = _average_moving_range(segment_values)

        for i in range(seg_length):
            idx = start_idx + i
            if idx < n_points:
                cl_val = central_line_array[idx]
                if cl_val is not None:
                    ucl_array[idx] = cl_val + avg_range * control_limit_multiplier  # type: ignore
                    lcl_array[idx] = cl_val - avg_range * control_limit_multiplier  # type: ignore
                else:
                    ucl_array[idx] = None
                    lcl_array[idx] = None

        # Detect signals
        signals_idx = _detect_spc_signals(
            df_segment=dff.iloc[start_idx:end_idx],
            offset=start_idx,
            central_line_array=central_line_array,  # type: ignore
            ucl_array=ucl_array,  # type: ignore
            lcl_array=lcl_array,  # type: ignore
            value_col=value_col,
            consecutive_run_length=consecutive_run_length,
        )

        # Mark signals in the global signal array
        # This loop takes the signal indices detected in the current segment
        # and marks them as True in the global signal_array that tracks
        # which data points have SPC rule violations (signals)
        for signal_idx in signals_idx:
            signal_array[signal_idx] = True

        # Check for consecutive signals to trigger recalculation
        recalc_idx = _check_consecutive_signals(signals_idx, consecutive_signal_threshold)

        if recalc_idx is not None and recalc_idx < n_points:
            start_idx = recalc_idx
        else:
            start_idx = end_idx

    # Prepare result DataFrame with SPC analysis
    dff["central_line"] = central_line_array
    dff["ucl"] = ucl_array
    dff["lcl"] = lcl_array
    dff["slope"] = slope_array
    dff["trend_signal_detected"] = signal_array

    # Calculate slope changes
    dff["slope_change"] = None
    for i in range(1, len(dff)):
        s_now = slope_array[i]
        s_prev = slope_array[i - 1]
        if s_prev is not None and abs(s_prev) > 1e-9 and s_now is not None:
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
        df["z_score"] = None
        mask = ~df["rolling_std"].isna() & (df["rolling_std"] > 0)
        if mask.any():
            df.loc[mask, "z_score"] = (df.loc[mask, value_col] - df.loc[mask, "rolling_mean"]) / df.loc[
                mask, "rolling_std"
            ]

        # Flag anomalies based on z-score threshold, but only where z_score is not None
        df["is_anomaly_variance"] = False
        z_score_mask = ~df["z_score"].isna()
        if z_score_mask.any():
            df.loc[z_score_mask, "is_anomaly_variance"] = np.abs(df.loc[z_score_mask, "z_score"]) > z_threshold
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
    method: AnomalyDetectionMethod = AnomalyDetectionMethod.COMBINED,
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

    if method not in [AnomalyDetectionMethod.VARIANCE, AnomalyDetectionMethod.SPC, AnomalyDetectionMethod.COMBINED]:
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


def detect_seasonality_pattern(
    df: pd.DataFrame, lookback_end: pd.Timestamp, date_col: str = "date", value_col: str = "value"
) -> Seasonality | None:
    """
    Detect seasonality pattern by comparing current value to value from one year ago.

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        date_col: Column name containing dates
        value_col: Column name containing values
        lookback_end: End date of the analysis window (defaults to last date in df)

    Returns:
        None if insufficient data or Seasonality object containing seasonality analysis results,
        - is_following_expected_pattern: bool, whether the current value is following the expected pattern
        - expected_change_percent: float, the expected change percent
        - actual_change_percent: float, the actual change percent
        - deviation_percent: float, the deviation percent
    """

    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    if df_sorted.empty:
        return None

    # Find data from approximately 1 year earlier than lookback_end
    yoy_date = lookback_end - pd.Timedelta(days=365)

    # Get the last row that is <= yoy_date
    yoy_df = df_sorted[df_sorted[date_col] <= yoy_date]

    # If we can't find a yoy reference, return None
    if yoy_df.empty:
        return None

    yoy_ref_value = yoy_df.iloc[-1][value_col]  # last row
    current_value = df_sorted.iloc[-1][value_col]  # final row

    try:
        actual_change_percent = calculate_percentage_difference(
            current_value, yoy_ref_value, handle_zero_reference=True
        )
    except Exception:
        actual_change_percent = None

    if actual_change_percent is None:
        return None

    # Define expected change as average YoY across all pairs
    yoy_changes = []
    for i in range(len(df_sorted)):
        this_date = df_sorted.iloc[i][date_col]
        search_date = this_date - pd.Timedelta(days=365)
        subset = df_sorted[df_sorted[date_col] <= search_date]
        if not subset.empty:
            ref_val = subset.iloc[-1][value_col]
            cur_val = df_sorted.iloc[i][value_col]
            try:
                change = calculate_percentage_difference(cur_val, ref_val, handle_zero_reference=True)
                if change is not None:
                    yoy_changes.append(change)
            except Exception as e:
                logger.warning(f"Error calculating percentage difference: {e}")
                pass

    expected_change = 0.0
    if yoy_changes:
        expected_change = float(np.mean(yoy_changes))

    deviation_percent = actual_change_percent - expected_change
    # Using threshold of 2% for determining if following expected pattern
    is_following = abs(deviation_percent) <= 2.0

    return Seasonality(
        is_following_expected_pattern=is_following,
        expected_change_percent=expected_change,
        actual_change_percent=actual_change_percent,
        deviation_percent=deviation_percent,
    )


def detect_trend_exceptions_using_spc_analysis(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "value",
    window_size: int = 5,
) -> TrendException | None:
    """
    Detect spikes and drops in a time series using SPC analysis.

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        date_col: Column name containing dates
        value_col: Column name containing values
        window_size: Number of periods to include in normal range calculation

    Returns:
        TrendException object containing exception details,
        - type: str, the type of exception
        - current_value: float, the current value
        - normal_range_low: float, the lower bound of the normal range
        - normal_range_high: float, the upper bound of the normal range
        - absolute_delta_from_normal_range: float, the absolute difference between current and normal range
        - magnitude_percent: float, the percentage difference between current and normal range
        or None if no exceptions are detected
    """

    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    # Check if we have enough data
    if len(df_sorted) < window_size:
        logger.warning("Insufficient data for trend exceptions detection")
        return None

    # Use SPC control limits for exception detection
    latest_row = df_sorted.iloc[-1]

    # Get current value and control limits
    current_value = latest_row[value_col]
    ucl = latest_row["ucl"]
    lcl = latest_row["lcl"]
    central_line = latest_row["central_line"]

    # Skip if control limits are not valid
    if pd.isna(ucl) or pd.isna(lcl) or pd.isna(central_line):
        logger.warning("Invalid control limits for trend exceptions detection")
        return None

    # Check for spike (above UCL)
    if current_value > ucl:
        delta_from_range = current_value - ucl
        magnitude_percent = (delta_from_range / ucl * 100.0) if ucl != 0 else None

        return TrendException(
            type=TrendExceptionType.SPIKE,
            current_value=current_value,
            normal_range_low=lcl,
            normal_range_high=ucl,
            absolute_delta_from_normal_range=delta_from_range,
            magnitude_percent=magnitude_percent,
        )

    # Check for drop (below LCL)
    elif current_value < lcl:
        delta_from_range = lcl - current_value
        magnitude_percent = (delta_from_range / abs(lcl) * 100.0) if lcl != 0 else None

        return TrendException(
            type=TrendExceptionType.DROP,
            current_value=current_value,
            normal_range_low=lcl,
            normal_range_high=ucl,
            absolute_delta_from_normal_range=-delta_from_range,  # Negative for drops
            magnitude_percent=magnitude_percent,
        )

    return None


def analyze_trend_using_spc_analysis(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "value",
    slope_col: str = "slope",
    signal_col: str = "trend_signal_detected",
    slope_threshold: float = 0.5,
    plateau_tolerance: float = 0.01,
    plateau_window: int = 7,
) -> pd.DataFrame:
    """
    Analyze trend types for each point in a time series with SPC data.

    This is a post-processing primitive that determines trend types based on:
    - Plateau detection using performance plateau analysis
    - Slope analysis with normalized thresholds
    - SPC signal detection results

    Family: trend_analysis
    Version: 1.0

    Args:
        df: DataFrame containing time series data with SPC analysis results
        date_col: Column name containing dates
        value_col: Column name containing values
        slope_col: Column name containing slope values from SPC analysis
        signal_col: Column name containing signal detection flags
        slope_threshold: Threshold for normalized slope to determine trend direction (as percentage)
        plateau_tolerance: Relative change threshold for plateau detection
        plateau_window: Window size for plateau detection

    Returns:
        DataFrame with added 'trend_type' column containing trend classifications
    """
    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    # Create working copy
    dff = df_sorted.copy()

    if len(dff) < 2:
        return dff

    n_points = len(dff)

    # Determine trend type for each point using signals and slopes
    for i in range(n_points):
        # First check for plateau
        window_size = min(plateau_window, len(dff))
        start_window = max(0, i - window_size // 2)
        end_window = min(len(dff), i + window_size // 2 + 1)
        window_data = dff.iloc[start_window:end_window].copy()

        plateau_result = detect_performance_plateau(
            window_data, value_col=value_col, tolerance=plateau_tolerance, window=min(window_size, len(window_data))
        )

        if plateau_result.is_plateaued:
            dff.loc[dff.index[i], "trend_type"] = TrendType.PLATEAU
        else:
            # If not plateaued, use slope and signals to determine trend
            slope = dff[slope_col].iloc[i] if slope_col in dff.columns else None
            is_signal = dff[signal_col].iloc[i] if signal_col in dff.columns else False

            # Use normalized slope comparison like in analyze_metric_trend
            if slope is not None and not pd.isna(slope):
                # Get a window around current point to calculate mean for normalization
                window_start = max(0, i - 3)
                window_end = min(len(dff), i + 4)
                window_values = dff[value_col].iloc[window_start:window_end]
                mean_val = window_values.mean()

                if mean_val != 0:
                    normalized_slope = (slope / abs(mean_val)) * 100  # as percentage
                else:
                    normalized_slope = 0 if slope == 0 else (100 if slope > 0 else -100)

                # Use threshold for trend determination
                if abs(normalized_slope) > slope_threshold:
                    if slope > 0:
                        dff.loc[dff.index[i], "trend_type"] = TrendType.UPWARD if is_signal else TrendType.STABLE
                    else:
                        dff.loc[dff.index[i], "trend_type"] = TrendType.DOWNWARD if is_signal else TrendType.STABLE
                else:
                    dff.loc[dff.index[i], "trend_type"] = TrendType.STABLE
            else:
                dff.loc[dff.index[i], "trend_type"] = None

    return dff
