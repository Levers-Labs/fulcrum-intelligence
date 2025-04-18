"""
Time Series Growth Primitives
=============================================================================

This module provides functions for analyzing growth patterns in time series data:
- Period-over-period growth calculations
- Rolling averages and smoothing
- Cumulative growth and indexing
- Time-to-date comparisons
- Trend slope calculation via linear regression

Dependencies:
- pandas as pd
- numpy as np
- scipy.stats for linear regression
"""

import logging

import numpy as np
import pandas as pd
from scipy.stats import linregress

from levers.exceptions import ValidationError
from levers.models import (
    AverageGrowth,
    AverageGrowthMethod,
    CumulativeGrowthMethod,
    DataFillMethod,
    Granularity,
    PartialInterval,
    TimeSeriesSlope,
)
from levers.models.patterns import BenchmarkComparison
from levers.primitives import calculate_difference, calculate_percentage_difference

logger = logging.getLogger(__name__)


def validate_date_sorted(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """
    Ensure DataFrame is sorted by date and dates are datetime objects.
    Returns a new sorted DataFrame without modifying the original.

    Family: time_series
    Version: 1.0

    Args:
        df: DataFrame to validate and sort
        date_col: Column name containing dates

    Returns:
        Date-sorted DataFrame with datetime column
    """
    result = df.copy()
    result[date_col] = pd.to_datetime(result[date_col])
    return result.sort_values(by=date_col)


def convert_grain_to_freq(grain: Granularity) -> str:
    """
    Convert a textual grain like 'day','week','month' into a pandas frequency alias.

    Family: time_series
    Version: 1.0

    Args:
        grain: Time grain description

    Returns:
        Pandas frequency alias

    Raises:
        ValueError: If the grain is not supported
    """
    g = grain.lower()
    if g in [Granularity.DAY]:
        return "D"
    elif g in [Granularity.WEEK]:
        return "W-MON"  # Start week on Monday
    elif g in [Granularity.MONTH]:
        return "MS"  # Month start
    elif g in [Granularity.QUARTER]:
        return "QS"  # Quarter start
    elif g in [Granularity.YEAR]:
        return "YS"  # Year start
    else:
        raise ValidationError(f"Unsupported grain '{grain}'. Use day, week, month, quarter, or year.")


def calculate_pop_growth(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "value",
    periods: int = 1,
    fill_method: DataFillMethod | None = None,
    annualize: bool = False,
    growth_col_name: str = "pop_growth",
) -> pd.DataFrame:
    """
    Calculate period-over-period growth rates.

    Family: time_series
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        date_col: Column name containing dates
        value_col: Column name containing values to calculate growth for
        periods: Number of periods to shift for growth calculation
        fill_method: Method to fill NA values: None, 'ffill', 'bfill', or 'interpolate'
        annualize: Whether to annualize growth rates
        growth_col_name: Name for the growth rate column in output

    Returns:
        Original DataFrame with added growth rate column
    """

    if periods <= 0:
        raise ValidationError("periods must be a positive integer")

    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    # Calculate previous values
    df_sorted["prev_value"] = df_sorted[value_col].shift(periods)

    # Calculate growth rate
    if annualize and date_col in df_sorted.columns:
        # Annualized growth calculation
        df_sorted["days_diff"] = (df_sorted[date_col] - df_sorted[date_col].shift(periods)).dt.days

        # Use numpy's power function for vectorized calculation
        mask = (df_sorted["prev_value"] > 0) & (df_sorted["days_diff"] > 0)
        df_sorted[growth_col_name] = np.nan

        if mask.any():
            ratio = df_sorted.loc[mask, value_col] / df_sorted.loc[mask, "prev_value"]
            exponent = 365.0 / df_sorted.loc[mask, "days_diff"]
            df_sorted.loc[mask, growth_col_name] = (np.power(ratio, exponent) - 1) * 100

        df_sorted.drop("days_diff", axis=1, inplace=True)
    else:
        # Standard period-over-period growth rate - vectorized calculation
        df_sorted[growth_col_name] = np.where(
            (df_sorted["prev_value"] != 0) & (~pd.isna(df_sorted["prev_value"])),
            ((df_sorted[value_col] - df_sorted["prev_value"]) / df_sorted["prev_value"]) * 100,
            np.nan,
        )

    # Handle infinities
    df_sorted[growth_col_name] = df_sorted[growth_col_name].replace([np.inf, -np.inf], np.nan)

    # Handle fill method if specified
    if fill_method:
        if fill_method in [DataFillMethod.FORWARD_FILL, DataFillMethod.BACKWARD_FILL]:
            df_sorted[growth_col_name] = df_sorted[growth_col_name].fillna(method=fill_method)  # type: ignore
        elif fill_method == DataFillMethod.INTERPOLATE:
            df_sorted[growth_col_name] = df_sorted[growth_col_name].interpolate()
        else:
            raise ValidationError(f"Unsupported fill_method: {fill_method}. Use 'ffill', 'bfill', or 'interpolate'")

    # Remove temporary column
    df_sorted.drop("prev_value", axis=1, inplace=True)

    return df_sorted


def calculate_average_growth(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "value",
    method: AverageGrowthMethod = AverageGrowthMethod.ARITHMETIC,
) -> AverageGrowth:
    """
    Calculate average growth rate across a time series.

    Family: time_series
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        date_col: Column name containing dates
        value_col: Column name containing values
        method: "arithmetic" for simple average or "cagr" for compound annual growth rate

    Returns:
        An AverageGrowth object containing average growth details,
        - average_growth: float, the average growth rate
        - total_growth: float, the total growth rate
        - periods: int, the number of periods in the time series
    """
    # Input validation for minimum number of data points
    if len(df) < 2:
        logger.warning("Not enough data points to calculate average growth")
        return AverageGrowth(average_growth=None, total_growth=None, periods=len(df))

    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    # Get first and last values
    first_value = df_sorted[value_col].iloc[0]
    last_value = df_sorted[value_col].iloc[-1]

    # Calculate total growth
    total_growth = calculate_percentage_difference(last_value, first_value)

    if method == AverageGrowthMethod.ARITHMETIC:
        # Calculate period-over-period growth
        growth_df = calculate_pop_growth(df_sorted, date_col, value_col)

        # Average the growth rates (excluding NaN)
        growth_rates = growth_df["pop_growth"].dropna().tolist()
        if not growth_rates:
            avg_growth = None
        else:
            avg_growth = float(np.mean(growth_rates))

    elif method == AverageGrowthMethod.CAGR:
        # Calculate compound annual growth rate
        if first_value <= 0 or last_value <= 0:
            avg_growth = None
        else:
            # Get the time difference in years
            first_date = df_sorted[date_col].iloc[0]
            last_date = df_sorted[date_col].iloc[-1]
            years_diff = (last_date - first_date).days / 365.25

            if years_diff > 0:
                avg_growth = ((last_value / first_value) ** (1 / years_diff) - 1) * 100
            else:
                avg_growth = None
    else:
        raise ValidationError(f"Unknown method: {method}. Use 'arithmetic' or 'cagr'.")

    return AverageGrowth(average_growth=avg_growth, total_growth=total_growth, periods=len(df_sorted) - 1)


def calculate_rolling_averages(
    df: pd.DataFrame,
    value_col: str = "value",
    windows: list[int] | None = None,
    min_periods: dict[int, int] | None = None,
    center: bool = False,
) -> pd.DataFrame:
    """
    Create rolling means for smoothing out fluctuations.

    Family: time_series
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        value_col: Column name containing values
        windows: List of window sizes for rolling calculations
        min_periods: Dictionary mapping window size to minimum periods required
        center: Whether to center the window

    Returns:
        Original DataFrame with added rolling average columns
    """
    windows = windows or [7, 28]

    # Input validation
    if value_col not in df.columns:
        raise ValidationError(f"Column '{value_col}' not found in DataFrame")

    # Create a copy of the DataFrame
    result_df = df.copy()

    # Set default min_periods if not provided
    if min_periods is None:
        min_periods = {w: w for w in windows}
    else:
        # Ensure all windows have min_periods
        for w in windows:
            if w not in min_periods:
                min_periods[w] = w

    # Calculate rolling averages for each window size
    for w in windows:
        col_name = f"rolling_avg_{w}"
        result_df[col_name] = result_df[value_col].rolling(window=w, min_periods=min_periods[w], center=center).mean()

    return result_df


def calculate_cumulative_growth(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "value",
    method: CumulativeGrowthMethod = CumulativeGrowthMethod.INDEX,
    base_index: float = 100.0,
    starting_date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """
    Transform a series into a cumulative index from a baseline.

    Family: time_series
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        date_col: Column name containing dates
        value_col: Column name containing values
        method: Method to calculate growth ('index', 'cumsum', 'cumprod')
        base_index: Starting index value when method='index'
        starting_date: Date to use as baseline; if None, uses the first date

    Returns:
        Original DataFrame with an added 'cumulative_growth' column
    """
    # Input validation
    if date_col not in df.columns:
        raise ValidationError(f"Column '{date_col}' not found in DataFrame")
    if value_col not in df.columns:
        raise ValidationError(f"Column '{value_col}' not found in DataFrame")

    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    # Filter to starting date if provided
    if starting_date is not None:
        starting_date = pd.to_datetime(starting_date)
        if starting_date not in df_sorted[date_col].values:
            raise ValidationError(f"Starting date {starting_date} not found in data")
        df_sorted = df_sorted[df_sorted[date_col] >= starting_date].copy()

    if df_sorted.empty:
        return df_sorted.copy()

    # Get base value for indexing
    base_value = df_sorted[value_col].iloc[0]

    if method == CumulativeGrowthMethod.INDEX:
        # Calculate index relative to first value
        if base_value == 0:
            df_sorted["cumulative_growth"] = np.nan
        else:
            df_sorted["cumulative_growth"] = df_sorted[value_col] / base_value * base_index

    elif method == CumulativeGrowthMethod.CUMSUM:
        # Running sum
        df_sorted["cumulative_growth"] = df_sorted[value_col].cumsum()

    elif method == CumulativeGrowthMethod.CUMPROD:
        # Calculate growth rates
        df_sorted["growth_rate"] = df_sorted[value_col].pct_change().fillna(0) + 1

        # Calculate cumulative product of growth rates
        df_sorted["cumulative_growth"] = df_sorted["growth_rate"].cumprod() * base_value

        # Remove temporary column
        df_sorted.drop("growth_rate", axis=1, inplace=True)

    return df_sorted


def calculate_slope_of_time_series(
    df: pd.DataFrame, date_col: str = "date", value_col: str = "value", normalize: bool = False
) -> TimeSeriesSlope:
    """
    Fit a linear regression to find the overall trend slope.

    Family: time_series
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        date_col: Column name containing dates
        value_col: Column name containing values
        normalize: Whether to normalize the slope as a percentage of the mean value

    Returns:
        TimeSeriesSlope object containing regression results:
        - 'slope': Slope coefficient (units per day or percentage per day if normalized)
        - 'intercept': Y-intercept value
        - 'r_value': Correlation coefficient
        - 'p_value': P-value for hypothesis test
        - 'std_err': Standard error
        - 'slope_per_day': Slope in units per day
        - 'slope_per_week': Slope in units per week
        - 'slope_per_month': Slope in units per month (30 days)
        - 'slope_per_year': Slope in units per year (365 days)

    Notes:
        When normalize=True, the slope is expressed as percentage change per day
        relative to the mean value of the series.
    """

    # Ensure df is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    # Create time index (days from first observation)
    first_date = df_sorted[date_col].min()
    df_sorted["time_index"] = (df_sorted[date_col] - first_date).dt.days

    # Get data without NaNs
    mask = ~df_sorted[["time_index", value_col]].isna().any(axis=1)
    if not mask.any() or len(df_sorted[mask]) < 2:
        return TimeSeriesSlope(
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

    # Run linear regression
    x = df_sorted.loc[mask, "time_index"].values
    y = df_sorted.loc[mask, value_col].values

    result = linregress(x, y)

    # Calculate normalized slope if requested
    slope_per_day = result.slope
    if normalize and np.mean(y) != 0:  # type: ignore
        slope_per_day = (slope_per_day / np.mean(y)) * 100  # type: ignore

    # Calculate different time scales
    slope_per_week = slope_per_day * 7
    slope_per_month = slope_per_day * 30
    slope_per_year = slope_per_day * 365

    return TimeSeriesSlope(
        slope=result.slope,
        intercept=result.intercept,
        r_value=result.rvalue,
        p_value=result.pvalue,
        std_err=result.stderr,
        slope_per_day=slope_per_day,
        slope_per_week=slope_per_week,
        slope_per_month=slope_per_month,
        slope_per_year=slope_per_year,
    )


def calculate_period_benchmarks(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "value",
    aggregator: str = "sum",
    benchmark_periods: list[PartialInterval] | None = None,
) -> list[BenchmarkComparison]:
    """
    Calculate benchmark comparisons between current and prior periods (WTD, MTD, QTD, YTD).

    Family: time_series
    Version: 1.0

    Args:
        df: DataFrame containing time series data
        date_col: Column name containing dates
        value_col: Column name containing values
        aggregator: How to aggregate values: 'sum', 'mean', 'median', 'min', 'max'
        benchmark_periods: List of period types to compare (default: ['WTD'] if grain is daily)

    Returns:
        List of BenchmarkComparison objects containing benchmark comparison details,
        - reference_period: str, the reference period
        - absolute_change: float, the absolute change
        - change_percent: float, the change percent
    """
    # Input validation
    if date_col not in df.columns:
        raise ValidationError(f"Column '{date_col}' not found in DataFrame")
    if value_col not in df.columns:
        raise ValidationError(f"Column '{value_col}' not found in DataFrame")

    # Check aggregator is valid
    valid_aggregators = {"sum", "mean", "median", "min", "max"}
    if aggregator not in valid_aggregators:
        raise ValidationError(f"Invalid aggregator: {aggregator}. Must be one of {valid_aggregators}")

    # Default benchmark periods if not specified
    if benchmark_periods is None:
        benchmark_periods = [PartialInterval.WTD]

    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    if df_sorted.empty:
        return []

    # Get the last date in the dataset
    last_date = df_sorted[date_col].max()

    benchmark_comparisons = []

    # Process each benchmark period type
    for period_type in benchmark_periods:
        # Skip if insufficient data for calculation
        if len(df_sorted) < 2:
            continue

        # Filter current period data based on period type
        if period_type == PartialInterval.WTD:
            # Monday of current week (day 0 is Monday)
            current_week_monday = last_date - pd.Timedelta(days=last_date.dayofweek)
            c_start = current_week_monday
            c_end = last_date

            # Prior WTD - shift by 7 days
            p_start = c_start - pd.Timedelta(days=7)
            p_end = c_end - pd.Timedelta(days=7)

        elif period_type == PartialInterval.MTD:
            # Current month-to-date
            c_start = last_date.replace(day=1)
            c_end = last_date

            # Prior month
            if c_start.month == 1:
                p_start = pd.Timestamp(year=c_start.year - 1, month=12, day=1)
            else:
                p_start = pd.Timestamp(year=c_start.year, month=c_start.month - 1, day=1)

            # Same day of month, or last day if prior month is shorter
            day_of_month = min(
                last_date.day, (p_start + pd.Timedelta(days=31)).replace(day=1) - pd.Timedelta(days=1)
            ).day
            p_end = p_start.replace(day=day_of_month)

        elif period_type == PartialInterval.QTD:
            # Current quarter-to-date
            quarter_month = ((last_date.month - 1) // 3) * 3 + 1
            c_start = pd.Timestamp(year=last_date.year, month=quarter_month, day=1)
            c_end = last_date

            # Days into quarter
            days_into_quarter = (last_date - c_start).days

            # Prior quarter
            if quarter_month == 1:
                p_start = pd.Timestamp(year=last_date.year - 1, month=10, day=1)
            else:
                p_start = pd.Timestamp(year=last_date.year, month=quarter_month - 3, day=1)

            p_end = p_start + pd.Timedelta(days=days_into_quarter)

        elif period_type == PartialInterval.YTD:
            # Current year-to-date
            c_start = pd.Timestamp(year=last_date.year, month=1, day=1)
            c_end = last_date

            # Prior year, same day of year
            p_start = pd.Timestamp(year=last_date.year - 1, month=1, day=1)
            day_of_year = min(
                last_date.dayofyear, 366 if pd.Timestamp(last_date.year - 1, 12, 31).is_leap_year else 365
            )
            p_end = p_start + pd.Timedelta(days=day_of_year - 1)

        # Filter dataframes for current and prior periods
        current_mask = (df_sorted[date_col] >= c_start) & (df_sorted[date_col] <= c_end)
        prior_mask = (df_sorted[date_col] >= p_start) & (df_sorted[date_col] <= p_end)

        current_df = df_sorted[current_mask]
        prior_df = df_sorted[prior_mask]

        # Skip if either period has no data
        if current_df.empty or prior_df.empty:
            continue

        # Apply aggregation
        agg_func = getattr(pd.Series, aggregator)
        current_value = agg_func(current_df[value_col])
        prior_value = agg_func(prior_df[value_col])

        # Calculate changes
        absolute_change = calculate_difference(current_value, prior_value)

        try:
            change_percent = calculate_percentage_difference(current_value, prior_value, handle_zero_reference=True)
        except Exception:
            change_percent = None

        # Add to results
        benchmark_comparisons.append(
            BenchmarkComparison(
                reference_period=period_type,
                absolute_change=absolute_change,
                change_percent=change_percent,
            )
        )

    return benchmark_comparisons
