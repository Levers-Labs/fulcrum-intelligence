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
    ComparisonType,
    CumulativeGrowthMethod,
    DataFillMethod,
    Granularity,
    TimeSeriesSlope,
)
from levers.models.patterns import Benchmark, BenchmarkComparison
from levers.primitives import calculate_difference, calculate_percentage_difference, get_prev_period_start_date

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


def _get_period_value(
    df_sorted: pd.DataFrame,
    reference_start: pd.Timestamp,
    date_col: str = "date",
    value_col: str = "value",
) -> float | None:
    """Get value for a specific period from time series data."""
    reference_date = reference_start.date()
    matching_row = df_sorted[df_sorted[date_col].dt.date == reference_date]

    # If no matching row, return None
    if matching_row.empty:
        return None

    # Return the value of the matching row
    return matching_row[value_col].iloc[0]


def _get_benchmark_configs(grain: str) -> list[dict]:
    """Get benchmark configurations for a given grain."""
    if grain == Granularity.WEEK:
        return [
            {
                "comparison_type": ComparisonType.LAST_WEEK,
                "label": "Last Week",
                "periods_back": 1,
                "grain": Granularity.WEEK,
            },
            {
                "comparison_type": ComparisonType.WEEK_IN_LAST_MONTH,
                "label": "Month Ago",
                "periods_back": 1,
                "grain": Granularity.MONTH,
            },
            {
                "comparison_type": ComparisonType.WEEK_IN_LAST_QUARTER,
                "label": "Quarter Ago",
                "periods_back": 1,
                "grain": Granularity.QUARTER,
            },
            {
                "comparison_type": ComparisonType.WEEK_IN_LAST_YEAR,
                "label": "Year Ago",
                "periods_back": 1,
                "grain": Granularity.YEAR,
            },
        ]
    elif grain == Granularity.MONTH:
        return [
            {
                "comparison_type": ComparisonType.LAST_MONTH,
                "label": "Last Month",
                "periods_back": 1,
                "grain": Granularity.MONTH,
            },
            {
                "comparison_type": ComparisonType.MONTH_IN_LAST_QUARTER,
                "label": "Quarter Ago",
                "periods_back": 1,
                "grain": Granularity.QUARTER,
            },
            {
                "comparison_type": ComparisonType.MONTH_IN_LAST_YEAR,
                "label": "Year Ago",
                "periods_back": 1,
                "grain": Granularity.YEAR,
            },
        ]
    return []


def _calculate_benchmark(
    df_sorted: pd.DataFrame,
    config: dict,
    current_value: float,
    latest_date: pd.Timestamp,
    date_col: str = "date",
    value_col: str = "value",
) -> Benchmark | None:
    """Calculate a single benchmark comparison based on configuration."""
    # Calculate reference period start date using period_grains primitive
    reference_start_date = get_prev_period_start_date(
        grain=Granularity(config["grain"]), period_count=config["periods_back"], latest_start_date=latest_date
    )

    # Convert to timestamp for consistency
    reference_start = pd.Timestamp(reference_start_date)

    # Get the reference value
    reference_value = _get_period_value(df_sorted, reference_start, date_col, value_col)

    # If no reference value, return None
    if reference_value is None:
        return None

    # Calculate absolute and percentage change
    absolute_change = calculate_difference(current_value, reference_value)
    change_percent = calculate_percentage_difference(current_value, reference_value, handle_zero_reference=True)

    # Create the benchmark object
    return Benchmark(
        reference_value=reference_value,
        reference_date=reference_start.date(),
        reference_period=config["label"],
        absolute_change=absolute_change,
        change_percent=change_percent,
    )


def calculate_benchmark_comparisons(
    df: pd.DataFrame, grain: Granularity, date_col: str = "date", value_col: str = "value"
) -> BenchmarkComparison | None:
    """
    Calculate benchmark comparisons for a given grain and DataFrame.

    Family: time_series
    Version: 1.0

    Args:
        df: Time series data with period start dates
        grain: Time grain for analysis (day, week, month)
        date_col: Column name containing period start dates
        value_col: Column name containing values

    Returns:
        BenchmarkComparison object containing benchmark comparison details with:
        - current_value: The current period's value
        - current_period: The current period's string representation
        - benchmarks: A dictionary of Benchmark objects, keyed by ComparisonType

        Returns None if insufficient data or unsupported grain (day grain is disabled)
    """
    # Disable for day grain as it is not supported
    if grain == Granularity.DAY:
        return None

    # Ensure data is sorted by date
    df_sorted = validate_date_sorted(df, date_col)

    if df_sorted.empty:
        return None

    # Get the latest date in the dataset (should be a period start date)
    latest_date = df_sorted[date_col].max().date()

    # Get the current value
    current_value = _get_period_value(df_sorted, pd.Timestamp(latest_date), date_col, value_col)
    if current_value is None:
        return None

    # Determine current period description
    if grain == Granularity.WEEK:
        current_period = "This Week"
    elif grain == Granularity.MONTH:
        current_period = "This Month"
    else:
        current_period = f"This {grain.value.title()}"

    # Create the main comparison object
    benchmark_comparison = BenchmarkComparison(current_value=current_value, current_period=current_period)

    # Define benchmark configurations for each grain
    benchmark_configs = _get_benchmark_configs(grain)
    if not benchmark_configs:
        return None

    # Calculate all comparisons
    for config in benchmark_configs:
        benchmark = _calculate_benchmark(
            df_sorted, config, current_value, pd.Timestamp(latest_date), date_col, value_col
        )
        if benchmark:
            benchmark_comparison.add_benchmark(config["comparison_type"], benchmark)

    return benchmark_comparison


def calculate_overall_growth(
    df: pd.DataFrame,
    value_col: str = "value",
) -> float:
    """Calculate the overall growth of a time series."""
    if len(df) < 2:
        return 0.0
    return calculate_percentage_difference(df[value_col].iloc[-1], df[value_col].iloc[0])
