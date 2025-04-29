"""
Dimensional Analysis Primitives
=============================================================================

This module provides functions for analyzing metrics across different dimensions:
- Slice aggregation and share calculations
- Ranking and comparison of dimension slices
- Composition changes and impact analysis
- Concentration and distribution metrics
- Key driver identification and attribution

Dependencies:
  - pandas as pd
  - numpy as np
"""

import numpy as np
import pandas as pd

from levers.exceptions import CalculationError, ValidationError
from levers.models import (
    ConcentrationMethod,
    HistoricalPeriodRanking,
    HistoricalSliceRankings,
    SliceComparison,
    SlicePerformance,
    SliceRanking,
    SliceShare,
    SliceStrength,
    TopSlice,
)
from levers.primitives import calculate_difference, calculate_relative_change, safe_divide

# =============================================================================
# Slice Metrics & Shares
# =============================================================================


def calculate_slice_metrics(
    df: pd.DataFrame,
    slice_col: str,
    value_col: str,
    agg_func: str = "sum",
    top_n: int | None = None,
    other_label: str = "Other",
    dropna_slices: bool = True,
) -> pd.DataFrame:
    """
    Group data by slice_col and aggregate value_col with the specified function.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df: Input DataFrame containing slice and value columns
        slice_col: Column name used for slicing/grouping
        value_col: Column name containing the metric values to aggregate
        agg_func: Aggregation function: 'sum', 'mean', 'min', 'max', 'count', or a custom function
        top_n: If provided, keep only the top_n slices and combine the rest into 'Other'
        other_label: Label to use for the combined smaller slices when top_n is used
        dropna_slices: Whether to exclude rows with NaN in slice_col

    Returns:
        DataFrame with columns [slice_col, "aggregated_value"] sorted by aggregated_value

    Raises:
        ValidationError: If slice_col or value_col not found in DataFrame or if agg_func is invalid
    """
    # Input validation
    if slice_col not in df.columns:
        raise ValidationError(
            f"slice_col '{slice_col}' not found in DataFrame",
            invalid_fields={"slice_col": slice_col, "available_columns": list(df.columns)},
        )
    if value_col not in df.columns:
        raise ValidationError(
            f"value_col '{value_col}' not found in DataFrame",
            invalid_fields={"value_col": value_col, "available_columns": list(df.columns)},
        )

    # Create a copy of the DataFrame
    dff = df.copy()

    # Drop rows with NaN in slice_col if requested
    if dropna_slices:
        dff = dff.dropna(subset=[slice_col])

    # Group by slice column and aggregate values
    valid_agg_funcs = {"sum", "mean", "min", "max", "count", "median"}
    if agg_func.lower() not in valid_agg_funcs:
        raise ValidationError(
            f"agg_func '{agg_func}' not recognized",
            {"agg_func": agg_func, "valid_agg_funcs": list(valid_agg_funcs)},
        )

    grouped = dff.groupby(slice_col)[value_col]
    try:
        result = grouped.agg(agg_func.lower()).reset_index(name="aggregated_value")
    except Exception as e:
        raise CalculationError(f"Error applying aggregation function '{agg_func}'", {"error": str(e)}) from e

    # Sort by aggregated value (descending)
    result.sort_values("aggregated_value", ascending=False, inplace=True, ignore_index=True)

    # Handle top_n if specified
    if top_n is not None and len(result) > top_n:
        top_slices = result.iloc[:top_n].copy()
        other_slices = result.iloc[top_n:]

        # Combine smaller slices into "Other"
        other_value = other_slices["aggregated_value"].sum()
        other_row = pd.DataFrame({slice_col: [other_label], "aggregated_value": [other_value]})

        # Combine and sort
        result = pd.concat([top_slices, other_row], ignore_index=True)
        result.sort_values("aggregated_value", ascending=False, inplace=True, ignore_index=True)

    return result


def compute_slice_shares(
    agg_df: pd.DataFrame, slice_col: str, val_col: str = "aggregated_value", share_col_name: str = "share_pct"
) -> pd.DataFrame:
    """
    Calculate each slice's percentage share of the total.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        agg_df: DataFrame containing aggregated values by slice
        slice_col: Column name for the slice/dimension
        val_col: Column containing the aggregated metric values
        share_col_name: Name of the resulting share percentage column

    Returns:
        DataFrame with an additional column for the percentage share

    Raises:
        ValidationError: If specified columns are not found in the DataFrame
    """
    # Input validation
    required_cols = [slice_col, val_col]
    for col in required_cols:
        if col not in agg_df.columns:
            raise ValidationError(
                f"Required column '{col}' not found in DataFrame",
                {"column": col, "available_columns": list(agg_df.columns)},
            )

    # Create a copy of the DataFrame
    dff = agg_df.copy()

    # Calculate total
    total = dff[val_col].sum()

    # Calculate share percentages
    if total == 0:
        dff[share_col_name] = 0.0
    else:
        # Convert each value to float and then calculate share percentage
        dff[share_col_name] = dff[val_col].astype(float).apply(lambda x: safe_divide(x, total, as_percentage=True))

    return dff


def rank_metric_slices(
    agg_df: pd.DataFrame, val_col: str = "aggregated_value", top_n: int = 5, ascending: bool = False
) -> pd.DataFrame:
    """
    Return the top or bottom slices sorted by value.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        agg_df: DataFrame with aggregated values
        val_col: Column containing the metric values
        top_n: Number of slices to return
        ascending: If True, returns the lowest slices; if False, returns the highest

    Returns:
        DataFrame with the ranked slices

    Raises:
        ValidationError: If val_col not found in DataFrame or top_n is invalid
    """
    # Input validation
    if val_col not in agg_df.columns:
        raise ValidationError(
            f"val_col '{val_col}' not found in DataFrame",
            {"val_col": val_col, "available_columns": list(agg_df.columns)},
        )
    if top_n <= 0:
        raise ValidationError("top_n must be a positive integer", {"top_n": top_n})

    # Create a copy and sort
    dff = agg_df.copy()
    dff.sort_values(val_col, ascending=ascending, inplace=True)

    # Return top_n rows
    return dff.head(top_n)


# =============================================================================
# Composition & Impact Analysis
# =============================================================================


def analyze_composition_changes(
    df_prior: pd.DataFrame, df_current: pd.DataFrame, slice_col: str, val_col: str = "aggregated_value"
) -> pd.DataFrame:
    """
    Compare values and percentage shares between two time periods (T0 and T1).

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df_prior: Aggregated data for time prior (earlier period)
        df_current: Aggregated data for time current (later period)
        slice_col: Column representing the slice/dimension
        val_col: Column with the aggregated metric values

    Returns:
        DataFrame comparing T0 and T1 with differences in values and shares

    Raises:
        ValidationError: If required columns are not found in the DataFrames

    Notes:
        The output contains:
        - slice_col: Dimension slice
        - {val_col}_prior: Value in period prior
        - {val_col}_current: Value in period current
        - share_pct_prior: Share percentage in prior
        - share_pct_current: Share percentage in current
        - abs_diff: Absolute difference (current - prior)
        - share_diff: Share difference (share_pct_current - share_pct_prior)
    """
    # Input validation
    required_cols = [slice_col, val_col]
    for col in required_cols:
        if col not in df_prior.columns or col not in df_current.columns:
            raise ValidationError(
                f"Required column '{col}' not found in one or both DataFrames",
                invalid_fields={
                    "slice_col": slice_col,
                    "prior_columns": list(df_prior.columns),
                    "current_columns": list(df_current.columns),
                },
            )

    # Ensure share percentages exist in both dataframes
    def ensure_share(df_in: pd.DataFrame) -> pd.DataFrame:
        if "share_pct" not in df_in.columns:
            return compute_slice_shares(df_in, slice_col, val_col=val_col)
        return df_in

    prior = ensure_share(df_prior.copy())
    current = ensure_share(df_current.copy())

    # Merge prior and current data
    merged = pd.merge(
        prior[[slice_col, val_col, "share_pct"]],
        current[[slice_col, val_col, "share_pct"]],
        on=slice_col,
        how="outer",
        suffixes=("_prior", "_current"),
    ).fillna(0)

    # Calculate differences
    merged["abs_diff"] = merged[f"{val_col}_current"] - merged[f"{val_col}_prior"]
    merged["share_diff"] = merged["share_pct_current"] - merged["share_pct_prior"]

    # Sort by absolute difference (descending)
    merged.sort_values("abs_diff", ascending=False, inplace=True, ignore_index=True)

    return merged


def analyze_dimension_impact(
    df_prior: pd.DataFrame, df_current: pd.DataFrame, dimension_col: str, value_col: str
) -> pd.DataFrame:
    """
    Analyze how each dimension slice contributed to the overall metric change.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df_prior: DataFrame for the comparison period (T0)
        df_current: DataFrame for the evaluation period (T1)
        dimension_col: Column representing the dimension slice
        value_col: Column with metric values

    Returns:
        DataFrame with columns showing each slice's contribution to overall change

    Raises:
        ValidationError: If required columns are not found in the DataFrames

    Notes:
        The output contains:
        - dimension_col: The dimension slice
        - {value_col}_prior: Value in period prior
        - {value_col}_current: Value in period current
        - delta: Absolute change (current - prior)
        - share_of_total_delta: Percentage contribution to overall change
    """
    # Input validation
    if dimension_col not in df_prior.columns or dimension_col not in df_current.columns:
        raise ValidationError(
            f"dimension_col '{dimension_col}' not found in one or both DataFrames",
            invalid_fields={
                "dimension_col": dimension_col,
                "prior_columns": list(df_prior.columns),
                "current_columns": list(df_current.columns),
            },
        )
    if value_col not in df_prior.columns or value_col not in df_current.columns:
        raise ValidationError(
            f"value_col '{value_col}' not found in one or both DataFrames",
            invalid_fields={
                "value_col": value_col,
                "prior_columns": list(df_prior.columns),
                "current_columns": list(df_current.columns),
            },
        )

    # Prepare DataFrames
    prior = df_prior.copy()
    current = df_current.copy()

    # Rename columns for clarity
    prior.rename(columns={value_col: f"{value_col}_prior"}, inplace=True)
    current.rename(columns={value_col: f"{value_col}_current"}, inplace=True)

    # Merge prior and current data
    merged = pd.merge(
        prior[[dimension_col, f"{value_col}_prior"]],
        current[[dimension_col, f"{value_col}_current"]],
        on=dimension_col,
        how="outer",
    ).fillna(0)

    # Calculate delta and share of total delta
    merged["delta"] = merged[f"{value_col}_current"] - merged[f"{value_col}_prior"]
    total_delta = merged["delta"].sum()

    # Calculate each slice's contribution to total delta
    merged["share_of_total_delta"] = merged["delta"].apply(
        lambda d: safe_divide(d, total_delta, default_value=0.0, as_percentage=True) or 0.0
    )

    # Sort by delta (descending)
    merged.sort_values("delta", ascending=False, inplace=True, ignore_index=True)

    return merged


def calculate_concentration_index(
    df: pd.DataFrame,
    val_col: str = "aggregated_value",
    method: ConcentrationMethod = ConcentrationMethod.HHI,
) -> float:
    """
    Calculate a concentration index to measure distribution inequality.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df: DataFrame with aggregated metric values
        val_col: Column containing the values
        method: Method to use: "HHI" (Herfindahl-Hirschman Index) or "gini" (Gini coefficient)

    Returns:
        The concentration index value (0 to 1 scale)

    Raises:
        ValidationError: If val_col not found in DataFrame or method is invalid
        CalculationError: If calculation fails due to invalid values

    Notes:
        - HHI is the sum of squared market shares (0=perfect equality, 1=monopoly)
        - Gini coefficient measures inequality (0=perfect equality, 1=perfect inequality)
    """
    # Input validation
    if val_col not in df.columns:
        raise ValidationError(
            f"val_col '{val_col}' not found in DataFrame", {"val_col": val_col, "available_columns": list(df.columns)}
        )

    # Handle empty dataframe
    if df.empty or df[val_col].sum() <= 0:
        return 0.0

    if method not in [ConcentrationMethod.HHI, ConcentrationMethod.GINI]:
        raise ValidationError(
            f"Unknown concentration method: {method}",
            invalid_fields={"method": method, "valid_methods": [ConcentrationMethod.HHI, ConcentrationMethod.GINI]},
        )

    if method == ConcentrationMethod.HHI:
        # HHI = sum of squared market shares
        total = df[val_col].sum()
        if total <= 0:
            return 0.0

        # Use safe_divide to calculate shares
        shares = df[val_col].apply(lambda x: safe_divide(x, total, default_value=0.0))
        return float(np.sum(shares**2))
    elif method == ConcentrationMethod.GINI:
        # Gini coefficient calculation
        values = df[val_col].to_numpy()
        if np.any(values < 0):
            raise CalculationError("Negative values not allowed for Gini coefficient", details={"values": values})

        # Sort values
        sorted_vals = np.sort(values)
        n = len(sorted_vals)
        if n <= 1:
            return 0.0

        # If all values are equal, Gini coefficient is 0
        if np.all(sorted_vals == sorted_vals[0]):
            return 0.0

        # Cumulative proportion of the population (x-axis)
        cum_people = np.arange(1, n + 1) / n

        # Cumulative proportion of income (y-axis)
        cum_income = np.cumsum(sorted_vals) / np.sum(sorted_vals)

        # Gini coefficient using area under Lorenz curve
        B = np.trapz(cum_income, cum_people)  # Area under the Lorenz curve  # noqa
        gini = 1 - 2 * B  # Gini = 1 - 2B

        return float(gini)


# =============================================================================
# Time Comparison & Period Calculation
# =============================================================================


# TODO: check for type of prior_start_date and current_start_date
def compare_dimension_slices_over_time(
    df: pd.DataFrame,
    slice_col: str,
    date_col: str = "date",
    value_col: str = "value",
    prior_start_date: str | pd.Timestamp | None = None,
    current_start_date: str | pd.Timestamp | None = None,
    agg: str = "sum",
) -> pd.DataFrame:
    """
    Compare each dimension slice between two time points.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df: Input DataFrame with time series data
        slice_col: Column representing the dimension slice
        date_col: Column containing date values
        value_col: Column with metric values
        prior_start_date: First time point (defaults to the earliest date)
        current_start_date: Second time point (defaults to the latest date)
        agg: Aggregation function to apply ('sum', 'mean', 'count', etc.)

    Returns:
        DataFrame comparing values at prior_start_date and current_start_date

    Raises:
        ValidationError: If required columns are not found in DataFrame or agg is invalid

    Notes:
        The output contains:
        - slice_col: Dimension slice
        - val_prior: Value at prior_start_date
        - val_current: Value at current_start_date
        - abs_diff: Absolute difference (current_start_date - prior_start_date)
        - pct_diff: Percentage difference
    """
    required_cols = [slice_col, date_col, value_col]
    for col in required_cols:
        if col not in df.columns:
            raise ValidationError(
                f"Required column '{col}' not found in DataFrame",
                {"column": col, "available_columns": list(df.columns)},
            )

    # Create a copy and convert date column to datetime
    dff = df.copy()
    dff[date_col] = pd.to_datetime(dff[date_col])

    # Determine prior_start_date and current_start_date if not provided
    prior_start_date = prior_start_date or dff[date_col].min()
    current_start_date = current_start_date or dff[date_col].max()

    prior_start_date = pd.to_datetime(prior_start_date)
    current_start_date = pd.to_datetime(current_start_date)

    # Filter data for prior_start_date and current_start_date
    df_prior = dff[dff[date_col] == prior_start_date]
    df_current = dff[dff[date_col] == current_start_date]

    # Check if any of the filtered dataframes is empty
    if df_prior.empty or df_current.empty:
        raise ValidationError(
            "No data found for the specified dates",
            {"prior_start_date": prior_start_date, "current_start_date": current_start_date},
        )

    # Validate aggregation method
    valid_agg_funcs = {"sum", "mean", "min", "max", "count", "median"}
    if agg not in valid_agg_funcs:
        raise ValidationError(
            f"Unsupported aggregation method: {agg}",
            invalid_fields={"agg": agg, "valid_agg_funcs": list(valid_agg_funcs)},
        )

    # Aggregate by slice for prior_start_date and current_start_date
    agg_prior = df_prior.groupby(slice_col)[value_col].agg(agg).reset_index().rename(columns={value_col: "val_prior"})
    agg_current = (
        df_current.groupby(slice_col)[value_col].agg(agg).reset_index().rename(columns={value_col: "val_current"})
    )

    # Merge prior_start_date and current_start_date data
    merged = pd.merge(agg_prior, agg_current, on=slice_col, how="outer").fillna(0)

    # Calculate differences
    merged["abs_diff"] = merged["val_current"] - merged["val_prior"]

    # Calculate percentage difference individually for each row to handle None values properly
    merged["pct_diff"] = merged.apply(  # type: ignore
        lambda row: (
            calculate_relative_change(row["val_current"], row["val_prior"], default_value=0) * 100.0  # type: ignore
            if row["val_prior"] != 0
            else 0
        ),
        axis=1,
    )

    # Sort by absolute difference (descending)
    merged.sort_values("abs_diff", ascending=False, inplace=True)
    merged.reset_index(drop=True, inplace=True)

    return merged


# =============================================================================
# Comparative Analysis
# =============================================================================


def difference_from_average(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """
    Add columns showing difference of each slice from the average of all other slices.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df: DataFrame with slice metrics
        value_col: Column containing values to compare

    Returns:
        DataFrame with additional columns:
        - avg_other_slices_value: Average value of all other slices
        - absolute_diff_from_avg: Absolute difference from avg_other_slices_value
        - absolute_diff_percent_from_avg: Percentage difference from avg_other_slices_value

    Raises:
        ValidationError: If value_col not found in DataFrame
    """
    if value_col not in df.columns:
        raise ValidationError(
            f"value_col '{value_col}' not found in DataFrame",
            invalid_fields={"value_col": value_col, "available_columns": list(df.columns)},
        )

    result_df = df.copy()
    result_df["avg_other_slices_value"] = np.nan
    result_df["absolute_diff_from_avg"] = np.nan
    result_df["absolute_diff_percent_from_avg"] = np.nan

    if df.empty:
        return result_df

    total = df[value_col].sum()
    n_slices = len(df)
    # Calculate avg_other_slices_value, absolute_diff_from_avg, and absolute_diff_percent_from_avg
    for i in range(n_slices):
        row_val = result_df.iloc[i][value_col]
        if n_slices > 1:
            sum_others = total - row_val
            avg_others = sum_others / (n_slices - 1)
            abs_diff = calculate_difference(row_val, avg_others)  # type: ignore
            pct_diff = calculate_relative_change(row_val, avg_others) * 100.0  # type: ignore

            result_df.at[result_df.index[i], "avg_other_slices_value"] = avg_others
            result_df.at[result_df.index[i], "absolute_diff_from_avg"] = abs_diff
            result_df.at[result_df.index[i], "absolute_diff_percent_from_avg"] = pct_diff
        else:
            result_df.at[result_df.index[i], "avg_other_slices_value"] = np.nan
            result_df.at[result_df.index[i], "absolute_diff_from_avg"] = np.nan
            result_df.at[result_df.index[i], "absolute_diff_percent_from_avg"] = np.nan

    return result_df


def compute_top_bottom_slices(
    df: pd.DataFrame, dim_col: str, value_col: str, top_n: int = 3, include_avg_comparison: bool = True
) -> tuple[list[SliceRanking], list[SliceRanking]]:
    """
    Compute both top and bottom slices based on provided value column.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df: DataFrame with slices and values
        dim_col: Name of the dimension column
        value_col: Column containing the metric values
        top_n: Number of top/bottom slices to compute
        include_avg_comparison: Whether to compute comparisons with average of other slices

    Returns:
        Tuple of (top_slices, bottom_slices) as lists of TopSlicePerformance objects

    Raises:
        ValidationError: If value_col not found in DataFrame
    """
    # Input validation
    required_cols = [dim_col, value_col]
    for col in required_cols:
        if col not in df.columns:
            raise ValidationError(
                f"Required column '{col}' not found in DataFrame",
                {"column": col, "available_columns": list(df.columns)},
            )

    # Sort for top/bottom
    sorted_df = df.sort_values(by=value_col, ascending=False)

    # Add relative comparison to average if requested
    if include_avg_comparison:
        sorted_df = compute_slice_vs_avg_others(sorted_df, value_col)

    # Extract top slices
    top_slices = []
    for i in range(min(top_n, len(sorted_df))):
        row = sorted_df.iloc[i]
        slice_perf = SliceRanking(
            dimension=dim_col,
            slice_value=str(row[dim_col]),
            metric_value=float(row[value_col]),
            avg_other_slices_value=float(row.get("avg_other_slices_value", 0.0)),
            absolute_diff_from_avg=float(row.get("absolute_diff_from_avg", 0.0)),
            absolute_diff_percent_from_avg=float(row.get("absolute_diff_percent_from_avg", 0.0)),
            rank=i + 1,
        )
        top_slices.append(slice_perf)

    # Extract bottom slices
    bottom_df = sorted_df.sort_values(by=value_col, ascending=True)
    bottom_slices = []
    for i in range(min(top_n, len(bottom_df))):
        row = bottom_df.iloc[i]
        slice_perf = SliceRanking(
            dimension=dim_col,
            slice_value=str(row[dim_col]),
            metric_value=float(row[value_col]),
            avg_other_slices_value=float(row.get("avg_other_slices_value", 0.0)),
            absolute_diff_from_avg=float(row.get("absolute_diff_from_avg", 0.0)),
            absolute_diff_percent_from_avg=float(row.get("absolute_diff_percent_from_avg", 0.0)),
            rank=len(bottom_df) - i,
        )
        bottom_slices.append(slice_perf)

    return top_slices, bottom_slices


def identify_largest_smallest_by_share(
    df_current: pd.DataFrame, df_prior: pd.DataFrame, slice_col: str, share_col: str = "share_pct"
) -> tuple[SliceShare | None, SliceShare | None]:
    """
    Identify largest and smallest slices by share in the current period.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df_current: DataFrame with current period data
        df_prior: DataFrame with prior period data
        slice_col: Column containing slice names
        share_col: Column containing share percentages

    Returns:
        Tuple of (largest_slice, smallest_slice) as SliceShare objects

    Raises:
        ValidationError: If required columns are not found
    """
    # Validation
    if slice_col not in df_current.columns or slice_col not in df_prior.columns:
        raise ValidationError(
            f"slice_col '{slice_col}' not found in one or both DataFrames",
            invalid_fields={
                "slice_col": slice_col,
                "current_columns": list(df_current.columns),
                "prior_columns": list(df_prior.columns),
            },
        )
    if share_col not in df_current.columns or share_col not in df_prior.columns:
        raise ValidationError(
            f"share_col '{share_col}' not found in one or both DataFrames",
            invalid_fields={
                "share_col": share_col,
                "current_columns": list(df_current.columns),
                "prior_columns": list(df_prior.columns),
            },
        )

    # Handle empty dataframes
    if df_current.empty or df_prior.empty:
        return None, None

    # Sort current and prior by share
    df_current_desc = df_current.sort_values(share_col, ascending=False).reset_index(drop=True)
    df_prior_desc = df_prior.sort_values(share_col, ascending=False).reset_index(drop=True)
    df_current_asc = df_current.sort_values(share_col, ascending=True).reset_index(drop=True)
    df_prior_asc = df_prior.sort_values(share_col, ascending=True).reset_index(drop=True)

    # Create largest slice
    largest_slice = SliceShare(
        slice_value=df_current_desc.iloc[0][slice_col],
        current_share_of_volume_percent=float(df_current_desc.iloc[0][share_col]),
        previous_slice_value=df_prior_desc.iloc[0][slice_col],
        previous_share_percent=float(df_prior_desc.iloc[0][share_col]),
    )

    # Create smallest slice
    smallest_slice = SliceShare(
        slice_value=df_current_asc.iloc[0][slice_col],
        current_share_of_volume_percent=float(df_current_asc.iloc[0][share_col]),
        previous_slice_value=df_prior_asc.iloc[0][slice_col],
        previous_share_percent=float(df_prior_asc.iloc[0][share_col]),
    )

    return largest_slice, smallest_slice


def identify_strongest_weakest_changes(
    df: pd.DataFrame, slice_col: str, current_val_col: str = "val_current", prior_val_col: str = "val_prior"
) -> tuple[SliceStrength | None, SliceStrength | None]:
    """
    Identify new strongest and weakest slices compared to prior period.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df: DataFrame with current and prior values
        slice_col: Column containing slice names
        current_val_col: Column with current period values
        prior_val_col: Column with prior period values

    Returns:
        Tuple of (strongest_slice, weakest_slice) as SliceStrength objects, or None if no change

    Raises:
        ValidationError: If required columns are not found
    """
    # Input validation
    required_cols = [slice_col, current_val_col, prior_val_col]
    for col in required_cols:
        if col not in df.columns:
            raise ValidationError(
                f"Required column '{col}' not found in DataFrame",
                {"column": col, "available_columns": list(df.columns)},
            )

    # Handle empty dataframe
    if df.empty:
        return None, None

    # Sort by current and prior values
    df_current_desc = df.sort_values(current_val_col, ascending=False).reset_index(drop=True)
    df_prior_desc = df.sort_values(prior_val_col, ascending=False).reset_index(drop=True)
    df_current_asc = df.sort_values(current_val_col, ascending=True).reset_index(drop=True)
    df_prior_asc = df.sort_values(prior_val_col, ascending=True).reset_index(drop=True)

    # Initialize result variables
    strongest_slice = None
    weakest_slice = None

    # Process strongest
    curr_strongest = df_current_desc.iloc[0][slice_col]
    prior_strongest = df_prior_desc.iloc[0][slice_col]

    if curr_strongest != prior_strongest:
        # Get the row for the new strongest slice
        curr_strongest_row = df[df[slice_col] == curr_strongest].iloc[0]

        strongest_slice = SliceStrength(
            slice_value=curr_strongest,
            previous_slice_value=prior_strongest,
            current_value=float(curr_strongest_row[current_val_col]),
            prior_value=float(curr_strongest_row[prior_val_col]),
            absolute_delta=float(curr_strongest_row[current_val_col] - curr_strongest_row[prior_val_col]),
            relative_delta_percent=float(
                calculate_relative_change(curr_strongest_row[current_val_col], curr_strongest_row[prior_val_col])  # type: ignore
                * 100.0  # type: ignore
            ),
        )

    # Process weakest
    curr_weakest = df_current_asc.iloc[0][slice_col]
    prior_weakest = df_prior_asc.iloc[0][slice_col]

    if curr_weakest != prior_weakest:
        # Get the row for the new weakest slice
        curr_weakest_row = df[df[slice_col] == curr_weakest].iloc[0]

        weakest_slice = SliceStrength(
            slice_value=curr_weakest,
            previous_slice_value=prior_weakest,
            current_value=float(curr_weakest_row[current_val_col]),
            prior_value=float(curr_weakest_row[prior_val_col]),
            absolute_delta=float(curr_weakest_row[current_val_col] - curr_weakest_row[prior_val_col]),
            relative_delta_percent=float(
                calculate_relative_change(curr_weakest_row[current_val_col], curr_weakest_row[prior_val_col]) * 100.0  # type: ignore
            ),
        )

    return strongest_slice, weakest_slice


def highlight_slice_comparisons(
    df: pd.DataFrame,
    slice_col: str,
    current_val_col: str = "val_current",
    prior_val_col: str = "val_prior",
    top_n: int = 2,
) -> list[SliceComparison]:
    """
    Compare top slices and calculate performance gaps.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df: DataFrame with slice data
        slice_col: Column name for slices
        current_val_col: Column with current period values
        prior_val_col: Column with prior period values
        top_n: Number of top slices to compare (usually 2)

    Returns:
        List of SliceComparison objects between pairs of slices

    Raises:
        ValidationError: If required columns are not found
    """
    # Input validation
    required_cols = [slice_col, current_val_col, prior_val_col]
    for col in required_cols:
        if col not in df.columns:
            raise ValidationError(
                f"Required column '{col}' not found in DataFrame",
                {"column": col, "available_columns": list(df.columns)},
            )

    # Handle case with insufficient slices
    if len(df) < top_n:
        return []

    # Sort by current values
    df_desc = df.sort_values(current_val_col, ascending=False).reset_index(drop=True)

    # Get top slices
    slices = []
    for i in range(top_n):
        slices.append(df_desc.iloc[i])

    # Compare slices in pairs
    comparisons = []
    for i in range(len(slices) - 1):
        for j in range(i + 1, len(slices)):
            slice_a = slices[i]
            slice_b = slices[j]

            # Calculate current gap
            gap_now = None
            if slice_b[current_val_col] != 0:
                gap_now = (slice_a[current_val_col] - slice_b[current_val_col]) / slice_b[current_val_col] * 100

            # Calculate prior gap
            gap_prior = None
            if slice_b[prior_val_col] != 0:
                gap_prior = (slice_a[prior_val_col] - slice_b[prior_val_col]) / slice_b[prior_val_col] * 100

            # Calculate gap change
            gap_change = None
            if gap_now is not None and gap_prior is not None:
                gap_change = gap_now - gap_prior

            # Create comparison as SliceComparison model
            comparison = SliceComparison(
                slice_a=str(slice_a[slice_col]),
                current_value_a=float(slice_a[current_val_col]),
                prior_value_a=float(slice_a[prior_val_col]),
                slice_b=str(slice_b[slice_col]),
                current_value_b=float(slice_b[current_val_col]),
                prior_value_b=float(slice_b[prior_val_col]),
                performance_gap_percent=float(gap_now) if gap_now is not None else None,
                gap_change_percent=float(gap_change) if gap_change is not None else None,
            )

            comparisons.append(comparison)

    return comparisons


def compute_historical_slice_rankings(
    df: pd.DataFrame,
    slice_col: str,
    date_col: str,
    value_col: str,
    num_periods: int = 8,
    period_length_days: int = 7,
    top_n: int = 5,
) -> HistoricalSliceRankings | None:
    """
    Analyze top slice rankings over multiple periods.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df: DataFrame with historical time series data
        slice_col: Column for dimension slices
        date_col: Column containing dates
        value_col: Column with metric values
        num_periods: Number of periods to analyze
        period_length_days: Length of each period in days
        top_n: Number of top slices to include in each period

    Returns:
        HistoricalSliceRankings object with period rankings

    Raises:
        ValidationError: If required columns are not found
    """
    # Validation
    for col in [slice_col, date_col, value_col]:
        if col not in df.columns:
            raise ValidationError(
                f"Column '{col}' not found in DataFrame", {"column": col, "available_columns": list(df.columns)}
            )

    # Prepare dataframe
    dff = df.copy()
    dff[date_col] = pd.to_datetime(dff[date_col])

    # Handle empty dataframe
    if dff.empty:
        return None

    # Get latest date
    end_of_latest = dff[date_col].max()
    period_rankings = []
    current_end = end_of_latest

    # Analyze each period
    for _ in range(num_periods):
        period_start = (current_end - pd.Timedelta(days=period_length_days - 1)).normalize()
        mask = (dff[date_col] >= period_start) & (dff[date_col] <= current_end)
        period_df = dff[mask]

        if period_df.empty:
            continue

        # Aggregate by slice
        agg_df = period_df.groupby(slice_col)[value_col].sum().reset_index()
        agg_df.sort_values(value_col, ascending=False, inplace=True)

        # Get top_n slices
        top_slices = agg_df.head(top_n)

        # Format for output
        top_slices_list = []
        for _, row in top_slices.iterrows():
            top_slices_list.append(
                TopSlice(dimension=slice_col, slice_value=str(row[slice_col]), metric_value=float(row[value_col]))
            )

        # Add period info
        period_rankings.append(
            HistoricalPeriodRanking(
                start_date=str(period_start.date()),
                end_date=str(current_end.date()),
                top_slices_by_performance=top_slices_list,
            )
        )

        # Move to the previous period
        current_end = period_start - pd.Timedelta(days=1)

    # Reverse to get chronological order
    period_rankings.reverse()

    return HistoricalSliceRankings(periods_analyzed=len(period_rankings), period_rankings=period_rankings)


def build_slices_performance_list(
    df: pd.DataFrame,
    slice_col: str,
    current_val_col: str = "val_current",
    prior_val_col: str = "val_prior",
    abs_diff_col: str = "abs_diff",
    pct_diff_col: str = "pct_diff",
    include_shares: bool = True,
    include_ranks: bool = True,
    include_avg_comparison: bool = True,
) -> list[SlicePerformance]:
    """
    Convert dataframe rows into a structured slices list for reporting.

    Family: dimensional_analysis
    Version: 1.0

    Args:
        df: DataFrame with slice data
        slice_col: Column name for slices
        current_val_col: Column with current period values
        prior_val_col: Column with prior period values
        abs_diff_col: Column with absolute difference
        pct_diff_col: Column with percentage difference
        include_shares: Whether to include share columns if present
        include_ranks: Whether to include rank columns if present
        include_avg_comparison: Whether to include average comparison if present

    Returns:
        List of SlicePerformanceMetrics objects

    Raises:
        ValidationError: If required columns are not found
    """
    # Input validation
    required_cols = [slice_col, current_val_col, prior_val_col]
    for col in required_cols:
        if col not in df.columns:
            raise ValidationError(
                f"Required column '{col}' not found in DataFrame",
                {"column": col, "available_columns": list(df.columns)},
            )

    slices_list = []

    # Process each row
    for _, row in df.iterrows():
        slice_dict = SlicePerformance(
            slice_value=row[slice_col],
            current_value=row[current_val_col],
            prior_value=row[prior_val_col],
        )

        # Add differences if available
        if abs_diff_col in df.columns:
            slice_dict.absolute_change = row[abs_diff_col]
        else:
            slice_dict.absolute_change = calculate_difference(row[current_val_col], row[prior_val_col])

        if pct_diff_col in df.columns:
            slice_dict.relative_change_percent = row[pct_diff_col]
        else:
            slice_dict.relative_change_percent = (
                calculate_relative_change(row[current_val_col], row[prior_val_col]) * 100.0  # type: ignore
            )

        # Add shares if requested and available
        if include_shares:
            if "share_pct_current" in df.columns:
                slice_dict.current_share_of_volume_percent = row["share_pct_current"]
            if "share_pct_prior" in df.columns:
                slice_dict.prior_share_of_volume_percent = row["share_pct_prior"]
            if "share_diff" in df.columns:
                slice_dict.share_of_volume_change_percent = row["share_diff"]

        # Add marginal impact if available
        slice_dict.absolute_marginal_impact = slice_dict.absolute_change

        # Add average comparison if requested and available
        if include_avg_comparison:
            avg_cols = ["avg_other_slices_value", "absolute_diff_from_avg", "absolute_diff_percent_from_avg"]
            for col in avg_cols:
                if col in df.columns:
                    slice_dict[col] = row[col]  # type: ignore

        # Add ranks if requested and available
        if include_ranks:
            if "rank_performance" in df.columns:
                slice_dict.rank_by_performance = row["rank_performance"]
            if "rank_share" in df.columns:
                slice_dict.rank_by_share = row["rank_share"]

        slices_list.append(slice_dict)

    return slices_list


def compute_slice_vs_avg_others(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """
    Compute comparison of each slice's value against the average of all other slices.

    Args:
        df: DataFrame with slices and values
        value_col: Column containing the metric values

    Returns:
        DataFrame with additional columns for average comparisons
    """
    if df.empty:
        return df

    result_df = df.copy()

    # Get dimension column (first column that is not value_col)
    dim_col = [col for col in df.columns if col != value_col][0]

    # For each row, compute average of all other rows
    for idx, row in result_df.iterrows():
        current_slice = row[dim_col]
        others_mask = result_df[dim_col] != current_slice

        if others_mask.any():
            avg_others = result_df.loc[others_mask, value_col].mean()
            abs_diff = calculate_difference(row[value_col], avg_others)  # type: ignore
            pct_diff = calculate_relative_change(row[value_col], avg_others) * 100.0  # type: ignore

            result_df.at[idx, "avg_other_slices_value"] = avg_others  # type: ignore
            result_df.at[idx, "absolute_diff_from_avg"] = abs_diff  # type: ignore
            result_df.at[idx, "absolute_diff_percent_from_avg"] = pct_diff  # type: ignore

    return result_df
