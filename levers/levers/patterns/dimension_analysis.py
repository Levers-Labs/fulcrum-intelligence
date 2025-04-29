"""
Dimension Analysis Pattern

This pattern performs slice-level analysis for a given metric and dimension at
a specified time grain (day, week, or month). It compares the current period
to the immediately prior period, calculates share of total metric volume,
performance deltas, ranks slices, identifies top and bottom slices, and more.

Dependencies:
  - pandas as pd
"""

import logging

import pandas as pd

from levers.exceptions import PatternError, TimeRangeError
from levers.models import (
    AnalysisWindow,
    AnalysisWindowConfig,
    DataSource,
    DataSourceType,
    PatternConfig,
    WindowStrategy,
)
from levers.models.patterns import DimensionAnalysis
from levers.patterns.base import Pattern
from levers.primitives import (
    build_slices_performance_list,
    compare_dimension_slices_over_time,
    compute_historical_slice_rankings,
    compute_slice_shares,
    compute_top_bottom_slices,
    difference_from_average,
    get_period_length_for_grain,
    get_period_range_for_grain,
    get_prior_period_range,
    highlight_slice_comparisons,
    identify_largest_smallest_by_share,
    identify_strongest_weakest_changes,
)

logger = logging.getLogger(__name__)


class DimensionAnalysisPattern(Pattern[DimensionAnalysis]):
    """
    Pattern for analyzing metrics across dimensions, comparing current vs. prior periods,
    and identifying key insights about slice performance and distribution changes.
    """

    name = "dimension_analysis"
    description = "Analyzes metrics across dimensions to identify top/bottom slices and changes over time"
    version = "1.0.0"
    required_primitives = [
        "compare_dimension_slices_over_time",
        "compute_slice_shares",
        "difference_from_average",
        "build_slices_performance_list",
        "compute_top_bottom_slices",
        "identify_largest_smallest_by_share",
        "identify_strongest_weakest_changes",
        "highlight_slice_comparisons",
        "compute_historical_slice_rankings",
    ]
    output_model = DimensionAnalysis

    @classmethod
    def get_default_config(cls) -> PatternConfig:
        """Return default configuration for the pattern."""
        return PatternConfig(
            pattern_name=cls.name,
            description=cls.description,
            version=cls.version,
            data_sources=[
                DataSource(source_type=DataSourceType.DIMENSIONAL_TIME_SERIES, is_required=True, data_key="ledger_df")
            ],
            analysis_window=AnalysisWindowConfig(
                strategy=WindowStrategy.FIXED_TIME, days=180, min_days=30, max_days=365, include_today=False
            ),
        )

    def analyze(
        self,
        metric_id: str,
        data: pd.DataFrame,
        analysis_window: AnalysisWindow,
        num_periods: int = 8,
        **kwargs,
    ) -> DimensionAnalysis:
        """
        Perform dimension analysis for a metric across slices of a dimension.

        Args:
            metric_id: ID of the metric to analyze
            data: DataFrame with columns: metric_id, time_grain, date, dimension, slice_value, metric_value
            analysis_window: Analysis window specifying the time range and grain
            num_periods: Number of periods for historical rankings

        Returns:
            DimensionAnalysis with slice metrics and insights

        Raises:
            ValidationError: For invalid inputs
            PatternError: For pattern execution errors
        """
        try:
            grain = analysis_window.grain
            dimension_name = kwargs.get("dimension_name", "dimension")

            # Validate input data
            required_columns = ["metric_id", "date", "dimension", "slice_value", "metric_value"]
            self.validate_data(data, required_columns)

            # Process the analysis date from the analysis window
            analysis_date = pd.to_datetime(analysis_window.end_date)

            # Pre Process data - Note: This might raise TimeRangeError if no data in date range
            try:
                ledger_df = self.preprocess_data(data, analysis_window)
            except TimeRangeError:
                # Handle the case where there's no data in the time range
                logger.info("No data found in the specified date range. Returning minimal output.")
                return self.handle_empty_data(metric_id, analysis_window)

            # If empty data or metric not present, return minimal output
            if ledger_df.empty or metric_id not in ledger_df["metric_id"].unique():
                logger.info("Empty data for metric_id=%s. Returning minimal output.", metric_id)
                return self.handle_empty_data(metric_id, analysis_window)

            # 1) Determine current and prior period ranges based on grain
            current_start, current_end = get_period_range_for_grain(analysis_date, grain)
            prior_start, _ = get_prior_period_range(current_start, current_end, grain)

            # 2) Filter the ledger for the metric and dimension
            filtered_df = ledger_df[
                (ledger_df["metric_id"] == metric_id) & (ledger_df["dimension"] == dimension_name)
            ].copy()

            # If no data for the specified dimension, return minimal output
            if filtered_df.empty:
                logger.info("No data found for dimension=%s. Returning minimal output.", dimension_name)
                return self.handle_empty_data(metric_id, analysis_window)

            # Preprocess data to ensure dates are parsed correctly
            filtered_df["date"] = pd.to_datetime(filtered_df["date"])

            # 3) Compare slices across time periods
            compare_df = compare_dimension_slices_over_time(
                df=filtered_df,
                slice_col="slice_value",
                date_col="date",
                value_col="metric_value",
                prior_start_date=str(prior_start.date()),
                current_start_date=str(current_start.date()),
                agg="sum",
            )

            # 4) Compute share percentages for prior and current
            prior_df, current_df, merged = self._compute_slice_shares(compare_df)

            # 5) Calculate comparison to average - make sure slice_value column exists
            if "slice_value" in merged.columns:
                merged = merged.rename(columns={"slice_value": "slice_col"})
            merged = difference_from_average(merged, value_col="val_current")

            # 6) Build the slices performance metrics list for reporting
            slices_list = build_slices_performance_list(
                merged,
                "slice_col",
                current_val_col="val_current",
                prior_val_col="val_prior",
                include_shares=True,
                include_avg_comparison=True,
            )

            # 7) Compute top and bottom slices - ensure slice_col exists as dimension column
            if "slice_value" not in merged.columns and "slice_col" in merged.columns:
                merged["slice_value"] = merged["slice_col"]
            top_slices, bottom_slices = compute_top_bottom_slices(
                merged, dim_col="slice_value", value_col="val_current", top_n=3
            )

            # 8) Identify largest and smallest by share
            current_share = current_df.rename(columns={"slice_value": "slice_col"})
            prior_share = prior_df.rename(columns={"slice_value": "slice_col"})
            largest_slice, smallest_slice = identify_largest_smallest_by_share(current_share, prior_share, "slice_col")

            # 9) Identify new strongest/weakest slices
            new_strongest, new_weakest = identify_strongest_weakest_changes(
                merged, "slice_col", current_val_col="val_current", prior_val_col="val_prior"
            )

            # 10) Create comparison highlights
            comparison_highlights = highlight_slice_comparisons(merged, "slice_col", top_n=2)

            # 11) Compute historical rankings
            period_length_days = get_period_length_for_grain(grain)
            historical_rankings = compute_historical_slice_rankings(
                filtered_df,
                "slice_value",
                "date",
                "metric_value",
                num_periods=num_periods,
                period_length_days=period_length_days,
            )

            # 12) Create result
            result = {
                "pattern": self.name,
                "version": self.version,
                "metric_id": metric_id,
                "analysis_window": analysis_window,
                "num_periods": len(filtered_df),
                "dimension_name": dimension_name,
                "slices": slices_list,
                "top_slices": top_slices,
                "bottom_slices": bottom_slices,
                "largest_slice": largest_slice,
                "smallest_slice": smallest_slice,
                "new_strongest_slice": new_strongest,
                "new_weakest_slice": new_weakest,
                "comparison_highlights": comparison_highlights,
                "historical_slice_rankings": historical_rankings,
            }
            logger.info("Successfully analyzed dimension analysis for metric_id=%s", metric_id)
            # Create and validate output
            return self.validate_output(result)

        except Exception as e:
            logger.error("Error executing dimension analysis: %s", str(e), exc_info=True)
            raise PatternError(
                f"Error executing dimension analysis: {str(e)}",
                self.name,
                {"metric_id": metric_id, "dimension": kwargs.get("dimension_name", "dimension")},
            ) from e

    def _compute_slice_shares(self, compare_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Compute share percentages for prior and current periods.

        Args:
            compare_df: DataFrame with slice comparison data

        Returns:
            Tuple of (prior_df, current_df, merged) DataFrames
        """
        # Compute share percentages for prior
        prior_df = compare_df[["slice_value", "val_prior"]].copy()
        prior_df = prior_df.rename(columns={"val_prior": "aggregated_value"})
        prior_df = compute_slice_shares(
            prior_df, "slice_value", val_col="aggregated_value", share_col_name="share_pct_prior"
        )

        # Compute share percentages for current
        current_df = compare_df[["slice_value", "val_current"]].copy()
        current_df = current_df.rename(columns={"val_current": "aggregated_value"})
        current_df = compute_slice_shares(
            current_df, "slice_value", val_col="aggregated_value", share_col_name="share_pct_current"
        )

        # Merge data together
        merged = compare_df.merge(prior_df[["slice_value", "share_pct_prior"]], on="slice_value", how="left")
        merged = merged.merge(current_df[["slice_value", "share_pct_current"]], on="slice_value", how="left")

        # Calculate share difference
        merged["share_diff"] = merged["share_pct_current"] - merged["share_pct_prior"]

        return prior_df, current_df, merged
