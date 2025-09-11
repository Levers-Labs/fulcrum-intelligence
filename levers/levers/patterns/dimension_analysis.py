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
from datetime import date

import pandas as pd

from levers.exceptions import (
    InsufficientDataError,
    LeversError,
    PatternError,
    ValidationError,
)
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
                DataSource(source_type=DataSourceType.DIMENSIONAL_TIME_SERIES, is_required=True, data_key="data")
            ],
            analysis_window=AnalysisWindowConfig(
                strategy=WindowStrategy.FIXED_TIME, days=180, min_days=30, max_days=365, include_today=False
            ),
            needs_dimension_analysis=True,
        )

    def preprocess_data(
        self, data: pd.DataFrame, analysis_window: AnalysisWindow, date_col: str = "date"
    ) -> pd.DataFrame:
        data = super().preprocess_data(data, analysis_window, date_col)
        # rename columns
        # existing column : new column
        column_mapping = {
            "dimension_name": "dimension",
            "dimension_slice": "slice_value",
        }
        data = data.rename(columns=column_mapping)
        return data

    def analyze(  # type: ignore
        self,
        metric_id: str,
        data: pd.DataFrame,
        analysis_window: AnalysisWindow,
        analysis_date: date | None = None,
        dimension_name: str | None = None,
        num_periods: int = 2,
    ) -> DimensionAnalysis:
        """
        Perform dimension analysis for a metric across slices of a dimension.

        Args:
            metric_id: ID of the metric to analyze
            data: DataFrame with columns: metric_id, date, dimension_name, dimension_slice, value
            analysis_window: Analysis window specifying the time range and grain
            analysis_date: Optional date of analysis
            dimension_name: Optional dimension name to analyze
            num_periods: Number of periods for historical rankings

        Returns:
            DimensionAnalysis with slice metrics and insights

        Raises:
            ValidationError: For invalid inputs
            PatternError: For pattern execution errors
        """
        try:
            if not dimension_name:
                raise ValidationError(
                    "Dimension name is required", invalid_fields={"dimension_name": "Dimension name is required"}
                )
            # Set analysis date to today if not provided
            analysis_date = analysis_date or date.today()
            grain = analysis_window.grain
            # Validate input data
            required_columns = ["date", "dimension_name", "dimension_slice", "value"]
            self.validate_data(data, required_columns)

            # Check if we have enough data points for analysis
            if len(data) < num_periods:
                logger.info("Insufficient data for metric_id=%s")
                raise InsufficientDataError("Insufficient data to perform Dimensional Analysis")

            # Pre Process data
            # renames columns to dimension, slice_value
            ledger_df = self.preprocess_data(data, analysis_window)

            # 1) Filter data for the current dimension
            ledger_df = ledger_df[ledger_df["dimension"] == dimension_name].copy()

            # If empty data not present, return minimal output
            if ledger_df.empty:
                logger.info("Empty data for dimension=%s Returning minimal output.", dimension_name)
                return self.handle_empty_data(metric_id, analysis_window)

            # 2) Determine current and prior period ranges based on grain
            current_start, current_end = get_period_range_for_grain(grain, analysis_date)
            prior_start, _ = get_prior_period_range(current_start, current_end, grain)

            # 3) Compare slices across time periods
            compare_df = compare_dimension_slices_over_time(
                df=ledger_df,  # type: ignore
                slice_col="slice_value",
                date_col="date",
                value_col="value",
                prior_start_date=str(prior_start.date()),
                current_start_date=str(current_start.date()),
                agg="sum",
            )

            # 4) Compute share percentages for prior and current
            prior_df, current_df, merged = self._compute_slice_shares(compare_df)

            # 5) Calculate comparison to average
            merged = difference_from_average(df=merged, value_col="val_current")

            # 6) Build the slices performance metrics list for reporting
            slices_list = build_slices_performance_list(
                df=merged,
                slice_col="slice_value",
                current_val_col="val_current",
                prior_val_col="val_prior",
                include_shares=True,
                include_avg_comparison=True,
            )

            # 7) Compute top and bottom slices
            top_slices, bottom_slices = compute_top_bottom_slices(
                df=merged, slice_col="slice_value", value_col="val_current", top_n=4, dimension_name=dimension_name
            )

            # 8) Identify largest and smallest by share
            current_share = current_df.rename(columns={"share_pct_current": "share_pct"})
            prior_share = prior_df.rename(columns={"share_pct_prior": "share_pct"})
            largest_slice, smallest_slice = identify_largest_smallest_by_share(
                current_share, prior_share, "slice_value"
            )

            # 9) Identify new strongest/weakest slices
            strongest, weakest = identify_strongest_weakest_changes(
                merged, "slice_value", current_val_col="val_current", prior_val_col="val_prior"
            )

            # 10) Create comparison highlights
            comparison_highlights = highlight_slice_comparisons(merged, "slice_value", top_n=2)

            # 11) Compute historical rankings
            period_length_days = get_period_length_for_grain(grain)
            historical_rankings = compute_historical_slice_rankings(
                df=ledger_df,
                slice_col="slice_value",
                date_col="date",
                value_col="value",
                num_periods=num_periods,
                period_length_days=period_length_days,
                dimension_name=dimension_name,
            )

            # 12) Create result
            result = {
                "pattern": self.name,
                "version": self.version,
                "metric_id": metric_id,
                "analysis_window": analysis_window,
                "num_periods": len(ledger_df),
                "analysis_date": analysis_date,
                "dimension_name": dimension_name,
                "slices": slices_list,
                "top_slices": top_slices,
                "bottom_slices": bottom_slices,
                "largest_slice": largest_slice,
                "smallest_slice": smallest_slice,
                "strongest_slice": strongest,
                "weakest_slice": weakest,
                "comparison_highlights": comparison_highlights,
                "historical_slice_rankings": historical_rankings,
            }
            logger.info("Successfully analyzed dimension analysis for metric_id=%s", metric_id)
            # Create and validate output
            return self.validate_output(result)

        except Exception as e:
            logger.error("Error executing dimension analysis: %s", str(e), exc_info=True)
            if isinstance(e, LeversError):
                raise
            raise PatternError(
                f"Error executing {self.name} for metric {metric_id}: {str(e)}",
                self.name,
                details={
                    "metric_id": metric_id,
                    "dimension": dimension_name,
                    "original_error": type(e).__name__,
                },
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
            agg_df=prior_df, slice_col="slice_value", val_col="aggregated_value", share_col_name="share_pct_prior"
        )

        # Compute share percentages for current
        current_df = compare_df[["slice_value", "val_current"]].copy()
        current_df = current_df.rename(columns={"val_current": "aggregated_value"})
        current_df = compute_slice_shares(
            agg_df=current_df, slice_col="slice_value", val_col="aggregated_value", share_col_name="share_pct_current"
        )

        # Merge data together
        merged = compare_df.merge(prior_df[["slice_value", "share_pct_prior"]], on="slice_value", how="left")
        merged = merged.merge(current_df[["slice_value", "share_pct_current"]], on="slice_value", how="left")

        # Calculate share difference
        merged["share_diff"] = merged["share_pct_current"] - merged["share_pct_prior"]

        return prior_df, current_df, merged
