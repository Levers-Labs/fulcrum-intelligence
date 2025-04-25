"""
Dimension Analysis Pattern

This pattern performs slice-level analysis for a given metric and dimension at
a specified time grain (day, week, or month). It compares the current period
to the immediately prior period, calculates share of total metric volume,
performance deltas, ranks slices, identifies top and bottom slices, and more.

Dependencies:
  - pandas as pd
"""

import datetime
import logging
from typing import Any

import pandas as pd

from levers.exceptions import PatternError, ValidationError
from levers.models import (
    AnalysisWindow,
    AnalysisWindowConfig,
    DataSource,
    DataSourceType,
    Granularity,
    PatternConfig,
    WindowStrategy,
)
from levers.models.patterns import DimensionAnalysisResult
from levers.patterns.base import Pattern
from levers.primitives.dimensional_analysis import (
    build_slices_list,
    compare_dimension_slices_over_time,
    compute_historical_slice_rankings,
    compute_slice_shares,
    compute_top_bottom_slices,
    difference_from_average,
    get_period_range_for_grain,
    get_prior_period_range,
    highlight_slice_comparisons,
    identify_largest_smallest_by_share,
    identify_strongest_weakest_changes,
)

logger = logging.getLogger(__name__)


class DimensionAnalysisPattern(Pattern[DimensionAnalysisResult]):
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
        "build_slices_list",
        "compute_top_bottom_slices",
        "identify_largest_smallest_by_share",
        "identify_strongest_weakest_changes",
        "highlight_slice_comparisons",
        "compute_historical_slice_rankings",
    ]
    output_model = DimensionAnalysisResult

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
        ledger_df: pd.DataFrame,
        analysis_window: AnalysisWindow,
        dimension_name: str,
        num_periods: int = 8,
        **kwargs,
    ) -> DimensionAnalysisResult:
        """
        Perform dimension analysis for a metric across slices of a dimension.

        Args:
            metric_id: ID of the metric to analyze
            ledger_df: DataFrame with columns: metric_id, time_grain, date, dimension, slice_value, metric_value
            analysis_window: Analysis window specifying the time range and grain
            dimension_name: Dimension to analyze (e.g., "region", "product")
            num_periods: Number of periods for historical rankings

        Returns:
            DimensionAnalysisResult with slice metrics and insights

        Raises:
            ValidationError: For invalid inputs
            PatternError: For pattern execution errors
        """
        try:
            # Validate inputs
            self._validate_inputs(ledger_df, metric_id, dimension_name, analysis_window.grain)

            # Process the analysis date from the analysis window
            start_date = pd.to_datetime(analysis_window.start_date)
            end_date = pd.to_datetime(analysis_window.end_date)
            analysis_date = end_date  # Use end date as the analysis date

            # Handle empty data case
            if ledger_df.empty or metric_id not in ledger_df["metric_id"].unique():
                return self._empty_result(metric_id, analysis_window.grain, analysis_date, dimension_name)

            # 1) Determine current and prior period ranges based on grain
            current_start, current_end = get_period_range_for_grain(analysis_date, analysis_window.grain)
            prior_start, prior_end = get_prior_period_range(current_start, current_end, analysis_window.grain)

            # 2) Filter the ledger for the metric and dimension
            filtered_df = ledger_df[
                (ledger_df["metric_id"] == metric_id) & (ledger_df["dimension"] == dimension_name)
            ].copy()

            # Preprocess data to ensure dates are parsed correctly
            filtered_df["date"] = pd.to_datetime(filtered_df["date"])

            # 3) Compare slices across time periods
            compare_df = compare_dimension_slices_over_time(
                df=filtered_df,
                slice_col="slice_value",
                date_col="date",
                value_col="metric_value",
                t0=str(prior_start.date()),
                t1=str(current_start.date()),
                agg="sum",
            )

            # 4) Compute share percentages for T0 and T1
            t0_df, t1_df, merged = self._compute_slice_shares(compare_df)

            # 5) Calculate comparison to average
            merged = merged.rename(columns={"slice_value": "slice_col"})
            merged = difference_from_average(merged, current_col="val_t1")

            # 6) Build the slices list for reporting
            slices_list = build_slices_list(
                merged,
                "slice_col",
                current_val_col="val_t1",
                prior_val_col="val_t0",
                include_shares=True,
                include_avg_comparison=True,
            )

            # 7) Compute top and bottom slices
            top_slices, bottom_slices = compute_top_bottom_slices(merged, value_col="val_t1", top_n=3)
            top_slices_list = [dict(slice.dict()) for slice in top_slices]
            bottom_slices_list = [dict(slice.dict()) for slice in bottom_slices]

            # 8) Identify largest and smallest by share
            t1_share = t1_df.rename(columns={"slice_value": "slice_col"})
            t0_share = t0_df.rename(columns={"slice_value": "slice_col"})
            largest_slice, smallest_slice = identify_largest_smallest_by_share(t1_share, t0_share, "slice_col")

            # 9) Identify new strongest/weakest slices
            new_strongest, new_weakest = identify_strongest_weakest_changes(
                merged, "slice_col", current_val_col="val_t1", prior_val_col="val_t0"
            )

            # 10) Create comparison highlights
            comparison_highlights = highlight_slice_comparisons(merged, "slice_col", top_n=2)

            # 11) Compute historical rankings
            period_length_days = self._get_period_length_for_grain(analysis_window.grain)
            historical_rankings = compute_historical_slice_rankings(
                filtered_df,
                "slice_value",
                "date",
                "metric_value",
                num_periods=num_periods,
                period_length_days=period_length_days,
            )

            # 12) Create result
            return DimensionAnalysisResult(
                pattern_name=self.name,
                schema_version=self.version,
                metric_id=metric_id,
                grain=analysis_window.grain,
                analysis_date=str(analysis_date),
                evaluation_time=str(datetime.datetime.now()),
                dimension_name=dimension_name,
                slices=slices_list,
                top_slices_by_performance=top_slices_list,
                bottom_slices_by_performance=bottom_slices_list,
                largest_slice=largest_slice,
                smallest_slice=smallest_slice,
                new_strongest_slice=new_strongest,
                new_weakest_slice=new_weakest,
                comparison_highlights=comparison_highlights,
                historical_slice_rankings=historical_rankings,
            )

        except Exception as e:
            logger.error("Error executing dimension analysis: %s", str(e), exc_info=True)
            raise PatternError(
                f"Error executing dimension analysis: {str(e)}",
                self.name,
                {"metric_id": metric_id, "dimension": dimension_name},
            ) from e

    def _compute_slice_shares(self, compare_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Compute share percentages for current and prior periods.

        Args:
            compare_df: DataFrame with slice comparison data

        Returns:
            Tuple of (t0_df, t1_df, merged) DataFrames
        """
        # Compute share percentages for T0
        t0_df = compare_df[["slice_value", "val_t0"]].copy()
        t0_df = t0_df.rename(columns={"val_t0": "aggregated_value"})
        t0_df = compute_slice_shares(t0_df, "slice_value", val_col="aggregated_value", share_col_name="share_pct_t0")

        # Compute share percentages for T1
        t1_df = compare_df[["slice_value", "val_t1"]].copy()
        t1_df = t1_df.rename(columns={"val_t1": "aggregated_value"})
        t1_df = compute_slice_shares(t1_df, "slice_value", val_col="aggregated_value", share_col_name="share_pct_t1")

        # Merge data together
        merged = compare_df.merge(t0_df[["slice_value", "share_pct_t0"]], on="slice_value", how="left")
        merged = merged.merge(t1_df[["slice_value", "share_pct_t1"]], on="slice_value", how="left")

        # Calculate share difference
        merged["share_diff"] = merged["share_pct_t1"] - merged["share_pct_t0"]

        return t0_df, t1_df, merged

    def _get_period_length_for_grain(self, grain: Granularity) -> int:
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
        else:
            return 7  # Default to week

    def _validate_inputs(self, df: pd.DataFrame, metric_id: str, dimension_name: str, grain: Granularity) -> None:
        """
        Validate input data and parameters.

        Args:
            df: Input DataFrame to validate
            metric_id: Metric ID to validate
            dimension_name: Dimension name to validate
            grain: Time grain to validate

        Raises:
            ValidationError: If validation fails
        """
        if df.empty:
            raise ValidationError("Input DataFrame is empty", {})

        required_columns = ["metric_id", "date", "dimension", "slice_value", "metric_value"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValidationError(
                f"Missing required columns: {', '.join(missing_columns)}",
                {"missing_columns": missing_columns, "available_columns": list(df.columns)},
            )

        if grain not in Granularity:
            raise ValidationError(
                f"Invalid grain: {grain}. Must be one of {list(Granularity)}",
                {"grain": grain, "valid_grains": list(Granularity)},
            )

    def _empty_result(
        self, metric_id: str, grain: Granularity, analysis_date: str | pd.Timestamp, dimension_name: str
    ) -> DimensionAnalysisResult:
        """
        Create an empty result when no data is available.

        Args:
            metric_id: ID of the metric
            grain: Time grain of the analysis
            analysis_date: Date of analysis
            dimension_name: Name of the dimension

        Returns:
            Empty DimensionAnalysisResult
        """
        return DimensionAnalysisResult(
            pattern_name=self.name,
            schema_version=self.version,
            metric_id=metric_id,
            grain=grain,
            analysis_date=str(analysis_date),
            evaluation_time=str(datetime.datetime.now()),
            dimension_name=dimension_name,
            slices=[],
            top_slices_by_performance=[],
            bottom_slices_by_performance=[],
            largest_slice={},
            smallest_slice={},
            new_strongest_slice={},
            new_weakest_slice={},
            comparison_highlights=[],
            historical_slice_rankings={"periods_analyzed": 0, "period_rankings": []},
        )
