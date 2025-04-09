"""
Historical Performance Pattern

This module implements the HistoricalPerformancePattern which analyzes a metric's
historical performance over a lookback window. It computes period-over-period
growth rates, acceleration, trends, record highs/lows, seasonal patterns, 
benchmark comparisons, and trend exceptions.
"""

import logging
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from levers.exceptions import ValidationError
from levers.models import (
    AnalysisWindowConfig,
    DataSource,
    DataSourceType,
    PatternConfig,
    WindowStrategy,
)
from levers.models.common import AnalysisWindow, Granularity
from levers.models.patterns.historical_performance import (
    BenchmarkComparison,
    HistoricalPerformance,
    PeriodMetrics,
    RankSummary,
    Seasonality,
    TrendException,
    TrendType,
)
from levers.patterns.base import Pattern
from levers.primitives.numeric import calculate_difference, calculate_percentage_difference
from levers.primitives.time_series import calculate_average_growth, calculate_pop_growth, convert_grain_to_freq
from levers.primitives.trend_analysis import (
    analyze_metric_trend,
    detect_record_high,
    detect_record_low,
    detect_trend_exceptions,
)

logger = logging.getLogger(__name__)


class HistoricalPerformancePattern(Pattern[HistoricalPerformance]):
    """Pattern for analyzing a metric's historical performance over time."""

    name = "historical_performance"
    version = "1.0"
    description = "Analyzes a metric's historical performance patterns over time"
    required_primitives = [
        "calculate_pop_growth",
        "calculate_average_growth",
        "analyze_metric_trend",
        "detect_record_high",
        "detect_record_low",
        "detect_trend_exceptions",
        "convert_grain_to_freq",
    ]
    output_model: type[HistoricalPerformance] = HistoricalPerformance

    def analyze(  # type: ignore
        self, metric_id: str, data: pd.DataFrame, analysis_window: AnalysisWindow, num_periods: int = 12
    ) -> HistoricalPerformance:
        """
        Execute the historical performance pattern.

        Args:
            metric_id: The ID of the metric being analyzed
            data: DataFrame containing columns: date, value
            analysis_window: AnalysisWindow object specifying the analysis time window
            num_periods: Number of periods to analyze (default: 12)

        Returns:
            HistoricalPerformance object with analysis results

        Raises:
            ValidationError: If input validation fails or calculation errors occur
        """
        try:
            grain = analysis_window.grain
            # Validate input data
            required_columns = ["date", "value"]
            self.validate_data(data, required_columns)
            self.validate_analysis_window(analysis_window)

            # Pre Process data
            data_window = self.preprocess_data(data, analysis_window)

            # Define lookback_end
            lookback_end = pd.to_datetime(analysis_window.end_date)

            # If empty data, return minimal output
            if data_window.empty:
                logger.info("Empty data for metric_id=%s. Returning minimal output.", metric_id)
                return self.handle_empty_data(metric_id, analysis_window)

            # Create a copy with date as index for resampling
            data_window_indexed = data_window.copy()
            data_window_indexed.set_index("date", inplace=True)

            # Group by grain and filter to specified number of periods
            grouped = self._group_by_grain(data_window_indexed, grain, num_periods)

            # If insufficient data after grouping, return minimal output
            if len(grouped) < 2:
                logger.info("Insufficient data after grouping for metric_id=%s. Returning minimal output.", metric_id)
                return self.handle_empty_data(metric_id, analysis_window)

            # Compute period metrics (growth and acceleration)
            period_metrics = self._compute_period_metrics(grouped)

            # Calculate growth summary statistics
            growth_stats = self._calculate_growth_statistics(grouped, period_metrics)

            # Analyze trends
            trend_info = self._analyze_trends(grouped, avg_pop_growth=growth_stats["average_pop_growth"])

            # Detect record values
            value_rankings = self._detect_record_values(data_window)

            # Analyze seasonality
            seasonality_data = self._analyze_seasonality(data_window, lookback_end)

            # Calculate benchmark comparisons
            benchmark_comparisons = self._calculate_benchmark_comparisons(data_window, grain)

            # Detect trend exceptions
            trend_exceptions = self._detect_trend_exceptions(grouped)

            # Construct result object
            result = {
                "pattern": self.name,
                "version": self.version,
                "metric_id": metric_id,
                "analysis_window": analysis_window,
                "num_periods": len(grouped),
                "period_metrics": period_metrics,
                "growth_stats": growth_stats,
                "current_trend": trend_info["current_trend"],
                "previous_trend": trend_info["previous_trend"],
                "high_rank": value_rankings["high_rank"],
                "low_rank": value_rankings["low_rank"],
                "seasonality": seasonality_data,
                "benchmark_comparisons": benchmark_comparisons,
                "trend_exceptions": trend_exceptions,
            }

            logger.info("Successfully analyzed historical performance for metric_id=%s", metric_id)
            return self.validate_output(result)

        except Exception as e:
            logger.error("Error in historical performance calculation: %s", str(e), exc_info=True)
            # Re-raise with pattern context
            raise ValidationError(
                f"Error in historical performance calculation: {str(e)}",
                {
                    "pattern": self.name,
                    "metric_id": metric_id,
                },
            ) from e

    def _group_by_grain(self, data_window: pd.DataFrame, grain: str, num_periods: int) -> pd.DataFrame:
        """
        Resample the data by the specified grain and filter to the number of periods.

        Args:
            data_window: DataFrame with data in the analysis window (with date as index)
            grain: Time grain for grouping (day, week, month, etc.)
            num_periods: Number of periods to include

        Returns:
            Grouped DataFrame filtered to specified number of periods
        """
        # Resample/group the data by the chosen grain
        freq = convert_grain_to_freq(grain)
        # todo: Check if we need this step since we already pass data grain wise, also how does it aggregate?
        grouped = data_window.resample(freq).last()[["value"]].dropna(subset=["value"])
        grouped = grouped.reset_index()

        # Keep only up to the last `num_periods` entries
        if len(grouped) > num_periods:
            grouped = grouped.iloc[-num_periods:].copy()

        return grouped

    def _compute_period_metrics(self, grouped: pd.DataFrame) -> list[PeriodMetrics]:
        """
        Compute period metrics including growth rates and acceleration rates.

        Args:
            grouped: DataFrame grouped by the chosen grain

        Returns:
            List of PeriodMetrics objects with growth and acceleration data
        """
        # Compute period-over-period (PoP) growth
        grouped_growth = calculate_pop_growth(
            df=grouped,
            date_col="date",
            value_col="value",
            periods=1,
            fill_method=None,
            annualize=False,
            growth_col_name="pop_growth",
        )

        # Build the period metrics list
        # First, create entries for each period with growth rate
        period_metrics = []
        for i in range(1, len(grouped_growth)):
            curr = grouped_growth.iloc[i]
            prev = grouped_growth.iloc[i - 1]

            period_metrics.append(
                PeriodMetrics(
                    period_start=prev["date"].strftime("%Y-%m-%d"),
                    period_end=curr["date"].strftime("%Y-%m-%d"),
                    pop_growth_percent=float(curr["pop_growth"]) if not pd.isna(curr["pop_growth"]) else None,
                    pop_acceleration_percent=None,  # Will set this in the next step
                )
            )

        # Now compute and set acceleration values starting from the second period
        for i in range(1, len(period_metrics)):
            this_growth = period_metrics[i].pop_growth_percent
            prev_growth = period_metrics[i - 1].pop_growth_percent

            if this_growth is not None and prev_growth is not None:
                accel = this_growth - prev_growth
                period_metrics[i].pop_acceleration_percent = accel

        return period_metrics

    def _calculate_growth_statistics(
        self,
        grouped: pd.DataFrame,
        period_metrics: list[PeriodMetrics],
    ) -> dict[str, Any]:
        """
        Calculate growth statistics including current growth, average growth,
        acceleration, and consecutive periods accelerating/slowing.

        Args:
            grouped: DataFrame grouped by the chosen grain
            period_metrics: List of period metrics including growth and acceleration

        Returns:
            Dictionary containing growth statistics
        """
        # Current period-over-period growth
        current_pop_growth = period_metrics[-1].pop_growth_percent if period_metrics else None

        # Calculate average growth using our primitive
        avg_growth_results = calculate_average_growth(grouped, "date", "value", "arithmetic")
        avg_pop_growth = avg_growth_results.get("average_growth")

        # Current growth acceleration
        current_growth_acceleration = period_metrics[-1].pop_acceleration_percent if len(period_metrics) > 1 else None

        # Count consecutive accelerating or slowing periods
        num_periods_accelerating, num_periods_slowing = 0, 0
        for pm in reversed(period_metrics[1:]):  # Skip the first period which has no acceleration
            accel_val = pm.pop_acceleration_percent
            if accel_val is None:
                break
            if accel_val > 0:
                if num_periods_slowing > 0:
                    # We had a slowing run, so break
                    break
                num_periods_accelerating += 1
            elif accel_val < 0:
                if num_periods_accelerating > 0:
                    break
                num_periods_slowing += 1
            else:
                break

        return {
            "current_pop_growth": current_pop_growth,
            "average_pop_growth": avg_pop_growth,
            "current_growth_acceleration": current_growth_acceleration,
            "num_periods_accelerating": num_periods_accelerating,
            "num_periods_slowing": num_periods_slowing,
        }

    def _analyze_trends(self, grouped: pd.DataFrame, avg_pop_growth: float | None) -> dict[str, Any]:
        """
        Analyze metric trends and classify current and previous trends.

        Args:
            grouped: DataFrame grouped by the chosen grain
            avg_pop_growth: Average growth rate over the analysis window

        Returns:
            Dictionary containing trend classification information
        """
        # Trend classification
        trend_result = analyze_metric_trend(
            df=grouped, value_col="value", date_col="date", window_size=min(5, len(grouped))
        )
        trend_type = trend_result.get("trend_direction")
        # If no trend, return None
        # No need to analyze previous trend in this case
        if trend_type is None:
            return {
                "current_trend": None,
                "previous_trend": None,
            }

        # Define trend_start_date as the earliest date in grouped
        trend_start_date = grouped["date"].iloc[0].strftime("%Y-%m-%d")

        # For "previous" trend info, analyze the previous subset
        previous_trend_info = self._analyze_previous_trend(grouped)

        return {
            "current_trend": {
                "trend_type": trend_type,
                "start_date": trend_start_date,
                "average_pop_growth": avg_pop_growth,
                "duration_grains": len(grouped),
            },
            "previous_trend": previous_trend_info,
        }

    def _analyze_previous_trend(self, grouped: pd.DataFrame) -> dict[str, Any] | None:
        """
        Analyze the previous trend by examining all points except the last one.

        Args:
            grouped: DataFrame grouped by the chosen grain

        Returns:
            Dictionary containing previous trend information
        """
        if len(grouped) < 2:
            return None
        prev_subset = grouped.iloc[:-1].copy()
        prev_trend_res = analyze_metric_trend(prev_subset, "value", "date")
        previous_trend_type = prev_trend_res.get("trend_type")
        previous_trend_start_date = prev_subset["date"].iloc[0].strftime("%Y-%m-%d")

        # Calculate average growth for previous subset
        prev_subset_growth_results = calculate_average_growth(prev_subset, "date", "value", "arithmetic")
        previous_trend_average_pop_growth = prev_subset_growth_results.get("average_growth")
        previous_trend_duration_grains = len(prev_subset)
        return {
            "trend_type": previous_trend_type,
            "start_date": previous_trend_start_date,
            "average_pop_growth": previous_trend_average_pop_growth,
            "duration_grains": previous_trend_duration_grains,
        }

    def _detect_record_values(self, data_window: pd.DataFrame) -> dict[str, RankSummary | None]:
        """
        Detect high and low values in the data window, always returning rank information.

        Args:
            data_window: DataFrame with date and value columns

        Returns:
            Dictionary containing high and low value summaries
        """
        # Use existing detect_record_high and detect_record_low functions
        high_result = detect_record_high(data_window, "value")
        low_result = detect_record_low(data_window, "value")

        # Create high value summary - always included regardless of record status
        # Extract date of prior record if available
        prior_high_date = None
        if high_result.get("prior_max_index") is not None:
            prior_idx = high_result["prior_max_index"]
            if prior_idx in data_window.index:
                prior_high_date = data_window.loc[prior_idx, "date"].strftime("%Y-%m-%d")

        high_rank = RankSummary(
            value=high_result["current_value"],
            rank=high_result["rank"],
            duration_grains=high_result["periods_compared"],
            prior_record_value=high_result["prior_max"],
            prior_record_date=prior_high_date,
            absolute_delta_from_prior_record=high_result["absolute_delta"],
            relative_delta_from_prior_record=high_result["percentage_delta"],
        )

        # Create low value summary - always included regardless of record status
        # Extract date of prior record if available
        prior_low_date = None
        if low_result.get("prior_min_index") is not None:
            prior_idx = low_result["prior_min_index"]
            if prior_idx in data_window.index:
                prior_low_date = data_window.loc[prior_idx, "date"].strftime("%Y-%m-%d")

        low_rank = RankSummary(
            value=low_result["current_value"],
            rank=low_result["rank"],
            duration_grains=low_result["periods_compared"],
            prior_record_value=low_result["prior_min"],
            prior_record_date=prior_low_date,
            absolute_delta_from_prior_record=low_result["absolute_delta"],
            relative_delta_from_prior_record=low_result["percentage_delta"],
        )

        return {
            "high_rank": high_rank,
            "low_rank": low_rank,
        }

    # TODO: should we have primitives for this? instead of core logic in the pattern?
    def _analyze_seasonality(self, data_window: pd.DataFrame, lookback_end: pd.Timestamp) -> dict[str, Any] | None:
        """
        Analyze seasonality by comparing current value to value from one year ago.

        Args:
            data_window: DataFrame with date and value columns
            lookback_end: End date of the analysis window

        Returns:
            Seasonality object if seasonality analysis is possible, None otherwise
        """
        # Find data from approximately 1 year earlier than lookback_end
        yoy_date = lookback_end - timedelta(days=365)

        # Sort data by date
        data_window_sorted = data_window.copy().sort_values("date")

        # Get the last row that is <= yoy_date
        yoy_df = data_window_sorted[data_window_sorted["date"] <= yoy_date]

        # If we can't find a yoy reference, we set empty seasonality results.
        if yoy_df.empty:
            logger.warning("No data found for metric_id=%s to calculate seasonality", self.metric_id)
            return None
        yoy_ref_value = yoy_df.iloc[-1]["value"]  # last row
        current_value = data_window_sorted.iloc[-1]["value"]  # final row

        try:
            actual_change_percent = calculate_percentage_difference(
                current_value, yoy_ref_value, handle_zero_reference=True
            )
        except Exception:
            actual_change_percent = None

        if actual_change_percent is None:
            logger.warning("Failed to calculate actual change percent for metric_id=%s", self.metric_id)
            return None

        # Define expected change as average YoY across all pairs
        yoy_changes = []
        for i in range(len(data_window_sorted)):
            this_date = data_window_sorted.iloc[i]["date"]
            search_date = this_date - timedelta(days=365)
            subset = data_window_sorted[data_window_sorted["date"] <= search_date]
            if not subset.empty:
                ref_val = subset.iloc[-1]["value"]
                cur_val = data_window_sorted.iloc[i]["value"]
                yoy_changes.append(calculate_percentage_difference(cur_val, ref_val, handle_zero_reference=True))

        expected_change = 0.0
        if yoy_changes:
            expected_change = float(np.mean(yoy_changes))

        deviation_percent = actual_change_percent - expected_change
        is_following = abs(deviation_percent) <= 2.0  # +/-2% threshold

        return {
            "is_following_expected_pattern": is_following,
            "expected_change_percent": expected_change,
            "actual_change_percent": actual_change_percent,
            "deviation_percent": deviation_percent,
        }

    # TODO: should we have a primitive for this?
    def _calculate_benchmark_comparisons(self, data_window: pd.DataFrame, grain: str) -> list[BenchmarkComparison]:
        """
        Calculate benchmark comparisons such as week-to-date vs. prior week-to-date.

        Args:
            data_window: DataFrame with date and value columns
            grain: Time grain for analysis

        Returns:
            List of benchmark comparisons
        """
        benchmark_comparisons = []
        # For week/month grains or others, we won't produce benchmarks here
        if grain != Granularity.DAY:
            return benchmark_comparisons

        # For daily data, compare current WTD to prior WTD
        last_date = data_window["date"].max()

        # Monday of current week
        current_week_monday = last_date - pd.Timedelta(days=last_date.dayofweek)

        # Date range for current WTD
        c_start = current_week_monday
        c_end = last_date

        # Prior WTD - shift by 7 days
        p_start = c_start - pd.Timedelta(days=7)
        p_end = c_end - pd.Timedelta(days=7)

        # Gather current WTD data
        c_mask = (data_window["date"] >= c_start) & (data_window["date"] <= c_end)
        c_vals = data_window.loc[c_mask, "value"]
        # TODO: do we need to always sum?
        current_sum = c_vals.sum()

        # Gather prior WTD data
        p_mask = (data_window["date"] >= p_start) & (data_window["date"] <= p_end)
        p_vals = data_window.loc[p_mask, "value"]
        prior_sum = p_vals.sum()

        abs_change = calculate_difference(current_sum, prior_sum)
        try:
            change_percent = calculate_percentage_difference(current_sum, prior_sum, handle_zero_reference=True)
        except Exception:
            change_percent = None

        benchmark_comparisons.append(
            {
                "reference_period": "WTD",
                "absolute_change": abs_change,
                "change_percent": change_percent,
            }
        )

        return benchmark_comparisons

    def _detect_trend_exceptions(self, grouped: pd.DataFrame) -> list[TrendException]:
        """
        Detect anomalies (spikes or drops) in the time series.

        Args:
            grouped: DataFrame grouped by the chosen grain

        Returns:
            List of detected trend exceptions
        """
        # Trend exceptions (spike / drop detection)
        exception_results = detect_trend_exceptions(
            df=grouped, date_col="date", value_col="value", window_size=min(5, len(grouped)), z_threshold=2.0
        )

        return exception_results

    @classmethod
    def get_default_config(cls) -> PatternConfig:
        """
        Get the default configuration for the historical performance pattern.

        Returns:
            PatternConfig with historical performance pattern configuration
        """
        return PatternConfig(
            pattern_name=cls.name,
            description=cls.description,
            version=cls.version,
            data_sources=[DataSource(source_type=DataSourceType.METRIC_TIME_SERIES, is_required=True, data_key="data")],
            analysis_window=AnalysisWindowConfig(
                strategy=WindowStrategy.FIXED_TIME, days=180, min_days=30, max_days=365, include_today=True
            ),
        )
