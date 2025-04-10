"""
Historical Performance Pattern

This module implements the HistoricalPerformancePattern which analyzes a metric's
historical performance over a lookback window. It computes period-over-period
growth rates, acceleration, trends, record highs/lows, seasonal patterns,
benchmark comparisons, and trend exceptions.
"""

import logging

import pandas as pd

from levers.exceptions import ValidationError
from levers.models import (
    AnalysisWindowConfig,
    AverageGrowthMethod,
    DataSource,
    DataSourceType,
    PatternConfig,
    WindowStrategy,
)
from levers.models.common import AnalysisWindow, Granularity
from levers.models.patterns.historical_performance import (
    BenchmarkComparison,
    GrowthStats,
    HistoricalPerformance,
    PeriodMetrics,
    RankSummary,
    Seasonality,
    TrendException,
    TrendInfo,
)
from levers.patterns.base import Pattern
from levers.primitives.time_series import calculate_average_growth, calculate_pop_growth, convert_grain_to_freq
from levers.primitives.trend_analysis import (
    analyze_metric_trend,
    calculate_benchmark_comparisons,
    detect_record_high,
    detect_record_low,
    detect_seasonality_pattern,
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
        "detect_seasonality_pattern",
        "calculate_benchmark_comparisons",
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
            trend_info = self._analyze_trends(grouped, avg_pop_growth=growth_stats.average_pop_growth)

            # Detect record values
            value_rankings = self._detect_record_values(data_window)

            # Analyze seasonality using the new primitive
            seasonality_pattern = self._detect_seasonality_pattern(data_window, lookback_end)

            # Calculate benchmark comparisons using the new primitive
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
                "seasonality": seasonality_pattern,
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

    def _group_by_grain(self, data_window: pd.DataFrame, grain: Granularity, num_periods: int) -> pd.DataFrame:
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
    ) -> GrowthStats:
        """
        Calculate growth statistics including current growth, average growth,
        acceleration, and consecutive periods accelerating/slowing.

        Args:
            grouped: DataFrame grouped by the chosen grain
            period_metrics: List of period metrics including growth and acceleration

        Returns:
            GrowthStats object containing growth statistics
        """
        # Current period-over-period growth
        current_pop_growth = period_metrics[-1].pop_growth_percent if period_metrics else None

        # Calculate average growth using our primitive
        avg_growth_results = calculate_average_growth(grouped, "date", "value", AverageGrowthMethod.ARITHMETIC)
        avg_pop_growth = avg_growth_results.average_growth

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

        return GrowthStats(
            current_pop_growth=current_pop_growth,
            average_pop_growth=avg_pop_growth,
            current_growth_acceleration=current_growth_acceleration,
            num_periods_accelerating=num_periods_accelerating,
            num_periods_slowing=num_periods_slowing,
        )

    def _analyze_trends(self, grouped: pd.DataFrame, avg_pop_growth: float | None) -> dict[str, TrendInfo | None]:
        """
        Analyze metric trends and classify current and previous trends.

        Args:
            grouped: DataFrame grouped by the chosen grain
            avg_pop_growth: Average growth rate over the analysis window

        Returns:
            Dictionary containing current and previous trend information
        """
        # Trend classification
        trend_result = analyze_metric_trend(
            df=grouped, value_col="value", date_col="date", window_size=min(5, len(grouped))
        )
        # If no trend, return None
        # No need to analyze previous trend in this case
        if trend_result is None:
            return {
                "current_trend": None,
                "previous_trend": None,
            }

        # Define trend_start_date as the earliest date in grouped
        trend_start_date = grouped["date"].iloc[0].strftime("%Y-%m-%d")

        # For "previous" trend info, analyze the previous subset
        previous_trend_info = self._analyze_previous_trend(grouped)

        return {
            "current_trend": TrendInfo(
                trend_type=trend_result.trend_type,
                start_date=trend_start_date,
                average_pop_growth=avg_pop_growth,
                duration_grains=len(grouped),
            ),
            "previous_trend": previous_trend_info,
        }

    def _analyze_previous_trend(self, grouped: pd.DataFrame) -> TrendInfo | None:
        """
        Analyze the previous trend by examining all points except the last one.

        Args:
            grouped: DataFrame grouped by the chosen grain

        Returns:
            TrendInfo object containing previous trend information
        """
        if len(grouped) < 2:
            return None
        prev_subset = grouped.iloc[:-1].copy()
        prev_trend_res = analyze_metric_trend(prev_subset, "value", "date")
        # If no trend, return None
        if prev_trend_res is None:
            return None

        previous_trend_type = prev_trend_res.trend_type
        previous_trend_start_date = prev_subset["date"].iloc[0].strftime("%Y-%m-%d")

        # Calculate average growth for previous subset
        prev_subset_growth_results = calculate_average_growth(
            prev_subset, "date", "value", AverageGrowthMethod.ARITHMETIC
        )
        previous_trend_average_pop_growth = prev_subset_growth_results.average_growth
        previous_trend_duration_grains = len(prev_subset)
        return TrendInfo(
            trend_type=previous_trend_type,
            start_date=previous_trend_start_date,
            average_pop_growth=previous_trend_average_pop_growth,
            duration_grains=previous_trend_duration_grains,
        )

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
        if high_result.prior_max_index is not None:
            prior_idx = high_result.prior_max_index
            if prior_idx in data_window.index:
                prior_high_date = data_window.loc[prior_idx, "date"].strftime("%Y-%m-%d")  # type: ignore

        high_rank = RankSummary(
            value=high_result.current_value,
            rank=high_result.rank,
            duration_grains=high_result.periods_compared,
            prior_record_value=high_result.prior_max,
            prior_record_date=prior_high_date,
            absolute_delta_from_prior_record=high_result.absolute_delta,
            relative_delta_from_prior_record=high_result.percentage_delta,
        )

        # Create low value summary - always included regardless of record status
        # Extract date of prior record if available
        prior_low_date = None
        if low_result.prior_min_index is not None:
            prior_idx = low_result.prior_min_index
            if prior_idx in data_window.index:
                prior_low_date = data_window.loc[prior_idx, "date"].strftime("%Y-%m-%d")  # type: ignore

        low_rank = RankSummary(
            value=low_result.current_value,
            rank=low_result.rank,
            duration_grains=low_result.periods_compared,
            prior_record_value=low_result.prior_min,
            prior_record_date=prior_low_date,
            absolute_delta_from_prior_record=low_result.absolute_delta,
            relative_delta_from_prior_record=low_result.percentage_delta,
        )

        return {
            "high_rank": high_rank,
            "low_rank": low_rank,
        }

    def _detect_seasonality_pattern(self, data_window: pd.DataFrame, lookback_end: pd.Timestamp) -> Seasonality | None:
        """
        Detect seasonality pattern by comparing current value to value from one year ago.

        Args:
            data_window: DataFrame with date and value columns
            lookback_end: End date of the analysis window

        Returns:
            Seasonality object containing seasonality analysis results or None if insufficient data
        """
        return detect_seasonality_pattern(data_window, lookback_end, "date", "value")

    def _calculate_benchmark_comparisons(
        self, data_window: pd.DataFrame, grain: str
    ) -> list[BenchmarkComparison] | list:
        """
        Calculate benchmark comparisons such as week-to-date vs. prior week-to-date.

        Args:
            data_window: DataFrame with date and value columns
            grain: Time grain for analysis (day, week, month)

        Returns:
            List of benchmark comparison objects or empty list if no benchmarks can be calculated
        """
        # For non-daily data, we won't produce benchmarks here
        if grain != Granularity.DAY or data_window.empty:
            return []

        return calculate_benchmark_comparisons(data_window, "date", "value")

    def _detect_trend_exceptions(self, grouped: pd.DataFrame) -> list[TrendException] | list:
        """
        Detect anomalies (spikes or drops) in the time series.

        Args:
            grouped: DataFrame grouped by the chosen grain

        Returns:
            List of detected trend exceptions or empty list if no exceptions are detected
        """
        return detect_trend_exceptions(
            df=grouped, date_col="date", value_col="value", window_size=min(5, len(grouped)), z_threshold=2.0
        )

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
