"""
Historical Performance Pattern

This module implements the HistoricalPerformancePattern which analyzes a metric's
historical performance over a lookback window. It computes period-over-period
growth rates, acceleration, trends, record highs/lows, seasonal patterns,
benchmark comparisons, and trend exceptions.
"""

import logging
from datetime import date

import pandas as pd

from levers.exceptions import LeversError, PatternError
from levers.models import (
    AnalysisWindow,
    AnalysisWindowConfig,
    AverageGrowthMethod,
    DataSource,
    DataSourceType,
    PatternConfig,
    WindowStrategy,
)
from levers.models.patterns import (
    GrowthStats,
    HistoricalPerformance,
    PeriodMetrics,
    RankSummary,
    Seasonality,
    TrendAnalysis,
    TrendException,
    TrendInfo,
)
from levers.patterns import Pattern
from levers.primitives import (
    analyze_trend_using_spc_analysis,
    calculate_average_growth,
    calculate_benchmark_comparisons,
    calculate_overall_growth,
    calculate_pop_growth,
    detect_record_high,
    detect_record_low,
    detect_seasonality_pattern,
    detect_trend_exceptions_using_spc_analysis,
    process_control_analysis,
)

logger = logging.getLogger(__name__)


class HistoricalPerformancePattern(Pattern[HistoricalPerformance]):
    """Pattern for analyzing a metric's historical performance over time."""

    name = "historical_performance"
    version = "1.0"
    description = "Analyzes a metric's historical performance patterns over time"
    required_primitives = [
        "analyze_trend_using_spc_analysis",
        "calculate_pop_growth",
        "calculate_average_growth",
        "detect_record_high",
        "detect_record_low",
        "detect_trend_exceptions_using_spc_analysis",
        "detect_seasonality_pattern",
        "calculate_benchmark_comparisons",
        "process_control_analysis",
        "calculate_overall_growth",
    ]
    output_model: type[HistoricalPerformance] = HistoricalPerformance

    def analyze(  # type: ignore
        self,
        metric_id: str,
        data: pd.DataFrame,
        analysis_window: AnalysisWindow,
        analysis_date: date | None = None,
        num_periods: int = 12,
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
            LeversError: if an errors like PrimitiveError, ValidationError, etc. occurs
            PatternError: If an error occurs during pattern execution
        """
        try:
            # Set analysis date to today if not provided
            analysis_date = analysis_date or date.today()
            grain = analysis_window.grain
            # Validate input data
            required_columns = ["date", "value"]
            self.validate_data(data, required_columns)

            # Pre Process data
            data_window = self.preprocess_data(data, analysis_window)

            # If empty data, return minimal output
            if data_window.empty:
                logger.info("Empty data for metric_id=%s. Returning minimal output.", metric_id)
                return self.handle_empty_data(metric_id, analysis_window)

            # Define lookback_end
            lookback_end = pd.to_datetime(analysis_window.end_date)

            # Create a copy with date as index for resampling
            data_window_indexed = data_window.copy()
            data_window_indexed.set_index("date", inplace=True)

            # Filter to specified number of periods
            period_data = self._filter_to_periods(data_window_indexed, num_periods)

            # If insufficient data after filtering, return minimal output
            if len(period_data) < 2:
                logger.info("Insufficient data after filtering for metric_id=%s. Returning minimal output.", metric_id)
                return self.handle_empty_data(metric_id, analysis_window)

            # Get trend analysis data
            trend_analysis_df = process_control_analysis(
                df=period_data,
                date_col="date",
                value_col="value",
                half_average_point=len(period_data) // 2,
            )

            # Compute period metrics (growth, acceleration)
            period_metrics = self._compute_period_metrics(period_data)

            # Calculate growth summary statistics
            growth_stats = self._calculate_growth_statistics(period_data, period_metrics)

            # Analyze trends using trend analysis data
            trend_info = self._analyze_trends(trend_analysis_df, avg_pop_growth=growth_stats.average_pop_growth)

            # Detect record values
            value_rankings = self._detect_record_values(data_window)

            # Analyze seasonality using the new primitive
            seasonality_pattern = self._detect_seasonality_pattern(data_window, lookback_end)

            # Calculate benchmark comparisons using the optimized approach
            benchmark_comparison = calculate_benchmark_comparisons(
                data_window, grain, date_col="date", value_col="value"
            )

            # Detect trend exception
            trend_exception = self._detect_trend_exceptions(trend_analysis_df)

            # Prepare trend analysis objects
            trend_analysis = self._prepare_trend_analysis_data(trend_analysis_df)

            # Construct result object
            result = {
                "pattern": self.name,
                "version": self.version,
                "metric_id": metric_id,
                "analysis_window": analysis_window,
                "num_periods": len(data_window),
                "analysis_date": analysis_date,
                "period_metrics": period_metrics,
                "growth_stats": growth_stats,
                "current_trend": trend_info["current_trend"],
                "previous_trend": trend_info["previous_trend"],
                "trend_analysis": trend_analysis,
                "high_rank": value_rankings["high_rank"],
                "low_rank": value_rankings["low_rank"],
                "seasonality": seasonality_pattern,
                "benchmark_comparison": benchmark_comparison,
                "trend_exception": trend_exception,
            }

            logger.info("Successfully analyzed historical performance for metric_id=%s", metric_id)
            return self.validate_output(result)

        except Exception as e:
            logger.error("Error executing historical performance pattern: %s", str(e), exc_info=True)
            if isinstance(e, LeversError):
                raise
            raise PatternError(
                f"Error executing {self.name} for metric {metric_id}: {str(e)}",
                pattern_name=self.name,
                details={
                    "metric_id": metric_id,
                    "original_error": type(e).__name__,
                },
            ) from e

    def _filter_to_periods(self, data_window: pd.DataFrame, num_periods: int) -> pd.DataFrame:
        """
        Filter the data to the specified number of periods.

        Args:
            data_window: DataFrame with data in the analysis window (with date as index)
            num_periods: Number of periods to include

        Returns:
            DataFrame filtered to specified number of periods
        """
        period_data = data_window.reset_index()

        # Keep only up to the last `num_periods` entries
        if len(period_data) > num_periods:
            period_data = period_data.iloc[-num_periods:].copy()

        return period_data

    def _compute_period_metrics(self, period_data: pd.DataFrame) -> list[PeriodMetrics]:
        """
        Compute period metrics including growth rates, acceleration rates.

        Args:
            period_data: DataFrame containing time series data at the appropriate grain

        Returns:
            List of PeriodMetrics objects containing growth, acceleration
        """
        # Compute period-over-period (PoP) growth
        grouped_growth = calculate_pop_growth(
            df=period_data,
            date_col="date",
            value_col="value",
            periods=1,
            fill_method=None,
            annualize=False,
            growth_col_name="pop_growth",
        )

        # Build the period metrics list
        period_metrics = []

        # Create PeriodMetrics objects from POP growth
        for i in range(1, len(grouped_growth)):
            curr = grouped_growth.iloc[i]
            prev = grouped_growth.iloc[i - 1]

            # Calculate acceleration
            pop_growth = float(curr["pop_growth"]) if not pd.isna(curr["pop_growth"]) else None
            prev_growth = float(prev["pop_growth"]) if not pd.isna(prev["pop_growth"]) and i > 1 else None

            pop_acceleration = None
            if pop_growth is not None and prev_growth is not None:
                pop_acceleration = pop_growth - prev_growth

            # Create period metrics with basic fields
            period_metrics.append(
                PeriodMetrics(
                    period_start=prev["date"].strftime("%Y-%m-%d"),
                    period_end=curr["date"].strftime("%Y-%m-%d"),
                    pop_growth_percent=pop_growth,
                    pop_acceleration_percent=pop_acceleration,
                )
            )
        return period_metrics

    def _prepare_trend_analysis_data(self, trend_analysis_df: pd.DataFrame) -> list[TrendAnalysis]:
        """
        Compute trend analysis objects from Trend Analysis data.

        Args:
            trend_analysis_df: DataFrame containing time series data with Trend Analysis results

        Returns:
            List of TrendAnalysis objects containing trend information for each data point
        """
        # Convert DataFrame to list of dictionaries for efficient processing
        records = trend_analysis_df.to_dict("records")

        # Create TrendAnalysis objects using list comprehension
        trend_analysis = [
            TrendAnalysis(
                value=record["value"],
                date=(
                    record["date"].strftime("%Y-%m-%d") if hasattr(record["date"], "strftime") else str(record["date"])
                ),
                central_line=record.get("central_line"),
                ucl=record.get("ucl"),
                lcl=record.get("lcl"),
                slope=record.get("slope"),
                slope_change_percent=record.get("slope_change"),
                trend_signal_detected=record.get("trend_signal_detected", False),
            )
            for record in records
        ]

        return trend_analysis

    def _calculate_growth_statistics(
        self,
        period_data: pd.DataFrame,
        period_metrics: list[PeriodMetrics],
    ) -> GrowthStats:
        """
        Calculate growth statistics including current growth, average growth,
        acceleration, and consecutive periods accelerating/slowing.

        Args:
            period_data: DataFrame containing time series data at the appropriate grain
            period_metrics: List of period metrics including growth and acceleration

        Returns:
            GrowthStats object containing growth statistics,
            - current_pop_growth: float, the growth rate of the current period
            - average_pop_growth: float, the average growth rate over the analysis window
            - current_growth_acceleration: float, the acceleration rate of the current period
            - num_periods_accelerating: int, the number of consecutive periods accelerating
            - num_periods_slowing: int, the number of consecutive periods slowing
        """
        # Current period-over-period growth
        current_pop_growth = period_metrics[-1].pop_growth_percent if period_metrics else None

        # Calculate average growth using our primitive
        avg_growth_results = calculate_average_growth(period_data, "date", "value", AverageGrowthMethod.ARITHMETIC)
        avg_pop_growth = avg_growth_results.average_growth

        # Calculate overall growth
        overall_growth = calculate_overall_growth(period_data, "value")

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
            overall_growth=overall_growth,
        )

    def _analyze_trends(self, df: pd.DataFrame, avg_pop_growth: float | None) -> dict[str, TrendInfo | None]:
        """
        Analyze metric trends and classify current and previous trends using Trend Analysis data.

        Args:
            df: DataFrame containing time series data with Trend Analysis results
            avg_pop_growth: Average growth rate over the analysis window

        Returns:
            Dictionary containing current and previous trend information
        """
        if len(df) < 2:
            return {
                "current_trend": None,
                "previous_trend": None,
            }

        # Check if all SPC analysis results are null/None - if so, cannot analyze trends
        spc_columns = ["central_line", "ucl", "lcl"]
        if all(df[col].isna().all() for col in spc_columns if col in df.columns):
            return {
                "current_trend": None,
                "previous_trend": None,
            }

        analyzed_trend_df = analyze_trend_using_spc_analysis(df)

        # Get the most recent trend type from Trend Analysis
        current_trend_type = analyzed_trend_df["trend_type"].iloc[-1]
        if current_trend_type is None:
            return {
                "current_trend": None,
                "previous_trend": None,
            }

        # Calculate the consecutive count of same trend type from the end (vectorized approach)
        # This finds the size of the last contiguous block where trend_type == current_trend_type
        same_type_mask = analyzed_trend_df["trend_type"] == current_trend_type
        consecutive_duration = int((same_type_mask[::-1].cumprod()).sum())

        # Calculate the actual start index of the current trend streak
        start_idx = len(analyzed_trend_df) - consecutive_duration
        trend_start_date = analyzed_trend_df["date"].iloc[start_idx].strftime("%Y-%m-%d")

        # For "previous" trend info, analyze only the data before the current trend started
        previous_subset = analyzed_trend_df.iloc[:start_idx] if start_idx > 0 else pd.DataFrame()
        previous_trend_info = self._analyze_previous_trend(previous_subset) if not previous_subset.empty else None

        return {
            "current_trend": TrendInfo(
                trend_type=current_trend_type,
                start_date=trend_start_date,
                average_pop_growth=avg_pop_growth,
                duration_grains=consecutive_duration,
            ),
            "previous_trend": previous_trend_info,
        }

    def _analyze_previous_trend(self, trend_analysis_df: pd.DataFrame) -> TrendInfo | None:
        """
        Analyze the previous trend from the subset of data before the current trend.

        Args:
            trend_analysis_df: DataFrame containing time series data with Trend Analysis results
                              (should be the subset before the current trend starts)

        Returns:
            TrendInfo object containing previous trend information
        """
        if len(trend_analysis_df) < 1:
            return None

        # Get the most recent trend type from this subset (which is the previous trend)
        previous_trend_type = trend_analysis_df["trend_type"].iloc[-1]
        if previous_trend_type is None:
            return None

        # Apply the same logic to find the start of the previous trend streak
        same_type_mask = trend_analysis_df["trend_type"] == previous_trend_type
        consecutive_duration = int((same_type_mask[::-1].cumprod()).sum())

        # Calculate the actual start index of the previous trend streak
        start_idx = len(trend_analysis_df) - consecutive_duration
        previous_trend_start_date = trend_analysis_df["date"].iloc[start_idx].strftime("%Y-%m-%d")

        # Calculate average growth for the previous trend subset
        prev_trend_subset = trend_analysis_df.iloc[start_idx:]
        prev_subset_growth_results = calculate_average_growth(
            prev_trend_subset, "date", "value", AverageGrowthMethod.ARITHMETIC
        )
        previous_trend_average_pop_growth = prev_subset_growth_results.average_growth

        return TrendInfo(
            trend_type=previous_trend_type,
            start_date=previous_trend_start_date,
            average_pop_growth=previous_trend_average_pop_growth,
            duration_grains=consecutive_duration,
        )

    def _detect_record_values(self, data_window: pd.DataFrame) -> dict[str, RankSummary | None]:
        """
        Detect high and low values in the data window, always returning rank information.

        Args:
            data_window: DataFrame with date and value columns

        Returns:
            Dictionary containing high and low value summaries,
            - high_rank: RankSummary object containing high value information
                - value: float, the value of the high rank
                - rank: int, the rank of the high value
                - duration_grains: int, the number of grains in the duration of the high value
                - prior_record_value: float, the value of the prior record
                - prior_record_date: str, the date of the prior record
                - absolute_delta_from_prior_record: float, the absolute delta from the prior record
                - relative_delta_from_prior_record: float, the relative delta from the prior record
            - low_rank: RankSummary object containing low value information
                - value: float, the value of the low rank
                - rank: int, the rank of the low value
                - duration_grains: int, the number of grains in the duration of the low value
                - prior_record_value: float, the value of the prior record
                - prior_record_date: str, the date of the prior record
                - absolute_delta_from_prior_record: float, the absolute delta from the prior record
                - relative_delta_from_prior_record: float, the relative delta from the prior record
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
            Seasonality object containing seasonality analysis results,
            - is_following_expected_pattern: bool, whether the current value is following the expected pattern
            - expected_change_percent: float, the expected change percent
            - actual_change_percent: float, the actual change percent
            - deviation_percent: float, the deviation percent
            or None if insufficient data
        """
        return detect_seasonality_pattern(data_window, lookback_end, "date", "value")

    def _detect_trend_exceptions(self, trend_analysis_df: pd.DataFrame) -> TrendException | None:
        """
        Detect anomalies (spikes or drops) in the time series.

        Args:
            trend_analysis_df: DataFrame containing time series data with Trend Analysis results

        Returns:
            TrendException object containing trend exception details,
            - type: str, the type of exception
            - current_value: float, the current value
            - normal_range_low: float, the lower bound of the normal range
            - normal_range_high: float, the upper bound of the normal range
            - absolute_delta_from_normal_range: float, the absolute delta from the normal range
            or None if no exceptions are detected
        """
        return detect_trend_exceptions_using_spc_analysis(
            df=trend_analysis_df,
            date_col="date",
            value_col="value",
            window_size=min(5, len(trend_analysis_df)),
        )

    @classmethod
    def get_default_config(cls) -> PatternConfig:
        """
        Get the default configuration for the historical performance pattern.

        Returns:
            PatternConfig with historical performance pattern configuration,
            - pattern_name: str, the name of the pattern
            - description: str, the description of the pattern
            - version: str, the version of the pattern
            - data_sources: list of DataSource objects, the data sources required by the pattern
            - analysis_window: AnalysisWindowConfig object, the analysis window configuration
        """
        return PatternConfig(
            pattern_name=cls.name,
            description=cls.description,
            version=cls.version,
            data_sources=[DataSource(source_type=DataSourceType.METRIC_TIME_SERIES, is_required=True, data_key="data")],
            analysis_window=AnalysisWindowConfig(
                strategy=WindowStrategy.FIXED_TIME, days=410, min_days=30, max_days=410
            ),
        )
