"""
Story evaluator for the historical performance pattern.
"""

import logging
from typing import Any

import pandas as pd

from commons.models.enums import Granularity
from levers.models import TrendExceptionType, TrendType
from levers.models.patterns import HistoricalPerformance
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator import StoryEvaluatorBase, render_story_text

logger = logging.getLogger(__name__)


class HistoricalPerformanceEvaluator(StoryEvaluatorBase[HistoricalPerformance]):
    """
    Evaluates historical performance pattern results and generates stories.

    Story types generated:
    - Growth Stories: SLOWING_GROWTH, ACCELERATING_GROWTH
    - Trend Stories: STABLE_TREND, NEW_UPWARD_TREND, NEW_DOWNWARD_TREND, PERFORMANCE_PLATEAU
    - Trend Exceptions: SPIKE, DROP
    - Performance Change: IMPROVING_PERFORMANCE, WORSENING_PERFORMANCE
    - Record Values: RECORD_HIGH, RECORD_LOW
    - Benchmark Comparisons: BENCHMARKS
    """

    pattern_name = "historical_performance"

    async def evaluate(self, pattern_result: HistoricalPerformance, metric: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Evaluate the historical performance pattern result and generate stories.

        Args:
            pattern_result: Historical performance pattern result
            metric: Metric details

        Returns:
            List of story dictionaries
        """

        stories = []
        metric_id = pattern_result.metric_id
        grain = Granularity(pattern_result.analysis_window.grain)

        # Growth Stories
        if pattern_result.growth_stats and pattern_result.growth_stats.current_growth_acceleration:
            if pattern_result.growth_stats.current_growth_acceleration < 0:
                stories.append(self._create_slowing_growth_story(pattern_result, metric_id, metric, grain))
            elif pattern_result.growth_stats.current_growth_acceleration > 0:
                stories.append(self._create_accelerating_growth_story(pattern_result, metric_id, metric, grain))

        # Trend Stories
        if pattern_result.current_trend:
            trend_type = pattern_result.current_trend.trend_type
            if trend_type == TrendType.STABLE:
                stories.append(self._create_stable_trend_story(pattern_result, metric_id, metric, grain))
            elif self._is_new_trend(pattern_result):
                if trend_type == TrendType.UPWARD:
                    stories.append(self._create_new_upward_trend_story(pattern_result, metric_id, metric, grain))
                elif trend_type == TrendType.DOWNWARD:
                    stories.append(self._create_new_downward_trend_story(pattern_result, metric_id, metric, grain))
            elif trend_type == TrendType.PLATEAU:
                stories.append(self._create_performance_plateau_story(pattern_result, metric_id, metric, grain))

        # Improving/Worsening Performance
        if pattern_result.current_trend and pattern_result.current_trend.average_pop_growth:
            if pattern_result.current_trend.average_pop_growth > 0:
                stories.append(self._create_improving_performance_story(pattern_result, metric_id, metric, grain))
            elif pattern_result.current_trend.average_pop_growth < 0:
                stories.append(self._create_worsening_performance_story(pattern_result, metric_id, metric, grain))

        # Trend Exception Stories (Spikes & Drops)
        if pattern_result.trend_exception:
            if pattern_result.trend_exception.type == TrendExceptionType.SPIKE:
                stories.append(self._create_spike_story(pattern_result, metric_id, metric, grain))
            elif pattern_result.trend_exception.type == TrendExceptionType.DROP:
                stories.append(self._create_drop_story(pattern_result, metric_id, metric, grain))

        # Record Value Stories
        if pattern_result.high_rank and pattern_result.high_rank.rank <= 2:  # Consider top 2 as record high
            stories.append(self._create_record_high_story(pattern_result, metric_id, metric, grain))
        elif pattern_result.low_rank and pattern_result.low_rank.rank <= 2:  # Consider bottom 2 as record low
            stories.append(self._create_record_low_story(pattern_result, metric_id, metric, grain))

        # Benchmark Stories
        if pattern_result.benchmark_comparison and pattern_result.high_rank:
            stories.append(self._create_benchmark_story(pattern_result, metric_id, metric, grain))

        return stories

    def _is_new_trend(self, pattern_result: HistoricalPerformance) -> bool:
        """Check if there's a new trend (current trend exists and different from previous)."""
        return (
            pattern_result.previous_trend is not None
            and pattern_result.current_trend.trend_type != pattern_result.previous_trend.trend_type  # type: ignore
        )

    def _populate_template_context(
        self, pattern_result: HistoricalPerformance, metric: dict, grain: Granularity, **kwargs
    ) -> dict[str, Any]:
        """
        Populate context for template rendering based on story type.

        Args:
            pattern_result: Historical performance pattern result
            metric: Metric details
            grain: Granularity of the analysis
            **kwargs: Additional keyword arguments including:
                - include: List of components to include in the context (default: [])

        Returns:
            Template context dictionary with only the necessary fields for the given story type
        """

        include = kwargs.get("include", [])
        context = self.prepare_base_context(metric, grain)

        # Add growth stats
        if "growth_stats" in include and pattern_result.growth_stats:
            context.update(
                {
                    "current_growth": abs(pattern_result.growth_stats.current_pop_growth or 0),
                    "average_growth": abs(pattern_result.growth_stats.average_pop_growth or 0),
                    "growth_acceleration": abs(pattern_result.growth_stats.current_growth_acceleration or 0),
                    "num_periods_accelerating": pattern_result.growth_stats.num_periods_accelerating,
                    "num_periods_slowing": pattern_result.growth_stats.num_periods_slowing,
                }
            )

        # Add current trend info
        if "current_trend" in include and pattern_result.current_trend:
            context.update(
                {
                    "trend_start_date": pattern_result.current_trend.start_date,
                    "trend_duration": pattern_result.current_trend.duration_grains,
                    "trend_avg_growth": abs(pattern_result.current_trend.average_pop_growth or 0),
                    "trend_direction": (
                        "increase"
                        if pattern_result.current_trend.average_pop_growth
                        and pattern_result.current_trend.average_pop_growth > 0
                        else "decrease"
                    ),
                }
            )

        # Add previous trend info
        if "previous_trend" in include and pattern_result.previous_trend:
            context.update(
                {
                    "prev_trend_duration": pattern_result.previous_trend.duration_grains,
                    "prev_trend_avg_growth": abs(pattern_result.previous_trend.average_pop_growth or 0),
                }
            )

        # Add rank summaries
        if "high_rank" in include and pattern_result.high_rank:
            context.update(
                {
                    "high_value": pattern_result.high_rank.value,
                    "high_rank": pattern_result.high_rank.rank,
                    "high_duration": pattern_result.high_rank.duration_grains,
                    "high_prior_value": pattern_result.high_rank.prior_record_value,
                    "high_prior_date": pattern_result.high_rank.prior_record_date,
                    "high_delta": abs(pattern_result.high_rank.absolute_delta_from_prior_record or 0),
                    "high_delta_percent": abs(pattern_result.high_rank.relative_delta_from_prior_record or 0),
                }
            )

        if "low_rank" in include and pattern_result.low_rank:
            context.update(
                {
                    "low_value": pattern_result.low_rank.value,
                    "low_rank": pattern_result.low_rank.rank,
                    "low_duration": pattern_result.low_rank.duration_grains,
                    "low_prior_value": pattern_result.low_rank.prior_record_value,
                    "low_prior_date": pattern_result.low_rank.prior_record_date,
                    "low_delta": abs(pattern_result.low_rank.absolute_delta_from_prior_record or 0),
                    "low_delta_percent": abs(pattern_result.low_rank.relative_delta_from_prior_record or 0),
                }
            )

        # Add trend exception info
        if "trend_exception" in include and pattern_result.trend_exception:
            context.update(
                {
                    "exception_value": pattern_result.trend_exception.current_value,
                    "normal_low": pattern_result.trend_exception.normal_range_low,
                    "normal_high": pattern_result.trend_exception.normal_range_high,
                    "deviation": abs(pattern_result.trend_exception.absolute_delta_from_normal_range or 0),
                    "deviation_percent": abs(pattern_result.trend_exception.magnitude_percent or 0),
                }
            )

        # Add benchmark info
        if "benchmark_comparison" in include and pattern_result.benchmark_comparison:
            # Map reference periods to more readable names for stories
            period_display_names = {
                "WTD": "Week to Date",
                "MTD": "Month to Date",
                "QTD": "Quarter to Date",
                "YTD": "Year to Date",
            }
            reference_period_mapping = {"WTD": "week", "MTD": "month", "QTD": "quarter", "YTD": "year"}

            # Get the benchmark data
            benchmark = pattern_result.benchmark_comparison

            # Add benchmark comparison info with direct mapping to template variables
            context["partial_interval_label"] = period_display_names.get(benchmark.reference_period)
            context["partial_interval"] = benchmark.reference_period
            context["change_percent"] = abs(benchmark.change_percent or 0)
            context["comparison_direction"] = (
                "higher" if benchmark.change_percent and benchmark.change_percent > 0 else "lower"
            )
            context["reference_period"] = reference_period_mapping.get(benchmark.reference_period)

        return context

    def _create_slowing_growth_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a SLOWING_GROWTH story."""
        story_group = StoryGroup.GROWTH_RATES
        story_type = StoryType.SLOWING_GROWTH

        context = self._populate_template_context(pattern_result, metric, grain, include=["growth_stats"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # First prepare the series data with growth rates story-specific customizations
        series_df = self._prepare_series_data_with_pop_growth(pattern_result)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        return self.prepare_story_model(
            genre=StoryGenre.GROWTH,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            series_data=series_data,
            **context,
        )

    def _create_accelerating_growth_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create an ACCELERATING_GROWTH story."""
        story_group = StoryGroup.GROWTH_RATES
        story_type = StoryType.ACCELERATING_GROWTH

        context = self._populate_template_context(pattern_result, metric, grain, include=["growth_stats"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # First prepare the series data with growth rates story-specific customizations
        series_df = self._prepare_series_data_with_pop_growth(pattern_result)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        return self.prepare_story_model(
            genre=StoryGenre.GROWTH,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            series_data=series_data,
            **context,
        )

    def _create_stable_trend_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a STABLE_TREND story."""
        story_group = StoryGroup.TREND_CHANGES
        story_type = StoryType.STABLE_TREND

        context = self._populate_template_context(
            pattern_result, metric, grain, include=["current_trend", "previous_trend"]
        )

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # prepare series data with SPC data
        series_df = self._prepare_trend_analysis_series_data(pattern_result)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            series_data=series_data,
            **context,
        )

    def _create_new_upward_trend_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a NEW_UPWARD_TREND story."""
        story_group = StoryGroup.TREND_CHANGES
        story_type = StoryType.NEW_UPWARD_TREND
        context = self._populate_template_context(
            pattern_result, metric, grain, include=["current_trend", "previous_trend"]
        )

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # prepare series data with SPC data
        series_df = self._prepare_trend_analysis_series_data(pattern_result)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            series_data=series_data,
            **context,
        )

    def _create_new_downward_trend_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a NEW_DOWNWARD_TREND story."""
        story_group = StoryGroup.TREND_CHANGES
        story_type = StoryType.NEW_DOWNWARD_TREND

        context = self._populate_template_context(
            pattern_result, metric, grain, include=["current_trend", "previous_trend"]
        )

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # prepare series data with SPC data
        series_df = self._prepare_trend_analysis_series_data(pattern_result)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            series_data=series_data,
            **context,
        )

    def _create_performance_plateau_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a PERFORMANCE_PLATEAU story."""
        story_group = StoryGroup.TREND_CHANGES
        story_type = StoryType.PERFORMANCE_PLATEAU

        context = self._populate_template_context(
            pattern_result, metric, grain, include=["current_trend", "previous_trend"]
        )

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # prepare series data with SPC data
        series_df = self._prepare_trend_analysis_series_data(pattern_result)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            series_data=series_data,
            **context,
        )

    def _create_spike_story(
        self,
        pattern_result: HistoricalPerformance,
        metric_id: str,
        metric: dict,
        grain: Granularity,
    ) -> dict[str, Any]:
        """Create a SPIKE story."""
        story_group = StoryGroup.TREND_EXCEPTIONS
        story_type = StoryType.SPIKE
        context = self._populate_template_context(pattern_result, metric, grain, include=["trend_exception"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # prepare series data with SPC data
        series_df = self._prepare_trend_analysis_series_data(pattern_result)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            series_data=series_data,
            **context,
        )

    def _create_drop_story(
        self,
        pattern_result: HistoricalPerformance,
        metric_id: str,
        metric: dict,
        grain: Granularity,
    ) -> dict[str, Any]:
        """Create a DROP story."""
        story_group = StoryGroup.TREND_EXCEPTIONS
        story_type = StoryType.DROP

        context = self._populate_template_context(pattern_result, metric, grain, include=["trend_exception"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # prepare series data with SPC data
        series_df = self._prepare_trend_analysis_series_data(pattern_result)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            series_data=series_data,
            **context,
        )

    def _create_improving_performance_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create an IMPROVING_PERFORMANCE story."""

        story_group = StoryGroup.LONG_RANGE
        story_type = StoryType.IMPROVING_PERFORMANCE

        context = self._populate_template_context(pattern_result, metric, grain, include=["current_trend"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # First prepare the series data with growth rates story-specific customizations
        series_df = self._prepare_series_data_with_pop_growth(pattern_result)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            series_data=series_data,
            **context,
        )

    def _create_worsening_performance_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a WORSENING_PERFORMANCE story."""

        story_group = StoryGroup.LONG_RANGE
        story_type = StoryType.WORSENING_PERFORMANCE

        context = self._populate_template_context(pattern_result, metric, grain, include=["current_trend"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # First prepare the series data with growth rates story-specific customizations
        series_df = self._prepare_series_data_with_pop_growth(pattern_result)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            series_data=series_data,
            **context,
        )

    def _create_record_high_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a RECORD_HIGH story."""

        story_group = StoryGroup.RECORD_VALUES
        story_type = StoryType.RECORD_HIGH
        context = self._populate_template_context(pattern_result, metric, grain, include=["high_rank"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_record_low_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a RECORD_LOW story."""

        story_group = StoryGroup.RECORD_VALUES
        story_type = StoryType.RECORD_LOW
        context = self._populate_template_context(pattern_result, metric, grain, include=["low_rank"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_benchmark_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a BENCHMARKS story."""
        story_group = StoryGroup.BENCHMARK_COMPARISONS
        context = self._populate_template_context(
            pattern_result, metric, grain, include=["high_rank", "benchmark_comparison"]
        )

        # Only generate story if we have both benchmark comparison and high rank data
        title = render_story_text(StoryType.BENCHMARKS, "title", context)
        detail = render_story_text(StoryType.BENCHMARKS, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.BENCHMARKS,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _prepare_series_data_with_pop_growth(self, pattern_result: HistoricalPerformance) -> pd.DataFrame:
        """
        Prepare the series data for growth stories.
        """

        # Make a copy to avoid modifying the original
        series_df = self.series_df.copy() if self.series_df is not None else pd.DataFrame()

        if not pattern_result.period_metrics:
            return series_df

        series_df["date"] = pd.to_datetime(series_df["date"])

        # Add period metrics to series data
        period_df = pd.DataFrame([period_metric.model_dump() for period_metric in pattern_result.period_metrics])

        # Map period_end to date for joining
        period_df["date"] = pd.to_datetime(period_df["period_end"])

        # Create a temporary merged dataframe to calculate growth values
        merged_df = (
            series_df[["date", "value"]]
            .merge(period_df[["date", "pop_growth_percent"]], on="date", how="left")
            .sort_values("date")
        )

        return merged_df

    def _prepare_trend_analysis_series_data(self, pattern_result: HistoricalPerformance) -> pd.DataFrame:
        """
        Prepare the series data for trend analysis stories.

        This method extracts SPC-related fields from the trend_analysis results
        (which are derived from process_control_analysis) and merges them
        with the base series data for visualization and story generation.

        Args:
            pattern_result: Historical performance pattern result

        Returns:
            DataFrame with series data and SPC metrics
        """
        # Make a copy to avoid modifying the original
        series_df = self.series_df.copy() if self.series_df is not None else pd.DataFrame()

        if not pattern_result.trend_analysis:  # Check trend_analysis
            return series_df

        series_df["date"] = pd.to_datetime(series_df["date"])

        # Extract data from trend_analysis results
        # TrendAnalysis objects contain all necessary fields
        trend_analysis_data = [data.model_dump() for data in pattern_result.trend_analysis]
        trend_analysis_df = pd.DataFrame(trend_analysis_data)

        # Select columns that are expected by stories.
        # These should align with fields in the TrendAnalysis model.
        columns_to_merge = [
            "date",
            "value",
            "central_line",
            "ucl",
            "lcl",
            "slope",
            "slope_change_percent",
            "trend_signal_detected",
            "trend_type",
        ]

        # Filter trend_analysis_df to only include necessary columns, ensure 'date' is present
        available_columns = [col for col in columns_to_merge if col in trend_analysis_df.columns]
        df_to_merge = trend_analysis_df[available_columns].drop(columns=["value"], errors="ignore")
        df_to_merge["date"] = pd.to_datetime(df_to_merge["date"])

        merged_df = pd.merge(series_df, df_to_merge, on="date", how="left").sort_values("date")

        return merged_df
