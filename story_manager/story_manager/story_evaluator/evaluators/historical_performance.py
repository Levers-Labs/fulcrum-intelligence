"""
Story evaluator for the historical performance pattern.
"""

import logging
from typing import Any, cast

import numpy as np
import pandas as pd

from commons.models.enums import Granularity
from levers.models import ComparisonType, TrendExceptionType, TrendType
from levers.models.patterns import BenchmarkComparison, GrowthStats, HistoricalPerformance
from levers.models.patterns.historical_performance import RankSummary
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
        if pattern_result.growth_stats:
            if self._is_slowing_growth(pattern_result.growth_stats):
                stories.append(self._create_slowing_growth_story(pattern_result, metric_id, metric, grain))
            elif self._is_accelerating_growth(pattern_result.growth_stats):
                stories.append(self._create_accelerating_growth_story(pattern_result, metric_id, metric, grain))

        # Improving/Worsening Performance
        if pattern_result.performance_trend and pattern_result.performance_trend.overall_growth:
            if (
                pattern_result.performance_trend.num_periods_improving > 2
                and pattern_result.performance_trend.overall_growth > 0
            ):
                stories.append(self._create_improving_performance_story(pattern_result, metric_id, metric, grain))
            elif (
                pattern_result.performance_trend.num_periods_worsening > 2
                and pattern_result.performance_trend.overall_growth < 0
            ):
                stories.append(self._create_worsening_performance_story(pattern_result, metric_id, metric, grain))

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

        # Trend Exception Stories (Spikes & Drops)
        if pattern_result.trend_exception:
            if pattern_result.trend_exception.type == TrendExceptionType.SPIKE:
                stories.append(self._create_spike_story(pattern_result, metric_id, metric, grain))
            elif pattern_result.trend_exception.type == TrendExceptionType.DROP:
                stories.append(self._create_drop_story(pattern_result, metric_id, metric, grain))

        # Record Value Stories
        if (
            pattern_result.high_rank
            and pattern_result.high_rank.rank <= 2
            and self._is_valid_record_value(pattern_result.high_rank)
        ):  # Consider top 2 as record high
            stories.append(self._create_record_high_story(pattern_result, metric_id, metric, grain))
        elif (
            pattern_result.low_rank
            and pattern_result.low_rank.rank <= 2
            and self._is_valid_record_value(pattern_result.low_rank)
        ):  # Consider bottom 2 as record low
            stories.append(self._create_record_low_story(pattern_result, metric_id, metric, grain))

        # Benchmark Stories (only for week and month grains)
        if (
            pattern_result.high_rank
            and grain in [Granularity.WEEK, Granularity.MONTH]
            and self._has_valid_benchmarks(pattern_result.benchmark_comparison)  # Ensure we have benchmarks
        ):
            stories.append(self._create_benchmark_story(pattern_result, metric_id, metric, grain))

        return stories

    def _is_new_trend(self, pattern_result: HistoricalPerformance) -> bool:
        """Check if there's a new trend (current trend exists and different from previous)."""
        return (
            pattern_result.previous_trend is not None
            and pattern_result.current_trend.trend_type != pattern_result.previous_trend.trend_type  # type: ignore
        )

    def _has_valid_benchmarks(self, benchmark_comparison) -> bool:
        """Check if benchmark comparison has valid benchmark data."""
        if not benchmark_comparison:
            return False

        # Check for new BenchmarkComparison model with get_all_benchmarks method
        if hasattr(benchmark_comparison, "get_all_benchmarks"):
            return bool(benchmark_comparison.get_all_benchmarks())

        # Check for old BenchmarkComparison model with direct fields
        if hasattr(benchmark_comparison, "change_percent"):
            return benchmark_comparison.change_percent is not None

        return False

    def _is_valid_record_value(self, record_value: RankSummary) -> bool:
        """Check if the current value is a record value."""

        return record_value.value != record_value.prior_record_value

    def _is_slowing_growth(self, growth_stats: GrowthStats) -> bool:
        """Check if the current growth is slowing down."""
        current_pop_growth = growth_stats.current_pop_growth
        average_pop_growth = growth_stats.average_pop_growth
        num_periods_slowing = growth_stats.num_periods_slowing

        if not current_pop_growth or not average_pop_growth or not num_periods_slowing:
            return False

        # if the current growth is less than the average growth and the number of periods slowing is greater than at
        # least 1, then it is slowing growth
        return current_pop_growth < average_pop_growth and num_periods_slowing > 1

    def _is_accelerating_growth(self, growth_stats: GrowthStats) -> bool:
        """Check if the current growth is accelerating."""
        current_pop_growth = growth_stats.current_pop_growth
        average_pop_growth = growth_stats.average_pop_growth
        num_periods_accelerating = growth_stats.num_periods_accelerating

        if not current_pop_growth or not average_pop_growth or not num_periods_accelerating:
            return False

        # if the current growth is greater than the average growth and the number of periods accelerating is greater
        # than at least 1, then it is accelerating growth
        return current_pop_growth > average_pop_growth and num_periods_accelerating > 1

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
                    "current_growth": pattern_result.growth_stats.current_pop_growth or 0,
                    "average_growth": pattern_result.growth_stats.average_pop_growth or 0,
                    "growth_acceleration": pattern_result.growth_stats.current_growth_acceleration or 0,
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
                    "trend_avg_growth": pattern_result.current_trend.average_pop_growth or 0,
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
                    "prev_trend_avg_growth": pattern_result.previous_trend.average_pop_growth or 0,
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
                    "deviation": pattern_result.trend_exception.absolute_delta_from_normal_range or 0,
                    "deviation_percent": pattern_result.trend_exception.magnitude_percent or 0,
                }
            )

        # Add benchmark info
        if "benchmark_comparison" in include and pattern_result.benchmark_comparison:
            context["benchmark"] = self._prepare_benchmark_context(grain, pattern_result.benchmark_comparison)

        # Add performance metrics info
        if "performance_trend" in include and pattern_result.performance_trend:
            context.update(
                {
                    "num_periods_improving": pattern_result.performance_trend.num_periods_improving or 0,
                    "num_periods_worsening": pattern_result.performance_trend.num_periods_worsening or 0,
                    "start_date": pattern_result.performance_trend.start_date or "",
                    "avg_growth": pattern_result.performance_trend.average_growth or 0,
                    "overall_growth": pattern_result.performance_trend.overall_growth or 0,
                }
            )

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
        series_df = self._prepare_trend_changes_series_data(pattern_result)
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
        series_df = self._prepare_trend_changes_series_data(pattern_result)
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
        series_df = self._prepare_trend_changes_series_data(pattern_result)
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
        series_df = self._prepare_trend_changes_series_data(pattern_result)
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
        series_df = self._prepare_spike_drop_series_data(pattern_result)
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
        series_df = self._prepare_spike_drop_series_data(pattern_result)
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

        context = self._populate_template_context(
            pattern_result, metric, grain, include=["current_trend", "performance_trend"]
        )

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

        context = self._populate_template_context(
            pattern_result, metric, grain, include=["current_trend", "performance_trend"]
        )

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

        series_data = self._prepare_record_values_series_data(context["high_duration"])

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

    def _create_record_low_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a RECORD_LOW story."""

        story_group = StoryGroup.RECORD_VALUES
        story_type = StoryType.RECORD_LOW
        context = self._populate_template_context(pattern_result, metric, grain, include=["low_rank"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        series_data = self._prepare_record_values_series_data(context["low_duration"])

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

    def _prepare_benchmark_context(
        self, grain: Granularity, benchmark_comparison: BenchmarkComparison
    ) -> dict[str, Any]:
        """
        Prepare the benchmark context for template rendering.

        Args:
            benchmark_comparison: The benchmark comparison object.

        Returns:
            A dictionary containing the benchmark context.
        """
        # Get all available benchmarks
        all_benchmarks = benchmark_comparison.get_all_benchmarks()

        # create benchmark context dict
        current_label_map = {Granularity.WEEK: "current week's", Granularity.MONTH: "current month's"}
        benchmark_context = {
            "current_value": benchmark_comparison.current_value,
            "current_period": current_label_map.get(grain, benchmark_comparison.current_period),
        }

        # Create lists for multiple comparisons
        comparison_details = []
        comparison_summaries = []
        # Process each benchmark comparison
        for comparison_type, benchmark in all_benchmarks.items():
            # Create readable labels for each comparison type
            comparison_labels = {
                ComparisonType.LAST_WEEK: "last week",
                ComparisonType.WEEK_IN_LAST_MONTH: "the same week last month",
                ComparisonType.WEEK_IN_LAST_QUARTER: "the same week last quarter",
                ComparisonType.WEEK_IN_LAST_YEAR: "the same week last year",
                ComparisonType.LAST_MONTH: "last month",
                ComparisonType.MONTH_IN_LAST_QUARTER: "the same month last quarter",
                ComparisonType.MONTH_IN_LAST_YEAR: "the same month last year",
            }
            comparison_label = comparison_labels.get(comparison_type, benchmark.reference_period)
            change_direction = "higher" if benchmark.change_percent and benchmark.change_percent > 0 else "lower"
            change_percent = abs(benchmark.change_percent or 0)
            # Add to comparison details for the story
            comparison_details.append(
                {
                    "label": comparison_label,
                    "change_percent": change_percent,
                    "direction": change_direction,
                    "reference_value": benchmark.reference_value,
                    "date": benchmark.reference_date,  # Add date field for sorting
                }
            )

        # Sort comparison details by date from closest to furthest
        comparison_details.sort(key=lambda x: x["date"], reverse=True)  # type: ignore

        # Convert date to string for template rendering
        for comparison in comparison_details:
            comparison["date"] = comparison["date"].strftime("%Y-%m-%d")  # type: ignore

        # Create summary text for each comparison
        for comparison in comparison_details:
            chg_pc = f"{comparison['change_percent']:.1f}%"
            comparison_summaries.append(f"{chg_pc} {comparison['direction']} than {comparison['label']}")

        # Add context variables for template rendering
        benchmark_context["comparison_details"] = comparison_details
        benchmark_context["comparison_summaries"] = comparison_summaries
        benchmark_context["num_comparisons"] = len(comparison_details)

        # Calculate historical average deviation for heuristic evaluation
        # CSV requirement: current value should be ≥5% off historical average
        benchmark_deviation_percent = 0.0
        if comparison_details:
            # Calculate average of all historical benchmark values
            historical_values = [
                detail["reference_value"]
                for detail in comparison_details
                if detail["reference_value"] is not None and isinstance(detail["reference_value"], float)
            ]
            historical_average = sum(historical_values) / len(historical_values)

            # Calculate percentage deviation of current value from historical average
            current_value = benchmark_comparison.current_value
            if historical_average != 0:
                benchmark_deviation_percent = abs((current_value - historical_average) / historical_average * 100)

        benchmark_context["deviation_percent"] = benchmark_deviation_percent

        return benchmark_context

    def _create_benchmark_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a BENCHMARKS story."""
        story_group = StoryGroup.BENCHMARK_COMPARISONS
        story_type = StoryType.BENCHMARKS
        benchmark_comparison = pattern_result.benchmark_comparison

        if not benchmark_comparison:
            logger.warning(
                "No benchmarks available for BENCHMARKS story. Skipping story creation for metric_id: %s", metric_id
            )
            raise ValueError("No benchmarks available for BENCHMARKS story. Ensure benchmark data is populated.")

        context = self._populate_template_context(
            pattern_result, metric, grain, include=["high_rank", "benchmark_comparison"]
        )

        # Generate story title and detail
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare series data for the chart visualization
        series_data = [
            {
                # todo: change later to actual date of the data
                "date": pattern_result.analysis_date.strftime("%Y-%m-%d"),
                "value": benchmark_comparison.current_value,
                # Textual representation of the current period e.g This Week, This Month
                "label": benchmark_comparison.current_period,
                "absolute_change": None,
                "change_percent": None,
            }
        ]
        for benchmark in benchmark_comparison.benchmarks.values():
            series_data.append(
                {
                    "date": benchmark.reference_date.strftime("%Y-%m-%d"),
                    "value": benchmark.reference_value,
                    # Textual representation of the reference period e.g Week Ago, Month Ago, Quarter Ago, Year Ago
                    "label": benchmark.reference_period,
                    "absolute_change": benchmark.absolute_change,
                    "change_percent": benchmark.change_percent,
                }
            )

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

    def _prepare_trend_changes_series_data(self, pattern_result: HistoricalPerformance) -> pd.DataFrame:
        """
        Prepare the series data for trend changes stories.

        This method extracts SPC-related fields from the trend_analysis results
        (which are derived from process_control_analysis) and merges them
        with the base series data for visualization and story generation.

        Args:
            pattern_result: Historical performance pattern result

        Returns:
            DataFrame with series data and SPC metrics for trend changes stories ( stable, new upward, new downward,
            performance plateau)
        """

        pop_growth_df = self._prepare_series_data_with_pop_growth(pattern_result)

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
        ]

        # Filter trend_analysis_df to only include necessary columns, ensure 'date' is present
        available_columns = [col for col in columns_to_merge if col in trend_analysis_df.columns]
        df_to_merge = trend_analysis_df[available_columns].drop(columns=["value"], errors="ignore")
        df_to_merge["date"] = pd.to_datetime(df_to_merge["date"])

        merged_df = pd.merge(pop_growth_df, df_to_merge, on="date", how="left").sort_values("date")

        return merged_df

    def _prepare_spike_drop_series_data(self, pattern_result: HistoricalPerformance) -> pd.DataFrame:
        """
        Prepare the series data for spike and drop stories.

        This method extracts SPC-related fields from the trend_analysis results
        (which are derived from process_control_analysis) and merges them
        with the base series data for visualization and story generation.

        Args:
            pattern_result: Historical performance pattern result

        Returns:
            DataFrame with series data and SPC metrics for spike and drop stories
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
        columns_to_merge = ["date", "value", "central_line", "ucl", "lcl", "slope"]

        # Filter trend_analysis_df to only include necessary columns, ensure 'date' is present
        available_columns = [col for col in columns_to_merge if col in trend_analysis_df.columns]
        df_to_merge = trend_analysis_df[available_columns].drop(columns=["value"], errors="ignore")
        df_to_merge["date"] = pd.to_datetime(df_to_merge["date"])

        merged_df = pd.merge(series_df, df_to_merge, on="date", how="left").sort_values("date")

        return merged_df

    def _prepare_record_values_series_data(self, min_series_length: int) -> list[dict[str, Any]]:
        """
        Prepare the series data for record values stories.
        """

        series = self.series_df if self.series_df is not None else pd.DataFrame()

        # Get the last n rows
        series = series.tail(min_series_length)
        series["date"] = pd.to_datetime(series["date"])
        series["date"] = series["date"].dt.date.apply(lambda d: d.isoformat())

        # Final cleanup: Replace any remaining NaN/inf values before converting to dict
        series = series.replace([float("inf"), float("-inf"), np.NaN], 0.0)

        # Add the time series data to the result
        data = series.to_dict(orient="records")
        return cast(list[dict[str, Any]], data)
