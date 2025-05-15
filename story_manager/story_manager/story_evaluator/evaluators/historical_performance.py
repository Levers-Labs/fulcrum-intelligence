"""
Story evaluator for the historical performance pattern.
"""

import logging
from typing import Any

import pandas as pd

from commons.utilities.grain_utils import GRAIN_META
from levers.models import Granularity, TrendExceptionType, TrendType
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

    def _prepare_story_series_data(
        self, pattern_result: HistoricalPerformance, story_type: StoryType
    ) -> pd.DataFrame | None:
        """
        Enhance the dataframe with pattern-specific metrics and indicators.

        Args:
            pattern_result: Historical performance pattern result
            story_type: Type of story being created

        Returns:
            A series dataframe with the additional data or None if the series dataframe is empty
        """
        if self.series_df is None or self.series_df.empty:
            return None

        # Make a copy to avoid modifying the original
        series_df = self.series_df.copy()

        # Ensure date is in datetime format
        if "date" in series_df.columns:
            series_df["date"] = pd.to_datetime(series_df["date"])

        if story_type in [
            StoryType.SLOWING_GROWTH,
            StoryType.ACCELERATING_GROWTH,
            StoryType.IMPROVING_PERFORMANCE,
            StoryType.WORSENING_PERFORMANCE,
        ]:
            # Add period metrics to series data
            if pattern_result.period_metrics:
                period_df = pd.DataFrame(
                    [period_metric.model_dump() for period_metric in pattern_result.period_metrics]
                )

                # Map period_end to date for joining
                period_df["date"] = pd.to_datetime(period_df["period_end"])

                # check if value column exists
                if "value" in series_df.columns:
                    # Create a temporary merged dataframe to calculate growth values
                    merged_df = (
                        series_df[["date", "value"]]
                        .merge(period_df[["date", "pop_growth_percent"]], on="date", how="left")
                        .sort_values("date")
                    )

                    return merged_df

        return series_df

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
        grain = pattern_result.analysis_window.grain

        # Growth Stories
        if pattern_result.growth_stats is not None and pattern_result.growth_stats.current_growth_acceleration:
            if pattern_result.growth_stats.current_growth_acceleration < 0:
                stories.append(self._create_slowing_growth_story(pattern_result, metric_id, metric, grain))
            elif pattern_result.growth_stats.current_growth_acceleration > 0:
                stories.append(self._create_accelerating_growth_story(pattern_result, metric_id, metric, grain))

        # Trend Stories
        if pattern_result.current_trend:
            trend_type = pattern_result.current_trend.trend_type
            if self._is_stable_trend(pattern_result) and trend_type == TrendType.STABLE:
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
                stories.append(
                    self._create_spike_story(pattern_result, pattern_result.trend_exception, metric_id, metric, grain)
                )
            elif pattern_result.trend_exception.type == TrendExceptionType.DROP:
                stories.append(
                    self._create_drop_story(pattern_result, pattern_result.trend_exception, metric_id, metric, grain)
                )

        # Record Value Stories
        if pattern_result.high_rank and pattern_result.high_rank.rank <= 2:  # Consider top 2 as record high
            stories.append(self._create_record_high_story(pattern_result, metric_id, metric, grain))
        elif pattern_result.low_rank and pattern_result.low_rank.rank <= 2:  # Consider bottom 2 as record low
            stories.append(self._create_record_low_story(pattern_result, metric_id, metric, grain))

        # Benchmark Stories
        if pattern_result.benchmark_comparison and pattern_result.high_rank:
            stories.append(self._create_benchmark_story(pattern_result, metric_id, metric, grain))

        return stories

    def _is_stable_trend(self, pattern_result: HistoricalPerformance) -> bool:
        """Check if the current trend is stable (has been ongoing for a while)."""
        return pattern_result.current_trend is not None and pattern_result.previous_trend is None

    def _is_new_trend(self, pattern_result: HistoricalPerformance) -> bool:
        """Check if there's a new trend (current trend exists and different from previous)."""
        return (
            pattern_result.previous_trend is not None
            and pattern_result.current_trend.trend_type != pattern_result.previous_trend.trend_type  # type: ignore
        )

    def _populate_template_context(
        self, pattern_result: HistoricalPerformance, metric: dict, grain: Granularity, include: list[str]
    ) -> dict[str, Any]:
        """
        Populate context for template rendering based on story type.

        Args:
            pattern_result: Historical performance pattern result
            metric: Metric details
            grain: Granularity of the analysis
            include: The components of the story being rendered, determines which context fields to include

        Returns:
            Template context dictionary with only the necessary fields for the given story type
        """
        grain_info = GRAIN_META.get(grain, {"label": "period", "pop": "PoP"})  # type: ignore

        # Base context always included
        context = {
            "metric": metric,
            "grain_label": grain_info["label"],
            "pop": grain_info["pop"],
        }

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

    # Growth Story Methods
    def _create_slowing_growth_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a SLOWING_GROWTH story."""
        story_group = StoryGroup.GROWTH_RATES
        story_type = StoryType.SLOWING_GROWTH

        context = self._populate_template_context(pattern_result, metric, grain, ["growth_stats"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # First prepare the series data with growth rates story-specific customizations
        series_df = self._prepare_story_series_data(pattern_result, story_type)
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

        context = self._populate_template_context(pattern_result, metric, grain, ["growth_stats"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # First prepare the series data with growth rates story-specific customizations
        series_df = self._prepare_story_series_data(pattern_result, story_type)
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

    # Trend Story Methods
    def _create_stable_trend_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a STABLE_TREND story."""
        story_group = StoryGroup.LONG_RANGE
        context = self._populate_template_context(pattern_result, metric, grain, ["current_trend", "previous_trend"])

        title = render_story_text(StoryType.STABLE_TREND, "title", context)
        detail = render_story_text(StoryType.STABLE_TREND, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.STABLE_TREND,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_new_upward_trend_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a NEW_UPWARD_TREND story."""
        story_group = StoryGroup.TREND_CHANGES
        context = self._populate_template_context(pattern_result, metric, grain, ["current_trend", "previous_trend"])

        title = render_story_text(StoryType.NEW_UPWARD_TREND, "title", context)
        detail = render_story_text(StoryType.NEW_UPWARD_TREND, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.NEW_UPWARD_TREND,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_new_downward_trend_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a NEW_DOWNWARD_TREND story."""
        story_group = StoryGroup.TREND_CHANGES
        context = self._populate_template_context(pattern_result, metric, grain, ["current_trend", "previous_trend"])

        title = render_story_text(StoryType.NEW_DOWNWARD_TREND, "title", context)
        detail = render_story_text(StoryType.NEW_DOWNWARD_TREND, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.NEW_DOWNWARD_TREND,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_performance_plateau_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a PERFORMANCE_PLATEAU story."""
        story_group = StoryGroup.TREND_CHANGES
        context = self._populate_template_context(pattern_result, metric, grain, ["current_trend", "previous_trend"])

        title = render_story_text(StoryType.PERFORMANCE_PLATEAU, "title", context)
        detail = render_story_text(StoryType.PERFORMANCE_PLATEAU, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.PERFORMANCE_PLATEAU,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    # Trend Exception Story Methods
    def _create_spike_story(
        self,
        pattern_result: HistoricalPerformance,
        trend_exception: Any,
        metric_id: str,
        metric: dict,
        grain: Granularity,
    ) -> dict[str, Any]:
        """Create a SPIKE story."""
        story_group = StoryGroup.TREND_EXCEPTIONS
        context = self._populate_template_context(pattern_result, metric, grain, ["trend_exception"])

        title = render_story_text(StoryType.SPIKE, "title", context)
        detail = render_story_text(StoryType.SPIKE, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.SPIKE,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_drop_story(
        self,
        pattern_result: HistoricalPerformance,
        trend_exception: Any,
        metric_id: str,
        metric: dict,
        grain: Granularity,
    ) -> dict[str, Any]:
        """Create a DROP story."""
        story_group = StoryGroup.TREND_EXCEPTIONS
        context = self._populate_template_context(pattern_result, metric, grain, ["trend_exception"])

        title = render_story_text(StoryType.DROP, "title", context)
        detail = render_story_text(StoryType.DROP, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.DROP,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    # Performance Change Story Methods
    def _create_improving_performance_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create an IMPROVING_PERFORMANCE story."""
        story_group = StoryGroup.TREND_CHANGES
        story_type = StoryType.IMPROVING_PERFORMANCE

        context = self._populate_template_context(pattern_result, metric, grain, ["current_trend"])

        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # First prepare the series data with growth rates story-specific customizations
        series_df = self._prepare_story_series_data(pattern_result, story_type)
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
        story_group = StoryGroup.TREND_CHANGES
        context = self._populate_template_context(pattern_result, metric, grain, ["current_trend"])

        title = render_story_text(StoryType.WORSENING_PERFORMANCE, "title", context)
        detail = render_story_text(StoryType.WORSENING_PERFORMANCE, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.WORSENING_PERFORMANCE,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    # Record Value Methods
    def _create_record_high_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a RECORD_HIGH story."""

        story_group = StoryGroup.RECORD_VALUES
        story_type = StoryType.RECORD_HIGH
        context = self._populate_template_context(pattern_result, metric, grain, ["high_rank"])

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
        context = self._populate_template_context(pattern_result, metric, grain, ["low_rank"])

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

    # Benchmark Methods
    def _create_benchmark_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a BENCHMARKS story."""
        story_group = StoryGroup.BENCHMARK_COMPARISONS
        context = self._populate_template_context(pattern_result, metric, grain, ["high_rank", "benchmark_comparison"])

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
