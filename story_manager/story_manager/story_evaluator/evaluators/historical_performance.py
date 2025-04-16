"""
Story evaluator for the historical performance pattern.
"""

import logging
from typing import Any

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
    - Seasonal Patterns: SEASONAL_PATTERN_MATCH, SEASONAL_PATTERN_BREAK
    - Record Values: RECORD_HIGH, RECORD_LOW
    - Benchmark Comparisons: BENCHMARKS
    """

    pattern_name = "historical_performance"
    # No default genre, we'll set it per story type

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
        if self._should_create_growth_story(pattern_result):
            if (
                pattern_result.growth_stats.current_growth_acceleration
                and pattern_result.growth_stats.current_growth_acceleration < 0
            ):
                stories.append(self._create_slowing_growth_story(pattern_result, metric_id, metric, grain))
            elif (
                pattern_result.growth_stats.current_growth_acceleration
                and pattern_result.growth_stats.current_growth_acceleration > 0
            ):
                stories.append(self._create_accelerating_growth_story(pattern_result, metric_id, metric, grain))

        # Trend Stories
        if pattern_result.current_trend:
            if self._is_stable_trend(pattern_result) and pattern_result.current_trend.trend_type == TrendType.STABLE:
                stories.append(self._create_stable_trend_story(pattern_result, metric_id, metric, grain))
            elif self._is_new_trend(pattern_result) and pattern_result.current_trend.trend_type == TrendType.UPWARD:
                stories.append(self._create_new_upward_trend_story(pattern_result, metric_id, metric, grain))
            elif self._is_new_trend(pattern_result) and pattern_result.current_trend.trend_type == TrendType.DOWNWARD:
                stories.append(self._create_new_downward_trend_story(pattern_result, metric_id, metric, grain))
            elif pattern_result.current_trend.trend_type == TrendType.PLATEAU:
                stories.append(self._create_performance_plateau_story(pattern_result, metric_id, metric, grain))

        # Improving/Worsening Performance
        if self._should_create_performance_change_story(pattern_result):
            if (
                pattern_result.current_trend
                and pattern_result.current_trend.average_pop_growth
                and pattern_result.current_trend.average_pop_growth > 0
            ):
                stories.append(self._create_improving_performance_story(pattern_result, metric_id, metric, grain))
            elif (
                pattern_result.current_trend
                and pattern_result.current_trend.average_pop_growth
                and pattern_result.current_trend.average_pop_growth < 0
            ):
                stories.append(self._create_worsening_performance_story(pattern_result, metric_id, metric, grain))

        # Trend Exception Stories (Spikes & Drops)
        for trend_exception in pattern_result.trend_exceptions:
            if trend_exception.type == TrendExceptionType.SPIKE:
                stories.append(self._create_spike_story(pattern_result, trend_exception, metric_id, metric, grain))
            elif trend_exception.type == TrendExceptionType.DROP:
                stories.append(self._create_drop_story(pattern_result, trend_exception, metric_id, metric, grain))

        # Seasonal Pattern Stories
        if pattern_result.seasonality:
            if pattern_result.seasonality.is_following_expected_pattern:
                stories.append(self._create_seasonal_pattern_match_story(pattern_result, metric_id, metric, grain))
            else:
                stories.append(self._create_seasonal_pattern_break_story(pattern_result, metric_id, metric, grain))

        # Record Value Stories
        if self._should_create_record_value_story(pattern_result):
            if pattern_result.high_rank.rank <= 2:  # Consider top 2 as record high
                stories.append(self._create_record_high_story(pattern_result, metric_id, metric, grain))
            if pattern_result.low_rank.rank <= 2:  # Consider bottom 2 as record low
                stories.append(self._create_record_low_story(pattern_result, metric_id, metric, grain))

        # Benchmark Stories
        if pattern_result.benchmark_comparisons:
            stories.append(self._create_benchmark_story(pattern_result, metric_id, metric, grain))

        return stories

    def _should_create_growth_story(self, pattern_result: HistoricalPerformance) -> bool:
        """Check if we should create a growth story."""
        return (
            pattern_result.growth_stats is not None
            and pattern_result.growth_stats.current_growth_acceleration is not None
            and abs(pattern_result.growth_stats.current_growth_acceleration)
            > 5  # 5% threshold for acceleration/deceleration
        )

    def _is_stable_trend(self, pattern_result: HistoricalPerformance) -> bool:
        """Check if the current trend is stable (has been ongoing for a while)."""
        return pattern_result.current_trend is not None and pattern_result.previous_trend is None

    def _is_new_trend(self, pattern_result: HistoricalPerformance) -> bool:
        """Check if there's a new trend (current trend exists and different from previous)."""
        return (
            pattern_result.current_trend is not None
            and pattern_result.previous_trend is not None
            and pattern_result.current_trend.trend_type != pattern_result.previous_trend.trend_type
        )

    def _should_create_performance_change_story(self, pattern_result: HistoricalPerformance) -> bool:
        """Check if we should create a performance change story."""
        return (
            pattern_result.current_trend is not None
            and pattern_result.current_trend.average_pop_growth is not None
            and pattern_result.current_trend.duration_grains >= 2  # At least 2 periods to establish a trend
        )

    def _should_create_record_value_story(self, pattern_result: HistoricalPerformance) -> bool:
        """Check if we should create a record value story."""
        return (
            pattern_result.high_rank is not None
            and pattern_result.low_rank is not None
            and (pattern_result.high_rank.rank <= 2 or pattern_result.low_rank.rank <= 2)  # Top/bottom 2
        )

    def _populate_template_context(
        self, pattern_result: HistoricalPerformance, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Populate context for template rendering.

        Args:
            pattern_result: Historical performance pattern result
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Template context dictionary
        """
        grain_info = GRAIN_META.get(grain, {"label": "period", "pop": "PoP"})  # type: ignore

        context = {
            "metric": metric,
            "grain_label": grain_info["label"],
            "pop": grain_info["pop"],
        }

        # Add growth stats
        if pattern_result.growth_stats:
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
        if pattern_result.current_trend:
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
        if pattern_result.previous_trend:
            context.update(
                {
                    "prev_trend_duration": pattern_result.previous_trend.duration_grains,
                    "prev_trend_avg_growth": abs(pattern_result.previous_trend.average_pop_growth or 0),
                }
            )

        # Add rank summaries
        if pattern_result.high_rank:
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

        if pattern_result.low_rank:
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

        # Add seasonality info
        if pattern_result.seasonality:
            context.update(
                {
                    "is_seasonal_match": pattern_result.seasonality.is_following_expected_pattern,
                    "expected_change": abs(pattern_result.seasonality.expected_change_percent or 0),
                    "actual_change": abs(pattern_result.seasonality.actual_change_percent or 0),
                    "seasonal_deviation": abs(pattern_result.seasonality.deviation_percent or 0),
                    "expected_direction": (
                        "increase"
                        if pattern_result.seasonality.expected_change_percent
                        and pattern_result.seasonality.expected_change_percent > 0
                        else "decrease"
                    ),
                    "actual_direction": (
                        "increase"
                        if pattern_result.seasonality.actual_change_percent
                        and pattern_result.seasonality.actual_change_percent > 0
                        else "decrease"
                    ),
                }
            )

        # Add benchmark info
        if pattern_result.benchmark_comparisons:
            # No need to sort by priority since reference period is always "WTD"

            # Handle the case of at least one benchmark
            if len(pattern_result.benchmark_comparisons) >= 1:
                prior = pattern_result.benchmark_comparisons[0]
                # Map WTD to a more descriptive term
                context["prior_period"] = "week"
                context["prior_change_percent"] = abs(prior.change_percent or 0)
                context["prior_direction"] = "higher" if prior.change_percent and prior.change_percent > 0 else "lower"

                # Handle the case of at least two benchmarks (though currently there's only one)
                if len(pattern_result.benchmark_comparisons) >= 2:
                    older = pattern_result.benchmark_comparisons[1]
                    context["older_period"] = "month"
                    context["older_change_percent"] = abs(older.change_percent or 0)
                    context["older_direction"] = (
                        "higher" if older.change_percent and older.change_percent > 0 else "lower"
                    )
                else:
                    # Only one benchmark, provide defaults for the second
                    context["older_period"] = "month"
                    context["older_change_percent"] = 0
                    context["older_direction"] = "unchanged from"

        return context

    # Growth Story Methods
    def _create_slowing_growth_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a SLOWING_GROWTH story."""
        story_group = StoryGroup.GROWTH_RATES
        context = self._populate_template_context(pattern_result, metric, grain)

        title = render_story_text(StoryType.SLOWING_GROWTH, "title", context)
        detail = render_story_text(StoryType.SLOWING_GROWTH, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.GROWTH,
            story_type=StoryType.SLOWING_GROWTH,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_accelerating_growth_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create an ACCELERATING_GROWTH story."""
        story_group = StoryGroup.GROWTH_RATES
        context = self._populate_template_context(pattern_result, metric, grain)

        title = render_story_text(StoryType.ACCELERATING_GROWTH, "title", context)
        detail = render_story_text(StoryType.ACCELERATING_GROWTH, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.GROWTH,
            story_type=StoryType.ACCELERATING_GROWTH,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    # Trend Story Methods
    def _create_stable_trend_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a STABLE_TREND story."""
        story_group = StoryGroup.LONG_RANGE
        context = self._populate_template_context(pattern_result, metric, grain)

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
        context = self._populate_template_context(pattern_result, metric, grain)

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
        context = self._populate_template_context(pattern_result, metric, grain)

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
        context = self._populate_template_context(pattern_result, metric, grain)

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
        context = self._populate_template_context(pattern_result, metric, grain)

        # Add trend exception specific context
        context.update(
            {
                "exception_value": trend_exception.current_value,
                "normal_low": trend_exception.normal_range_low,
                "normal_high": trend_exception.normal_range_high,
                "deviation": abs(trend_exception.absolute_delta_from_normal_range or 0),
                "deviation_percent": abs(trend_exception.magnitude_percent or 0),
            }
        )

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
        context = self._populate_template_context(pattern_result, metric, grain)

        # Add trend exception specific context
        context.update(
            {
                "exception_value": trend_exception.current_value,
                "normal_low": trend_exception.normal_range_low,
                "normal_high": trend_exception.normal_range_high,
                "deviation": abs(trend_exception.absolute_delta_from_normal_range or 0),
                "deviation_percent": abs(trend_exception.magnitude_percent or 0),
            }
        )

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
        context = self._populate_template_context(pattern_result, metric, grain)

        title = render_story_text(StoryType.IMPROVING_PERFORMANCE, "title", context)
        detail = render_story_text(StoryType.IMPROVING_PERFORMANCE, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.IMPROVING_PERFORMANCE,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_worsening_performance_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a WORSENING_PERFORMANCE story."""
        story_group = StoryGroup.TREND_CHANGES
        context = self._populate_template_context(pattern_result, metric, grain)

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

    # Seasonal Pattern Methods
    def _create_seasonal_pattern_match_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a SEASONAL_PATTERN_MATCH story."""
        story_group = StoryGroup.SEASONAL_PATTERNS
        context = self._populate_template_context(pattern_result, metric, grain)

        title = render_story_text(StoryType.SEASONAL_PATTERN_MATCH, "title", context)
        detail = render_story_text(StoryType.SEASONAL_PATTERN_MATCH, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.SEASONAL_PATTERN_MATCH,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_seasonal_pattern_break_story(
        self, pattern_result: HistoricalPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """Create a SEASONAL_PATTERN_BREAK story."""
        story_group = StoryGroup.SEASONAL_PATTERNS
        context = self._populate_template_context(pattern_result, metric, grain)

        title = render_story_text(StoryType.SEASONAL_PATTERN_BREAK, "title", context)
        detail = render_story_text(StoryType.SEASONAL_PATTERN_BREAK, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.SEASONAL_PATTERN_BREAK,
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
        context = self._populate_template_context(pattern_result, metric, grain)

        title = render_story_text(StoryType.RECORD_HIGH, "title", context)
        detail = render_story_text(StoryType.RECORD_HIGH, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.RECORD_HIGH,
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
        context = self._populate_template_context(pattern_result, metric, grain)

        title = render_story_text(StoryType.RECORD_LOW, "title", context)
        detail = render_story_text(StoryType.RECORD_LOW, "detail", context)

        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.RECORD_LOW,
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
        context = self._populate_template_context(pattern_result, metric, grain)

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
