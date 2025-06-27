"""
Story evaluator for the forecasting pattern.
"""

import logging
from typing import Any

from commons.models.enums import Granularity
from levers.models.enums import MetricGVAStatus, PeriodType
from levers.models.patterns.forecasting import Forecasting
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator import StoryEvaluatorBase, render_story_text

logger = logging.getLogger(__name__)


class ForecastingEvaluator(StoryEvaluatorBase[Forecasting]):
    """
    Evaluates forecasting pattern results and generates stories.

    Stories types generated:
    - FORECASTED_ON_TRACK: Forecast shows metric will beat target
    - FORECASTED_OFF_TRACK: Forecast shows metric will miss target
    - PACING_ON_TRACK: Current pacing indicates metric will beat target
    - PACING_OFF_TRACK: Current pacing indicates metric will miss target
    - REQUIRED_PERFORMANCE: Shows required growth to meet target
    """

    pattern_name = "forecasting"

    async def evaluate(self, pattern_result: Forecasting, metric: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Evaluate the forecasting pattern result and generate stories.

        Args:
            pattern_result: Forecasting pattern result
            metric: Metric details

        Returns:
            List of story dictionaries
        """
        stories = []
        metric_id = pattern_result.metric_id
        grain = Granularity(pattern_result.analysis_window.grain)

        # Generate forecast vs target stories
        if pattern_result.forecast_vs_target_stats:
            forecast_stats = pattern_result.forecast_vs_target_stats
            if forecast_stats.forecast_status == MetricGVAStatus.ON_TRACK:
                stories.append(self._create_forecasted_on_track_story(pattern_result, metric_id, metric, grain))
            elif forecast_stats.forecast_status == MetricGVAStatus.OFF_TRACK:
                stories.append(self._create_forecasted_off_track_story(pattern_result, metric_id, metric, grain))

        # Generate pacing stories
        if pattern_result.pacing:
            pacing = pattern_result.pacing
            if pacing.status and pacing.projected_value is not None:
                if pacing.status == "on_track":
                    stories.append(self._create_pacing_on_track_story(pattern_result, metric_id, metric, grain))
                elif pacing.status == "off_track":
                    stories.append(self._create_pacing_off_track_story(pattern_result, metric_id, metric, grain))

        # Generate required performance story
        if pattern_result.required_performance:
            required_perf = pattern_result.required_performance
            if (
                required_perf.required_pop_growth_percent is not None
                and required_perf.remaining_periods_count is not None
                and required_perf.remaining_periods_count > 0
            ):
                stories.append(self._create_required_performance_story(pattern_result, metric_id, metric, grain))

        return stories

    def _populate_template_context(
        self, pattern_result: Forecasting, metric: dict, grain: Granularity, **kwargs
    ) -> dict[str, Any]:
        """
        Populate context for template rendering.

        Args:
            pattern_result: Forecasting pattern result
            metric: Metric details
            grain: Granularity of the analysis
            period_type: Type of period for the forecast
            **kwargs: Additional keyword arguments

        Returns:
            Template context dictionary
        """
        period_type = pattern_result.period_type

        context = self.prepare_base_context(metric, grain)

        # Add period type label
        period_labels = {
            PeriodType.END_OF_WEEK: "week",
            PeriodType.END_OF_MONTH: "month",
            PeriodType.END_OF_QUARTER: "quarter",
            PeriodType.END_OF_YEAR: "year",
        }
        context["period_type"] = period_labels.get(period_type, "period")

        forecast_stats = pattern_result.forecast_vs_target_stats
        if forecast_stats:
            context.update(
                {
                    "forecasted_value": forecast_stats.forecasted_value,
                    "target_value": forecast_stats.target_value,
                    "gap_percent": abs(forecast_stats.forecasted_gap_percent or 0),
                }
            )

        pacing = pattern_result.pacing
        if pacing and forecast_stats:
            context.update(
                {
                    "percent_elapsed": pacing.percent_of_period_elapsed or 0,
                    "projected_value": pacing.projected_value,
                    "target_value": forecast_stats.target_value,
                    "gap_percent": abs(pacing.gap_percent or 0),
                }
            )

        required_perf = pattern_result.required_performance
        if required_perf and forecast_stats:
            # Determine trend direction
            delta = required_perf.delta_from_historical_growth or 0
            trend_direction = "increase" if delta > 0 else "decrease"

            context.update(
                {
                    "required_growth": abs(required_perf.required_pop_growth_percent or 0),
                    "remaining_periods": required_perf.remaining_periods_count,
                    "target_value": forecast_stats.target_value,
                    "delta_growth": abs(delta),
                    "trend_direction": trend_direction,
                    "past_growth": abs(required_perf.past_pop_growth_percent or 0),
                    "past_periods": 4,  # Default from the pattern configuration
                }
            )

        return context

    def _create_forecasted_on_track_story(
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a FORECASTED_ON_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.FORECASTED_ON_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,
            **context,
        )

    def _create_forecasted_off_track_story(
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a FORECASTED_OFF_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.FORECASTED_OFF_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,
            **context,
        )

    def _create_pacing_on_track_story(
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a PACING_ON_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.PACING_ON_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,
            **context,
        )

    def _create_pacing_off_track_story(
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a PACING_OFF_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.PACING_OFF_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,
            **context,
        )

    def _create_required_performance_story(
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a REQUIRED_PERFORMANCE story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.REQUIRED_PERFORMANCE

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,
            **context,
        )
