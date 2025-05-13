"""
Story evaluator for the performance status pattern.
"""

import logging
from typing import Any

from commons.utilities.grain_utils import GRAIN_META
from levers.models.common import Granularity
from levers.models.patterns.performance_status import MetricGVAStatus, MetricPerformance
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator import StoryEvaluatorBase, render_story_text

logger = logging.getLogger(__name__)


class PerformanceStatusEvaluator(StoryEvaluatorBase[MetricPerformance]):
    """
    Evaluates performance status pattern results and generates stories.

    Stories types generated:
    - ON_TRACK: Metric is meeting or exceeding its target
    - OFF_TRACK: Metric is falling short of its target
    - IMPROVING_STATUS: Metric status has improved
    - WORSENING_STATUS: Metric status has worsened
    - HOLD_STEADY: Metric is at or above target and needs to maintain
    """

    pattern_name = "performance_status"

    async def evaluate(self, pattern_result: MetricPerformance, metric: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Evaluate the performance status pattern result and generate stories.

        Args:
            pattern_result: Performance status pattern result
            metric: Metric details

        Returns:
            List of story dictionaries
        """
        stories = []
        metric_id = pattern_result.metric_id
        grain = pattern_result.analysis_window.grain

        # Check the current status
        current_status = pattern_result.status

        # Add status story (on track or off track)
        if current_status == MetricGVAStatus.ON_TRACK:
            stories.append(self._create_on_track_story(pattern_result, metric_id, metric, grain))
        elif current_status == MetricGVAStatus.OFF_TRACK:
            stories.append(self._create_off_track_story(pattern_result, metric_id, metric, grain))

        # Check for status change
        if pattern_result.status_change and pattern_result.status_change.has_flipped:
            if current_status == MetricGVAStatus.ON_TRACK:
                # Status improved
                stories.append(self._create_improving_status_story(pattern_result, metric_id, metric, grain))
            elif current_status == MetricGVAStatus.OFF_TRACK:
                # Status worsened
                stories.append(self._create_worsening_status_story(pattern_result, metric_id, metric, grain))

        # Check for hold steady
        if pattern_result.hold_steady and pattern_result.hold_steady.is_currently_at_or_above_target:
            stories.append(self._create_hold_steady_story(pattern_result, metric_id, metric, grain))

        return stories

    def _populate_template_context(
        self, pattern_result: MetricPerformance, metric: dict, grain: Granularity, required_components: list[str]
    ) -> dict[str, Any]:
        """
        Populate context for template rendering.

        Args:
            pattern_result: Performance status pattern result
            metric: Metric details
            grain: Granularity of the analysis
            required_components: The components of the story being rendered, determines which context fields to include
        Returns:
            Template context dictionary
        """
        grain_info = GRAIN_META.get(grain, {"label": "period", "pop": "PoP"})  # type: ignore

        # Determine trend direction
        trend_direction = (
            "up" if pattern_result.pop_change_percent and pattern_result.pop_change_percent > 0 else "down"
        )

        # Determine gap trend (for off track)
        gap_trend = (
            "is narrowing"
            if pattern_result.prior_value is not None
            and pattern_result.target_value is not None
            and abs(pattern_result.current_value - pattern_result.target_value)
            < abs(pattern_result.prior_value - pattern_result.target_value)
            else "is widening"
        )

        # Determine performance trend (for on track)
        performance_trend = (
            "improving" if pattern_result.pop_change_percent and pattern_result.pop_change_percent > 0 else "declining"
        )

        context = {
            "metric": metric,
            "current_value": pattern_result.current_value,
            "target_value": pattern_result.target_value,
            "performance_percent": abs(pattern_result.percent_over_performance or 0),
            "gap_percent": abs(pattern_result.percent_gap or 0),
            "change_percent": abs(pattern_result.pop_change_percent or 0),
            "pop": grain_info["pop"],
            "grain_label": grain_info["label"],
            "streak_length": pattern_result.streak.length if pattern_result.streak else 0,
            "trend_direction": trend_direction,
            "gap_trend": gap_trend,
            "performance_trend": performance_trend,
        }

        # Add status change specifics
        if "status_change" in required_components and pattern_result.status_change:
            context["old_status_duration"] = pattern_result.status_change.old_status_duration_grains or 0

        # Add hold steady specifics
        if "hold_steady" in required_components and pattern_result.hold_steady:
            context["current_margin"] = pattern_result.hold_steady.current_margin_percent or 0
            context["time_to_maintain"] = pattern_result.hold_steady.time_to_maintain_grains or 0

        return context

    def _create_on_track_story(
        self, pattern_result: MetricPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create an ON_TRACK story.

        Args:
            pattern_result: Performance status pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group for this story type
        story_group = StoryGroup.GOAL_VS_ACTUAL
        story_type = StoryType.ON_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, [])

        # Render title and detail from templates
        title = render_story_text(StoryType.ON_TRACK, "title", context)
        detail = render_story_text(StoryType.ON_TRACK, "detail", context)

        # Get story series data
        series_data = self.export_dataframe_as_story_series(
            self.series_df,
            story_type,
            story_group,
            grain,  # type: ignore
        )

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
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

    def _create_off_track_story(
        self, pattern_result: MetricPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create an OFF_TRACK story.

        Args:
            pattern_result: Performance status pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group for this story type
        story_group = StoryGroup.GOAL_VS_ACTUAL
        story_type = StoryType.OFF_TRACK
        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, [])

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Get story series data
        series_data = self.export_dataframe_as_story_series(
            self.series_df,
            story_type,
            story_group,
            grain,  # type: ignore
        )

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
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

    def _create_improving_status_story(
        self, pattern_result: MetricPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create an IMPROVING_STATUS story.

        Args:
            pattern_result: Performance status pattern result
            metric_id: Metric ID
            metric_label: Metric label
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group for this story type
        story_group = StoryGroup.STATUS_CHANGE
        story_type = StoryType.IMPROVING_STATUS

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, ["status_change"])

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Get story series data
        series_data = self.export_dataframe_as_story_series(
            self.series_df,
            story_type,
            story_group,
            grain,  # type: ignore
        )

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
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

    def _create_worsening_status_story(
        self, pattern_result: MetricPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a WORSENING_STATUS story.

        Args:
            pattern_result: Performance status pattern result
            metric_id: Metric ID
            metric_label: Metric label
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group for this story type
        story_group = StoryGroup.STATUS_CHANGE
        story_type = StoryType.WORSENING_STATUS

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, ["status_change"])

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Get story series data
        series_data = self.export_dataframe_as_story_series(
            self.series_df,
            story_type,
            story_group,
            grain,  # type: ignore
        )

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
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

    def _create_hold_steady_story(
        self, pattern_result: MetricPerformance, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a HOLD_STEADY story.

        Args:
            pattern_result: Performance status pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group for this story type
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.HOLD_STEADY
        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, ["hold_steady"])

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Get story series data
        series_data = self.export_dataframe_as_story_series(
            self.series_df,
            story_type,
            story_group,
            grain,  # type: ignore
        )

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
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
