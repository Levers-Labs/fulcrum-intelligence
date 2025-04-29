"""
Story evaluator for the dimension analysis pattern.
"""

import logging
from typing import Any

from commons.utilities.grain_utils import GRAIN_META
from levers.models.common import Granularity
from levers.models.patterns.dimension_analysis import DimensionAnalysis
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator import StoryEvaluatorBase, render_story_text

logger = logging.getLogger(__name__)


class DimensionAnalysisEvaluator(StoryEvaluatorBase[DimensionAnalysis]):
    """
    Evaluates dimension analysis pattern results and generates stories.

    Stories types generated:
    - TOP_4_SEGMENTS: Top performing segments
    - BOTTOM_4_SEGMENTS: Bottom performing segments
    - SEGMENT_COMPARISONS: Comparison between two notable segments
    - NEW_STRONGEST_SEGMENT: A segment becomes the strongest performer
    - NEW_WEAKEST_SEGMENT: A segment becomes the weakest performer
    - NEW_LARGEST_SEGMENT: A segment becomes the largest by share
    - NEW_SMALLEST_SEGMENT: A segment becomes the smallest by share
    """

    pattern_name = "dimension_analysis"

    async def evaluate(self, pattern_result: DimensionAnalysis, metric: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Evaluate the dimension analysis pattern result and generate stories.

        Args:
            pattern_result: Dimension analysis pattern result
            metric: Metric details

        Returns:
            List of story dictionaries
        """
        stories = []
        metric_id = pattern_result.metric_id
        grain = pattern_result.analysis_window.grain

        # Check for top segments
        if pattern_result.top_slices and len(pattern_result.top_slices) >= 4:
            stories.append(self._create_top_segments_story(pattern_result, metric_id, metric, grain))

        # Check for bottom segments
        if pattern_result.bottom_slices and len(pattern_result.bottom_slices) >= 4:
            stories.append(self._create_bottom_segments_story(pattern_result, metric_id, metric, grain))

        # Check for notable segment comparisons in comparison highlights
        if pattern_result.comparison_highlights and len(pattern_result.comparison_highlights) > 0:
            # Take the most significant comparison based on performance difference
            most_significant = max(
                pattern_result.comparison_highlights, key=lambda x: abs(x.performance_gap_percent or 0)
            )
            stories.append(
                self._create_segment_comparison_story(most_significant, pattern_result, metric_id, metric, grain)
            )

        # Check for new strongest segment
        if pattern_result.new_strongest_slice:
            stories.append(self._create_new_strongest_segment_story(pattern_result, metric_id, metric, grain))

        # Check for new weakest segment
        if pattern_result.new_weakest_slice:
            stories.append(self._create_new_weakest_segment_story(pattern_result, metric_id, metric, grain))

        # Check for largest slice by share
        if pattern_result.largest_slice:
            stories.append(self._create_largest_segment_story(pattern_result, metric_id, metric, grain))

        # Check for smallest slice by share
        if pattern_result.smallest_slice:
            stories.append(self._create_smallest_segment_story(pattern_result, metric_id, metric, grain))

        return stories

    def _populate_template_context(
        self, pattern_result: DimensionAnalysis, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Populate common context for template rendering.

        Args:
            pattern_result: Dimension analysis pattern result
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Template context dictionary
        """
        grain_info = GRAIN_META.get(grain, {"label": "period", "pop": "PoP"})  # type: ignore

        context = {
            "metric": metric,
            "dimension_name": pattern_result.dimension_name,
            "grain_label": grain_info["label"],
            "pop": grain_info["pop"],
        }

        return context

    def _create_top_segments_story(
        self, pattern_result: DimensionAnalysis, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a TOP_4_SEGMENTS story.

        Args:
            pattern_result: Dimension analysis pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group
        story_group = StoryGroup.SIGNIFICANT_SEGMENTS

        # Get top 4 segments
        top_segments = pattern_result.top_slices[:4]

        # Format segment names
        top_segment_names = [slice.slice_value for slice in top_segments]
        formatted_names = ", ".join(top_segment_names[:-1]) + f", and {top_segment_names[-1]}"

        # Calculate performance metrics
        min_diff_percent = min(abs(slice.absolute_diff_percent_from_avg or 0) for slice in top_segments)
        max_diff_percent = max(abs(slice.absolute_diff_percent_from_avg or 0) for slice in top_segments)

        # Calculate total share percent if available in the pattern_result.slices
        total_share_percent = 0
        for segment in top_segment_names:
            for slice_perf in pattern_result.slices:
                if slice_perf.slice_value == segment:
                    if slice_perf.current_share_of_volume_percent:
                        total_share_percent += slice_perf.current_share_of_volume_percent
                    break

        # Find longest streak
        streak_length = 0
        for segment in top_segment_names:
            for slice_perf in pattern_result.slices:
                if slice_perf.slice_value == segment:
                    if (
                        slice_perf.consecutive_above_avg_streak
                        and slice_perf.consecutive_above_avg_streak > streak_length
                    ):
                        streak_length = slice_perf.consecutive_above_avg_streak
                    break

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)
        context.update(
            {
                "top_segments": formatted_names,
                "min_diff_percent": min_diff_percent,
                "max_diff_percent": max_diff_percent,
                "total_share_percent": total_share_percent,
                "streak_length": streak_length,
            }
        )

        # Render title and detail from templates
        title = render_story_text(StoryType.TOP_4_SEGMENTS, "title", context)
        detail = render_story_text(StoryType.TOP_4_SEGMENTS, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=StoryType.TOP_4_SEGMENTS,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_bottom_segments_story(
        self, pattern_result: DimensionAnalysis, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a BOTTOM_4_SEGMENTS story.

        Args:
            pattern_result: Dimension analysis pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group
        story_group = StoryGroup.SIGNIFICANT_SEGMENTS

        # Get bottom 4 segments
        bottom_segments = pattern_result.bottom_slices[:4]

        # Format segment names
        bottom_segment_names = [slice.slice_value for slice in bottom_segments]
        formatted_names = ", ".join(bottom_segment_names[:-1]) + f", and {bottom_segment_names[-1]}"

        # Calculate performance metrics
        min_diff_percent = min(abs(slice.absolute_diff_percent_from_avg or 0) for slice in bottom_segments)
        max_diff_percent = max(abs(slice.absolute_diff_percent_from_avg or 0) for slice in bottom_segments)

        # Calculate total share percent if available in the pattern_result.slices
        total_share_percent = 0
        for segment in bottom_segment_names:
            for slice_perf in pattern_result.slices:
                if slice_perf.slice_value == segment:
                    if slice_perf.current_share_of_volume_percent:
                        total_share_percent += slice_perf.current_share_of_volume_percent
                    break

        # Find longest streak of underperformance
        streak_length = 0
        for segment in bottom_segment_names:
            for slice_perf in pattern_result.slices:
                if slice_perf.slice_value == segment:
                    if slice_perf.consecutive_above_avg_streak and slice_perf.consecutive_above_avg_streak < 0:
                        # If negative streak means consecutive underperformance
                        below_avg_streak = abs(slice_perf.consecutive_above_avg_streak)
                        if below_avg_streak > streak_length:
                            streak_length = below_avg_streak
                    break

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)
        context.update(
            {
                "bottom_segments": formatted_names,
                "min_diff_percent": min_diff_percent,
                "max_diff_percent": max_diff_percent,
                "total_share_percent": total_share_percent,
                "streak_length": streak_length,
            }
        )

        # Render title and detail from templates
        title = render_story_text(StoryType.BOTTOM_4_SEGMENTS, "title", context)
        detail = render_story_text(StoryType.BOTTOM_4_SEGMENTS, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=StoryType.BOTTOM_4_SEGMENTS,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_segment_comparison_story(
        self, comparison: Any, pattern_result: DimensionAnalysis, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a SEGMENT_COMPARISONS story.

        Args:
            comparison: Slice comparison data
            pattern_result: Dimension analysis pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group
        story_group = StoryGroup.SIGNIFICANT_SEGMENTS

        # Determine gap trend
        gap_trend = "widened"
        if comparison.gap_change_percent is not None and comparison.performance_gap_percent is not None:
            if abs(comparison.performance_gap_percent) < abs(comparison.gap_change_percent):
                gap_trend = "narrowed"

        # Calculate the gap change percentage
        gap_change_percent = 0
        if comparison.gap_change_percent is not None and comparison.performance_gap_percent is not None:
            gap_change_percent = abs(comparison.performance_gap_percent - comparison.gap_change_percent)

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)
        context.update(
            {
                "segment_a": comparison.slice_a,
                "segment_b": comparison.slice_b,
                "performance_diff_percent": abs(comparison.performance_gap_percent or 0),
                "gap_trend": gap_trend,
                "gap_change_percent": gap_change_percent,
            }
        )

        # Render title and detail from templates
        title = render_story_text(StoryType.SEGMENT_COMPARISONS, "title", context)
        detail = render_story_text(StoryType.SEGMENT_COMPARISONS, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=StoryType.SEGMENT_COMPARISONS,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_new_strongest_segment_story(
        self, pattern_result: DimensionAnalysis, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a NEW_STRONGEST_SEGMENT story.

        Args:
            pattern_result: Dimension analysis pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group
        story_group = StoryGroup.SEGMENT_CHANGES

        # Get new strongest segment info
        strongest = pattern_result.new_strongest_slice

        # Determine trend direction for previous segment
        trend_direction = "up"
        if strongest.current_value < strongest.prior_value:
            trend_direction = "down"

        # Calculate change percentage
        change_percent = 0
        if strongest.prior_value != 0:
            change_percent = abs((strongest.current_value - strongest.prior_value) / strongest.prior_value * 100)

        # Find average value across all segments
        avg_value = 0
        for slice_perf in pattern_result.slices:
            avg_value += slice_perf.current_value
        if len(pattern_result.slices) > 0:
            avg_value /= float(len(pattern_result.slices))  # type: ignore

        # Calculate difference from average
        diff_from_avg_percent = 0
        if avg_value != 0:
            diff_from_avg_percent = (strongest.current_value - avg_value) / avg_value * 100

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)
        context.update(
            {
                "segment_name": strongest.slice_value,
                "current_value": strongest.current_value,
                "previous_segment": strongest.previous_slice_value,
                "previous_value": strongest.prior_value,
                "trend_direction": trend_direction,
                "change_percent": change_percent,
                "avg_value": avg_value,
                "diff_from_avg_percent": diff_from_avg_percent,
            }
        )

        # Render title and detail from templates
        title = render_story_text(StoryType.NEW_STRONGEST_SEGMENT, "title", context)
        detail = render_story_text(StoryType.NEW_STRONGEST_SEGMENT, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.NEW_STRONGEST_SEGMENT,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_new_weakest_segment_story(
        self, pattern_result: DimensionAnalysis, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a NEW_WEAKEST_SEGMENT story.

        Args:
            pattern_result: Dimension analysis pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group
        story_group = StoryGroup.SEGMENT_CHANGES

        # Get new weakest segment info
        weakest = pattern_result.new_weakest_slice

        # Determine trend direction for previous segment
        trend_direction = "up"
        if weakest.current_value < weakest.prior_value:
            trend_direction = "down"

        # Calculate change percentage
        change_percent = 0
        if weakest.prior_value != 0:
            change_percent = abs((weakest.current_value - weakest.prior_value) / weakest.prior_value * 100)

        # Find average value across all segments
        avg_value = 0
        for slice_perf in pattern_result.slices:
            avg_value += slice_perf.current_value
        if len(pattern_result.slices) > 0:
            avg_value /= float(len(pattern_result.slices))  # type: ignore

        # Calculate difference from average (negative for weakest)
        diff_from_avg_percent = 0
        if avg_value != 0:
            diff_from_avg_percent = (weakest.current_value - avg_value) / avg_value * 100

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)
        context.update(
            {
                "segment_name": weakest.slice_value,
                "current_value": weakest.current_value,
                "previous_segment": weakest.previous_slice_value,
                "previous_value": weakest.prior_value,
                "trend_direction": trend_direction,
                "change_percent": change_percent,
                "avg_value": avg_value,
                "diff_from_avg_percent": abs(
                    diff_from_avg_percent
                ),  # Using absolute value here, template uses "lower than"
            }
        )

        # Render title and detail from templates
        title = render_story_text(StoryType.NEW_WEAKEST_SEGMENT, "title", context)
        detail = render_story_text(StoryType.NEW_WEAKEST_SEGMENT, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.NEW_WEAKEST_SEGMENT,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_largest_segment_story(
        self, pattern_result: DimensionAnalysis, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a NEW_LARGEST_SEGMENT story.

        Args:
            pattern_result: Dimension analysis pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group
        story_group = StoryGroup.SEGMENT_CHANGES

        # Get largest segment info
        largest = pattern_result.largest_slice

        # Find current and prior share percentages
        current_share_percent = largest.current_share_of_volume_percent or 0
        prior_share_percent = 0

        # Find previous segment's current share percent
        previous_share_percent = 0

        # Find slice performance data for this segment to get prior share
        for slice_perf in pattern_result.slices:
            if slice_perf.slice_value == largest.slice_value:
                prior_share_percent = slice_perf.prior_share_of_volume_percent or 0
                break

        # Find previous segment's share
        if largest.previous_slice_value:
            for slice_perf in pattern_result.slices:
                if slice_perf.slice_value == largest.previous_slice_value:
                    previous_share_percent = slice_perf.current_share_of_volume_percent or 0
                    break

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)
        context.update(
            {
                "segment_name": largest.slice_value,
                "current_share_percent": current_share_percent,
                "prior_share_percent": prior_share_percent,
                "previous_segment": largest.previous_slice_value,
                "previous_share_percent": previous_share_percent,
            }
        )

        # Render title and detail from templates
        title = render_story_text(StoryType.NEW_LARGEST_SEGMENT, "title", context)
        detail = render_story_text(StoryType.NEW_LARGEST_SEGMENT, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=StoryType.NEW_LARGEST_SEGMENT,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )

    def _create_smallest_segment_story(
        self, pattern_result: DimensionAnalysis, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a NEW_SMALLEST_SEGMENT story.

        Args:
            pattern_result: Dimension analysis pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        # Get the story group
        story_group = StoryGroup.SEGMENT_CHANGES

        # Get smallest segment info
        smallest = pattern_result.smallest_slice

        # Find current and prior share percentages
        current_share_percent = smallest.current_share_of_volume_percent or 0
        prior_share_percent = 0

        # Find previous segment's current share percent and prior share percent
        previous_share_percent = 0
        previous_prior_share_percent = 0

        # Find slice performance data for this segment to get prior share
        for slice_perf in pattern_result.slices:
            if slice_perf.slice_value == smallest.slice_value:
                prior_share_percent = slice_perf.prior_share_of_volume_percent or 0
                break

        # Find previous segment's shares
        if smallest.previous_slice_value:
            for slice_perf in pattern_result.slices:
                if slice_perf.slice_value == smallest.previous_slice_value:
                    previous_share_percent = slice_perf.current_share_of_volume_percent or 0
                    previous_prior_share_percent = slice_perf.prior_share_of_volume_percent or 0
                    break

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)
        context.update(
            {
                "segment_name": smallest.slice_value,
                "current_share_percent": current_share_percent,
                "prior_share_percent": prior_share_percent,
                "previous_segment": smallest.previous_slice_value,
                "previous_share_percent": previous_share_percent,
                "previous_prior_share_percent": previous_prior_share_percent,
            }
        )

        # Render title and detail from templates
        title = render_story_text(StoryType.NEW_SMALLEST_SEGMENT, "title", context)
        detail = render_story_text(StoryType.NEW_SMALLEST_SEGMENT, "detail", context)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.TRENDS,
            story_type=StoryType.NEW_SMALLEST_SEGMENT,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,  # type: ignore
            **context,
        )
