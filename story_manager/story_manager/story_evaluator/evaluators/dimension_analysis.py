"""
Story evaluator for the dimension analysis pattern.
"""

import logging
from typing import Any

from levers.models.common import Granularity
from levers.models.patterns.dimension_analysis import DimensionAnalysis
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator import StoryEvaluatorBase, render_story_text
from story_manager.story_evaluator.utils import format_date_column, format_segment_names

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
            stories.append(self._create_segment_comparison_story(pattern_result, metric_id, metric, grain))

        # Check for new strongest segment
        if pattern_result.strongest_slice:
            stories.append(self._create_new_strongest_segment_story(pattern_result, metric_id, metric, grain))

        # Check for new weakest segment
        if pattern_result.weakest_slice:
            stories.append(self._create_new_weakest_segment_story(pattern_result, metric_id, metric, grain))

        # Check for largest slice by share
        if pattern_result.largest_slice:
            stories.append(self._create_largest_segment_story(pattern_result, metric_id, metric, grain))

        # Check for smallest slice by share
        if pattern_result.smallest_slice:
            stories.append(self._create_smallest_segment_story(pattern_result, metric_id, metric, grain))

        return stories

    def _populate_template_context(
        self, pattern_result: DimensionAnalysis, metric: dict, grain: Granularity, include: list[str]
    ) -> dict[str, Any]:
        """
        Populate common context for template rendering.

        Args:
            pattern_result: Dimension analysis pattern result
            metric: Metric details
            grain: Granularity of the analysis
            include: List of sections to include in the context

        Returns:
            Template context dictionary
        """
        context = self.prepare_base_context(metric, grain)

        context.update(
            {
                "dimension_name": pattern_result.dimension_name,
            }
        )

        # TODO: check if we can merge the top and bottom segments lo
        if "top_slices" in include and "slices" in include:
            # Get top 4 segments and their names
            top_segments = pattern_result.top_slices[:4]
            top_segment_names = [s.slice_value for s in top_segments]
            formatted_names = format_segment_names(top_segment_names)

            # Create a lookup dict for quick access
            slice_lookup = {s.slice_value: s for s in pattern_result.slices}

            # Compute metrics
            slices = [slice_lookup.get(name) for name in top_segment_names if slice_lookup.get(name)]
            diffs = [abs(s.absolute_diff_percent_from_avg or 0) for s in slices]  # type: ignore
            min_diff_percent = min(diffs, default=0)
            max_diff_percent = max(diffs, default=0)
            total_share_percent = sum(s.current_share_of_volume_percent or 0 for s in slices)  # type: ignore
            streak_length = max((s.consecutive_above_avg_streak or 0 for s in slices), default=0)  # type: ignore
            context.update(
                {
                    "top_segments": formatted_names,
                    "min_diff_percent": min_diff_percent,
                    "max_diff_percent": max_diff_percent,
                    "total_share_percent": total_share_percent,
                    "streak_length": streak_length,
                }
            )
        if "bottom_slices" in include and "slices" in include:
            # Get bottom 4 segments and their names
            bottom_segments = pattern_result.bottom_slices[:4]
            bottom_segment_names = [s.slice_value for s in bottom_segments]
            formatted_names = format_segment_names(bottom_segment_names)

            # Create a lookup dict for quick access
            slice_lookup = {s.slice_value: s for s in pattern_result.slices}
            slices = [slice_lookup.get(name) for name in bottom_segment_names if slice_lookup.get(name)]

            # Compute metrics
            diffs = [abs(s.absolute_diff_percent_from_avg or 0) for s in slices]  # type: ignore
            min_diff_percent = min(diffs, default=0)
            max_diff_percent = max(diffs, default=0)
            total_share_percent = sum(s.current_share_of_volume_percent or 0 for s in slices)  # type: ignore

            # Longest negative (under performance) streak
            streak_length = max(
                (
                    abs(s.consecutive_above_avg_streak)  # type: ignore
                    for s in slices
                    if s.consecutive_above_avg_streak and s.consecutive_above_avg_streak < 0  # type: ignore
                ),
                default=0,
            )
            context.update(
                {
                    "bottom_segments": formatted_names,
                    "min_diff_percent": min_diff_percent,
                    "max_diff_percent": max_diff_percent,
                    "total_share_percent": total_share_percent,
                    "streak_length": streak_length,
                }
            )
        if "comparison_highlights" in include:
            comparison = max(pattern_result.comparison_highlights, key=lambda x: abs(x.performance_gap_percent or 0))

            # Determine gap trend
            gap_trend = "widened"
            if comparison.gap_change_percent is not None and comparison.performance_gap_percent is not None:
                if abs(comparison.performance_gap_percent) < abs(comparison.gap_change_percent):
                    gap_trend = "narrowed"

            # Calculate the gap change percentage
            gap_change_percent = 0.0
            if comparison.gap_change_percent is not None and comparison.performance_gap_percent is not None:
                gap_change_percent = abs(comparison.performance_gap_percent - comparison.gap_change_percent)

            context.update(
                {
                    "segment_a": comparison.slice_a,
                    "segment_b": comparison.slice_b,
                    "performance_diff_percent": abs(comparison.performance_gap_percent or 0),
                    "gap_trend": gap_trend,
                    "gap_change_percent": gap_change_percent,
                }
            )
        if "strongest_slice" in include:
            # Get new strongest segment info
            strongest = pattern_result.strongest_slice

            # Determine trend direction for previous segment
            trend_direction = "up"
            if strongest.current_value < strongest.prior_value:  # type: ignore
                trend_direction = "down"

            # Calculate change percentage
            change_percent = 0
            if strongest.prior_value != 0:  # type: ignore
                change_percent = abs(
                    (strongest.current_value - strongest.prior_value) / strongest.prior_value * 100  # type: ignore
                )  # type: ignore

            # Find average value across all segments
            avg_value = 0
            for slice_perf in pattern_result.slices:
                avg_value += slice_perf.current_value  # type: ignore
            if len(pattern_result.slices) > 0:
                avg_value /= float(len(pattern_result.slices))  # type: ignore

            # Calculate difference from average
            diff_from_avg_percent = 0
            if avg_value != 0:
                diff_from_avg_percent = (strongest.current_value - avg_value) / avg_value * 100  # type: ignore

            context.update(
                {
                    "segment_name": strongest.slice_value,  # type: ignore
                    "current_value": strongest.current_value,  # type: ignore
                    "previous_segment": strongest.previous_slice_value,  # type: ignore
                    "previous_value": strongest.prior_value,  # type: ignore
                    "trend_direction": trend_direction,
                    "change_percent": change_percent,
                    "avg_value": avg_value,
                    "diff_from_avg_percent": diff_from_avg_percent,
                }
            )
        if "weakest_slice" in include:
            # Get new weakest segment info
            weakest = pattern_result.weakest_slice

            # Determine trend direction for previous segment
            trend_direction = "up"
            if weakest.current_value < weakest.prior_value:  # type: ignore
                trend_direction = "down"

            # Calculate change percentage
            change_percent = 0
            if weakest.prior_value != 0:  # type: ignore
                change_percent = abs(
                    (weakest.current_value - weakest.prior_value) / weakest.prior_value * 100  # type: ignore
                )  # type: ignore

            # Find average value across all segments
            avg_value = 0
            for slice_perf in pattern_result.slices:
                avg_value += slice_perf.current_value  # type: ignore
            if len(pattern_result.slices) > 0:
                avg_value /= float(len(pattern_result.slices))  # type: ignore

            # Calculate difference from average (negative for weakest)
            diff_from_avg_percent = 0
            if avg_value != 0:
                diff_from_avg_percent = (weakest.current_value - avg_value) / avg_value * 100  # type: ignore

            context.update(
                {
                    "segment_name": weakest.slice_value,  # type: ignore
                    "current_value": weakest.current_value,  # type: ignore
                    "previous_segment": weakest.previous_slice_value,  # type: ignore
                    "previous_value": weakest.prior_value,  # type: ignore
                    "trend_direction": trend_direction,
                    "change_percent": change_percent,
                    "avg_value": avg_value,
                    "diff_from_avg_percent": abs(diff_from_avg_percent),
                }
            )
        if "largest_slice" in include:
            # Get largest segment info
            largest = pattern_result.largest_slice

            # Find current and prior share percentages
            current_share_percent = largest.current_share_of_volume_percent or 0  # type: ignore
            prior_share_percent = 0

            # Find previous segment's current share percent
            previous_share_percent = 0

            # Find slice performance data for this segment to get prior share
            for slice_perf in pattern_result.slices:
                if slice_perf.slice_value == largest.slice_value:  # type: ignore
                    prior_share_percent = slice_perf.prior_share_of_volume_percent or 0  # type: ignore
                    break

            # Find previous segment's share
            if largest.previous_slice_value:  # type: ignore
                for slice_perf in pattern_result.slices:
                    if slice_perf.slice_value == largest.previous_slice_value:  # type: ignore
                        previous_share_percent = slice_perf.current_share_of_volume_percent or 0  # type: ignore
                        break

            context.update(
                {
                    "segment_name": largest.slice_value,  # type: ignore
                    "current_share_percent": current_share_percent,
                    "prior_share_percent": prior_share_percent,
                    "previous_segment": largest.previous_slice_value,  # type: ignore
                    "previous_share_percent": previous_share_percent,
                }
            )
        if "smallest_slice" in include:
            # Get smallest segment info
            smallest = pattern_result.smallest_slice

            # Find current and prior share percentages
            current_share_percent = smallest.current_share_of_volume_percent or 0  # type: ignore
            prior_share_percent = 0

            # Find previous segment's current share percent and prior share percent
            previous_share_percent = 0
            previous_prior_share_percent = 0

            # Find slice performance data for this segment to get prior share
            for slice_perf in pattern_result.slices:
                if slice_perf.slice_value == smallest.slice_value:  # type: ignore
                    prior_share_percent = slice_perf.prior_share_of_volume_percent or 0  # type: ignore
                    break

            # Find previous segment's shares
            if smallest.previous_slice_value:  # type: ignore
                for slice_perf in pattern_result.slices:
                    if slice_perf.slice_value == smallest.previous_slice_value:  # type: ignore
                        previous_share_percent = slice_perf.current_share_of_volume_percent or 0  # type: ignore
                        previous_prior_share_percent = slice_perf.prior_share_of_volume_percent or 0  # type: ignore
                        break

            context.update(
                {
                    "segment_name": smallest.slice_value,  # type: ignore
                    "current_share_percent": current_share_percent,
                    "prior_share_percent": prior_share_percent,
                    "previous_segment": smallest.previous_slice_value,  # type: ignore
                    "previous_share_percent": previous_share_percent,
                    "previous_prior_share_percent": previous_prior_share_percent,
                }
            )

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
        story_type = StoryType.TOP_4_SEGMENTS

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, ["top_slices", "slices"])

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        series_data = self._prepare_top_bottom_segment_series_data(pattern_result, story_type)

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
        story_type = StoryType.BOTTOM_4_SEGMENTS

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, ["bottom_slices", "slices"])

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        series_data = self._prepare_top_bottom_segment_series_data(pattern_result, story_type)

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

    def _create_segment_comparison_story(
        self, pattern_result: DimensionAnalysis, metric_id: str, metric: dict, grain: Granularity
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
        story_type = StoryType.SEGMENT_COMPARISONS

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, ["comparison_highlights"])

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        series_data = self._prepare_comparison_series_data(pattern_result)

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
        story_type = StoryType.NEW_STRONGEST_SEGMENT
        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, ["strongest_slice"])

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        series_data = self._prepare_strongest_weakest_series_data(pattern_result, story_type)

        # Prepare the story model
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
        story_type = StoryType.NEW_WEAKEST_SEGMENT

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, ["weakest_slice"])

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        series_data = self._prepare_strongest_weakest_series_data(pattern_result, story_type)

        # Prepare the story model
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
        story_type = StoryType.NEW_LARGEST_SEGMENT

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, ["largest_slice"])

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        series_data = self._prepare_largest_smallest_series_data(pattern_result, story_type)

        # Prepare the story model
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
        story_type = StoryType.NEW_SMALLEST_SEGMENT

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain, ["smallest_slice"])

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        series_data = self._prepare_largest_smallest_series_data(pattern_result, story_type)

        # Prepare the story model
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

    def _prepare_comparison_series_data(self, pattern_result: DimensionAnalysis) -> list[dict[str, Any]]:
        """
        Prepare series data for comparison stories.

        Args:
            pattern_result: Dimension analysis pattern result

        Returns:
            List containing dictionary with series data separated by segments
        """
        # Find the comparison with the largest performance gap
        comparison = max(pattern_result.comparison_highlights, key=lambda x: abs(x.performance_gap_percent or 0))

        segments = {"segment_a": comparison.slice_a, "segment_b": comparison.slice_b}

        result: dict = {key: [] for key in segments}

        if self.series_df is None or self.series_df.empty:
            return [result]  # Return list with empty result dictionary

        series_df = self.series_df.copy()

        # Ensure date column is present and properly formatted
        if "date" in series_df.columns:
            series_df = format_date_column(series_df)

        # Filter for both segments
        filtered_df = series_df[series_df["dimension_slice"].isin(segments.values())]
        if filtered_df.empty:
            return [result]  # Return list with empty result dictionary

        # Loop through each segment and populate results
        for key, segment in segments.items():
            segment_df = filtered_df[filtered_df["dimension_slice"] == segment]
            if not segment_df.empty:
                result[key] = segment_df[["date", "dimension_slice", "value"]].to_dict(orient="records")

        return [result]

    def _prepare_strongest_weakest_series_data(
        self, pattern_result: DimensionAnalysis, story_type: StoryType
    ) -> list[dict[str, Any]]:
        """
        Prepare series data for strongest / weakest stories.

        Returns:
            Dictionary with 'current', 'prior', and 'average' series data.
        """
        slice_obj = (
            pattern_result.strongest_slice
            if story_type == StoryType.NEW_STRONGEST_SEGMENT
            else pattern_result.weakest_slice
        )

        current_slice = slice_obj.slice_value  # type: ignore
        previous_slice = slice_obj.previous_slice_value  # type: ignore

        result: dict = {"current": [], "prior": [], "average": []}

        if self.series_df is None or self.series_df.empty:
            return [result]

        series_df = self.series_df.copy()
        # Ensure date column is present and properly formatted
        if "date" in series_df.columns:
            series_df = format_date_column(series_df)

        # Filter for current and previous segments
        filtered_df = series_df[series_df["dimension_slice"].isin([current_slice, previous_slice])]

        # Group to compute average value per date across all slices
        avg_df = series_df.groupby("date", as_index=False)["value"].mean().rename(columns={"value": "value"})  # type: ignore

        # Build segment-wise data
        for key, segment in {"current": current_slice, "prior": previous_slice}.items():
            seg_df = filtered_df[filtered_df["dimension_slice"] == segment]
            if not seg_df.empty:
                result[key] = seg_df.rename(columns={"dimension_slice": "segment"})[
                    ["date", "segment", "value"]
                ].to_dict(orient="records")

        # Build average data
        result["average"] = avg_df.to_dict(orient="records")

        return [result]

    def _prepare_top_bottom_segment_series_data(
        self, pattern_result: DimensionAnalysis, story_type: StoryType, limit: int = 4
    ) -> list[dict[str, Any]]:
        """
        Create a simple list of segment data from raw segment objects.

        Args:
            pattern_result: Dimension analysis pattern result
            story_type: Type of story being created
            limit: Maximum number of segments to include

        Returns:
            List of dictionaries with segment name and value
        """

        slices = (
            pattern_result.top_slices[:limit]
            if story_type == StoryType.TOP_4_SEGMENTS
            else pattern_result.bottom_slices[:limit]
        )

        # Calculate average value from all slices
        avg_value = 0
        if pattern_result.slices and len(pattern_result.slices) > 0:
            total = sum(s.current_value for s in pattern_result.slices)
            avg_value = total / len(pattern_result.slices)  # type: ignore

        # Create segment entries
        segments = [{"segment": slice_obj.slice_value, "value": slice_obj.metric_value} for slice_obj in slices]

        # Add average as an additional entry
        avg_entry = {"segment": "Average", "value": avg_value}

        if story_type == StoryType.TOP_4_SEGMENTS:
            segments.append(avg_entry)  # Add average at the end for top segments
        else:
            segments.insert(0, avg_entry)  # Insert average at the beginning for bottom segments

        return segments

    def _prepare_largest_smallest_series_data(
        self, pattern_result: DimensionAnalysis, story_type: StoryType
    ) -> list[dict[str, Any]]:
        """
        Prepare series data for segment comparison stories.

        Returns:
            Dictionary with 'current', 'prior', and 'average' series data.
        """
        slice_obj = (
            pattern_result.largest_slice
            if story_type == StoryType.NEW_LARGEST_SEGMENT
            else pattern_result.smallest_slice
        )

        current_slice = slice_obj.slice_value  # type: ignore
        previous_slice = slice_obj.previous_slice_value  # type: ignore

        result: dict = {
            "current": [],
            "prior": [],
        }

        if self.series_df is None or self.series_df.empty:
            return [result]

        series_df = self.series_df.copy()

        # Format dates using utility method
        if "date" in series_df.columns:
            series_df = format_date_column(series_df)

        # Filter for current and previous segments
        filtered_df = series_df[series_df["dimension_slice"].isin([current_slice, previous_slice])]

        # Build segment-wise data
        for key, segment in {"current": current_slice, "prior": previous_slice}.items():
            seg_df = filtered_df[filtered_df["dimension_slice"] == segment]
            if not seg_df.empty:
                result[key] = seg_df.rename(columns={"dimension_slice": "segment"})[
                    ["date", "segment", "value"]
                ].to_dict(orient="records")

        return [result]
