import logging

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_builder import StoryBuilderBase
from story_manager.story_builder.utils import convert_snake_case_to_label, fetch_dimensions_from_metric

logger = logging.getLogger(__name__)


class SegmentDriftStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.ROOT_CAUSES
    group = StoryGroup.SEGMENT_DRIFT
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    async def generate_stories(self, metric_id: str, grain: str) -> list[dict]:
        """
        Generating segment drift stories for a give metric
        - Each story includes details about the performance of a dimension slice, whether the value and share are
        growing, improving or shrinking, worsening.
        - We are considering top 4 stories with single dimension slice, ranking is based on sort value provided by
        dsensei.

        Input:
            The input to get_segment_drift consist of
            - metric_id : metric for which we are generating the stories.
            - evaluation_start_date: start date for current period data.
            - evaluation_end_date: end date for current period data.
            - comparison_start_date: start date for past data, which will be used for comparison.
            - comparison_end_date: end date for past data, which will be used for comparison.

        Logic:
            There are 4 types of stories possible
            - Growing Segment: If the share of the evaluation data has increased
            - Improving Segment: If the percentage change of the evaluation slice value is positive
            - Shrinking Segment: If the share of the evaluation data has decreased
            - Worsening Segment: If the percentage change of the evaluation slice value is negative

        Calculation:
            - For Growing Segment and Shrinking Segment:
                evaluation value share - comparison value share ( of each dimension slice)
            - For Improving Segment and Worsening Segment:
                (evaluation value - comparison value)/ comparison value ( of each dimension slice)

        Output:
            a list of story dictionaries.

        """
        evaluation_start_date, evaluation_end_date = self._get_input_time_range(grain)  # type:ignore
        comparison_start_date, comparison_end_date = self._get_input_time_range(
            grain,  # type:ignore
            curr_date=evaluation_start_date,
        )
        metric = await self.query_service.get_metric(metric_id)
        metric_dimensions = fetch_dimensions_from_metric(metric_details=metric)

        segment_drift = await self.analysis_service.get_segment_drift(
            metric_id=metric_id,
            evaluation_start_date=evaluation_start_date,
            evaluation_end_date=evaluation_end_date,
            comparison_start_date=comparison_start_date,
            comparison_end_date=comparison_end_date,
            dimensions=metric_dimensions,
        )

        stories = []
        top_4_slices = self.get_top_4_single_dimension_slices(segment_drift["dimension_slices"])
        for slice_info in top_4_slices:
            # Story for Growing or Shrinking segment slice
            percentage_difference = self.analysis_manager.calculate_percentage_difference(
                slice_info["evaluation_value"]["slice_value"],
                slice_info["comparison_value"]["slice_value"],
                precision=2,
            )

            data = {
                "past_val": round(slice_info["comparison_value"]["slice_share"], 2),
                "current_val": round(slice_info["evaluation_value"]["slice_share"], 2),
                "dimension": convert_snake_case_to_label(slice_info["key"][0]["dimension"]),
                "slice": slice_info["key"][0]["value"],
                "pressure_change": round(slice_info["slice_share_change_percentage"], 2),
                "pressure_direction": slice_info["pressure"].lower(),
            }

            story_details = self.prepare_story_dict(
                story_type=self.get_story_type_growing_or_shrinking(slice_info["slice_share_change_percentage"]),
                grain=grain,  # type: ignore
                metric=metric,
                df=pd.DataFrame(data, index=[0]),
                **data,
            )
            stories.append(story_details)
            logger.info(f"A new segment drift story created for metric {metric_id} with grain {grain}")
            logger.info(f"Story details: {story_details}")

            # Generating Story For Improving or Worsening segment slice
            if slice_info["impact"] != 0:
                data = {
                    "past_val": round(slice_info["comparison_value"]["slice_value"], 2),
                    "current_val": round(slice_info["evaluation_value"]["slice_value"], 2),
                    "dimension": convert_snake_case_to_label(slice_info["key"][0]["dimension"]),
                    "slice": slice_info["key"][0]["value"],
                    "percentage_difference": round(percentage_difference, 2),
                    "pressure_change": round(slice_info["change_percentage"], 2),
                    "pressure_direction": slice_info["pressure"].lower(),
                }

                story_details = self.prepare_story_dict(
                    story_type=self.get_story_type_worsening_or_improving(slice_info["impact"]),
                    grain=grain,  # type: ignore
                    metric=metric,
                    df=pd.DataFrame(data, index=[0]),
                    **data,
                )
                stories.append(story_details)
                logger.info(f"A new segment drift story created for metric {metric_id} with grain {grain}")
                logger.info(f"Story details: {story_details}")
        return stories

    def get_story_type_worsening_or_improving(self, impact):
        if impact > 0:
            return StoryType.IMPROVING_SEGMENT
        else:
            return StoryType.WORSENING_SEGMENT

    def get_story_type_growing_or_shrinking(self, slice_share_change):
        if slice_share_change > 0:
            return StoryType.GROWING_SEGMENT
        else:
            return StoryType.SHRINKING_SEGMENT

    def get_top_4_single_dimension_slices(self, dimension_slices):
        top_4_single_dimension_slices = []
        for single_dimension_slice in dimension_slices:
            if "|" in single_dimension_slice["serialized_key"]:
                continue
            top_4_single_dimension_slices.append(single_dimension_slice)
            if len(top_4_single_dimension_slices) == 4:
                break

        return top_4_single_dimension_slices
