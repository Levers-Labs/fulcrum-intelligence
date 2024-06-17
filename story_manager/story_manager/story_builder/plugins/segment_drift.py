import logging

import pandas as pd

from commons.models.enums import Granularity
from story_manager.config import get_settings
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_builder import StoryBuilderBase

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
        org_story_date = self.story_date
        settings = get_settings()
        evaluation_start_date, evaluation_end_date = self._get_input_time_range(
            grain,  # type: ignore
            half_time_range=True,
        )

        # Need to calc comparison date w.r.t the evaluation date
        self.story_date = evaluation_start_date
        comparison_start_date, comparison_end_date = self._get_input_time_range(
            grain,  # type: ignore
            half_time_range=True,
        )

        # setting the story_date back to the original story_date
        self.story_date = org_story_date

        metric = await self.query_service.get_metric(metric_id)
        metric_dimensions = self.get_metric_dimension_id_label_map(metric_details=metric)
        dimension_ids = list(metric_dimensions.keys())

        data_df = await self.query_service.get_metric_time_series_df(
            metric_id,
            comparison_start_date,
            evaluation_end_date,
            grain=grain,  # type: ignore
            dimensions=dimension_ids,
        )

        segment_drift = await self.analysis_manager.segment_drift(
            dsensei_base_url=settings.DSENSEI_BASE_URL,
            df=data_df,
            evaluation_start_date=evaluation_start_date,
            evaluation_end_date=evaluation_end_date,
            comparison_start_date=comparison_start_date,
            comparison_end_date=comparison_end_date,
            dimensions=dimension_ids,
        )

        stories = []
        df = self.convert_segment_drift_res_to_dataframe(segment_drift["dimension_slices"], metric_dimensions)
        sorted_segment_drift_df = df.sort_values(by="sort_value", ascending=False)

        top_4_slice_df = self.get_top_dimension_slices_df(
            df=sorted_segment_drift_df,
            no_of_slices=4,
            single_dimension=True,
        )

        for _, row in top_4_slice_df.iterrows():
            # Growing or Shrinking story
            story_details = self.prepare_story_dict(
                story_type=self.get_story_type_growing_or_shrinking(row["slice_share_change_percentage"]),
                grain=grain,  # type: ignore
                metric=metric,
                df=pd.DataFrame(row).transpose(),
                **row,
            )

            stories.append(story_details)
            logger.info(f"A new segment drift story created for metric {metric_id} with grain {grain}")
            logger.info(f"Story details: {story_details}")

            # If there's no change in slice value, we are skipping the improving or worsening story creation
            if row["impact"] == 0:
                continue

            # Improving or Worsening Story
            story_details = self.prepare_story_dict(
                story_type=self.get_story_type_worsening_or_improving(row["impact"]),
                grain=grain,  # type: ignore
                metric=metric,
                df=pd.DataFrame(row).transpose(),
                **row,
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

    def get_top_dimension_slices_df(
        self,
        df: pd.DataFrame,
        no_of_slices: int = 1,
        single_dimension: bool = True,
    ):
        """
        To fetch top n segments
        Input:
            - dimension slice dataframe
            - no_of_slices: top n slices to fetch
            - single_dimension: whether to fetch a single dimension slice
        """
        if not single_dimension:
            return df.iloc[:no_of_slices]

        return df[~df["serialized_key"].str.contains(r"\|")].iloc[:no_of_slices]

    def convert_segment_drift_res_to_dataframe(self, dimension_slices: dict, metric_dimensions: dict) -> pd.DataFrame:
        """
        Converting Segement Drift data to a pandas dataframe
        It would help in simplifying the overall analysis part.

        Input:
            Dimension Slices: list of dimension slices permutations we received from dsensei
            metric_dimensions: dictionary of metric dimensions {id : label}
        Output:
            Pandas Dataframe consist of:
                - comparison slice share
                - evalutaion slice share
                - dimension
                - slice name i.e region = Asia then slice name would be Asia
                - slice share change percentage: diff of evalutaion slice share and comparison slice share
                - pressure direction: derived from impact attribute of each dimension slice
                - comparison slice value
                - evalutaion slice value
                - slice value change percentage: change in evaluation value wrt comparison value
                - pressure change: derived from change percentage attribute of each dimension slice
                - impact
                - sort value : absolute impact value
                - serialized key: dimension slice Region = Asia then key would be Region:Asia

        """
        df = pd.DataFrame([])
        for dimension_slice in dimension_slices:
            row = {
                "previous_share": round(dimension_slice["comparison_value"]["slice_share"], 2),
                "current_share": round(dimension_slice["evaluation_value"]["slice_share"], 2),
                "dimension": metric_dimensions[dimension_slice["key"][0]["dimension"]],
                "slice_name": dimension_slice["key"][0]["value"],
                "slice_share_change_percentage": round(dimension_slice["slice_share_change_percentage"], 2),
                "pressure_direction": dimension_slice["pressure"].lower(),
                "previous_value": round(dimension_slice["comparison_value"]["slice_value"], 2),
                "current_value": round(dimension_slice["evaluation_value"]["slice_value"], 2),
                "deviation": self.analysis_manager.calculate_percentage_difference(
                    dimension_slice["evaluation_value"]["slice_value"],
                    dimension_slice["comparison_value"]["slice_value"],
                    precision=2,
                ),
                "pressure_change": round(dimension_slice["change_percentage"], 2),
                "impact": dimension_slice["impact"],
                "sort_value": dimension_slice["sort_value"],
                "serialized_key": dimension_slice["serialized_key"],
            }
            df = pd.concat([df, pd.DataFrame(row, index=[0])], ignore_index=True)
        return df
