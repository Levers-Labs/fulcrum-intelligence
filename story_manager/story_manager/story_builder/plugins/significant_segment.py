import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_builder import StoryBuilderBase
from story_manager.story_builder.utils import fetch_dimensions_from_metric


class SignificantSegmentStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.SIGNIFICANT_SEGMENTS
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    async def generate_stories(self, metric_id: str, grain: str) -> list[dict]:
        start_date, end_date = self._get_current_period_range(grain)  # type:ignore

        metric = await self.query_service.get_metric(metric_id)
        metric_dimensions = fetch_dimensions_from_metric(metric_details=metric)

        dimension_slices_dataframe = pd.DataFrame()

        for dimension in metric_dimensions:
            dimension_slice_values = await self.query_service.get_metric_values(
                metric_id=metric_id,
                dimensions=[dimension],
                start_date=start_date,
                end_date=end_date,
            )

            single_dimension_df = self.analysis_manager.get_dimension_slices_values_df(
                dimension_slice_values, dimension
            )

            dimension_slices_dataframe = pd.concat([dimension_slices_dataframe, single_dimension_df], ignore_index=True)

        dimension_slices_dataframe = dimension_slices_dataframe.sort_values(by="value", ascending=False)
        values_sum = dimension_slices_dataframe["value"].sum()
        no_of_slices = len(dimension_slices_dataframe)
        values_average = round(values_sum / no_of_slices, 2) if no_of_slices > 0 else 0

        top_4_dimensions_slices = self.analysis_manager.get_top_or_bottom_n_segments(
            dimension_slices_dataframe, top=True, no_of_slices=4
        )

        bottom_4_dimensions_slices = self.analysis_manager.get_top_or_bottom_n_segments(
            dimension_slices_dataframe, top=False, no_of_slices=4
        )

        top_4_segment_story = self.prepare_story_dict(
            story_type=StoryType.TOP_4_SEGMENTS,
            grain=grain,  # type: ignore
            metric=metric,
            df=top_4_dimensions_slices,
            average=values_average,
        )

        bottom_4_segment_story = self.prepare_story_dict(
            story_type=StoryType.BOTTOM_4_SEGMENTS,
            grain=grain,  # type: ignore
            metric=metric,
            df=bottom_4_dimensions_slices,
            average=values_average,
        )

        return [top_4_segment_story, bottom_4_segment_story]
