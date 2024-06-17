from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_builder import StoryBuilderBase


class SignificantSegmentStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.SIGNIFICANT_SEGMENTS
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    async def generate_stories(self, metric_id: str, grain: str) -> list[dict]:
        start_date, end_date = self._get_current_period_range(grain)  # type:ignore
        metric = await self.query_service.get_metric(metric_id)
        metric_dimensions = self.get_metric_dimension_id_label_map(metric_details=metric)
        dimension_ids = list(metric_dimensions.keys())
        dimensions_slices_df_map = {}

        for dimension in dimension_ids:
            dimension_slices_df = await self.query_service._get_metric_values_df(
                metric_id=metric_id,
                dimensions=[dimension],
                start_date=start_date,
                end_date=end_date,
            )

            dimensions_slices_df_map[dimension] = dimension_slices_df

        dimensions_slices_df = self.analysis_manager.get_dimension_slices_df(dimensions_slices_df_map)
        dimensions_slices_df = dimensions_slices_df.sort_values(by="value", ascending=False)

        avg_value = round(dimensions_slices_df["value"].mean() or 0.0, 2)

        top_4_dimensions_slices_df = dimensions_slices_df.head(4)
        top_4_segment_story = self.prepare_story_dict(
            story_type=StoryType.TOP_4_SEGMENTS,
            grain=grain,  # type: ignore
            metric=metric,
            df=top_4_dimensions_slices_df,
            average=avg_value,
        )

        bottom_4_dimensions_slices_df = dimensions_slices_df.tail(4)
        bottom_4_segment_story = self.prepare_story_dict(
            story_type=StoryType.BOTTOM_4_SEGMENTS,
            grain=grain,  # type: ignore
            metric=metric,
            df=bottom_4_dimensions_slices_df,
            average=avg_value,
        )

        return [top_4_segment_story, bottom_4_segment_story]
