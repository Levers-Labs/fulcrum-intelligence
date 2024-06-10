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
        evaluation_start_date, evaluation_end_date = self._get_input_time_range(grain)  # type:ignore
        comparison_start_date, comparison_end_date = self._get_input_time_range(
            grain,  # type:ignore
            curr_date=evaluation_start_date,
        )
        metric = await self.query_service.get_metric(metric_id)
        metric_dimensions = fetch_dimensions_from_metric(metric_details=metric)

        significant_segments = await self.analysis_service.get_significant_segment(
            metric_id=metric_id,
            evaluation_start_date=evaluation_start_date,
            evaluation_end_date=evaluation_end_date,
            comparison_start_date=comparison_start_date,
            comparison_end_date=comparison_end_date,
            dimensions=metric_dimensions,
        )

        top_4_segment_story = self.prepare_story_dict(
            story_type=StoryType.TOP_4_SEGMENTS,
            grain=grain,  # type: ignore
            metric=metric,
            df=pd.DataFrame(significant_segments["top_4_segments"]),
        )

        bottom_4_segment_story = self.prepare_story_dict(
            story_type=StoryType.TOP_4_SEGMENTS,
            grain=grain,  # type: ignore
            metric=metric,
            df=pd.DataFrame(significant_segments["bottom_4_segments"]),
        )

        return [top_4_segment_story, bottom_4_segment_story]
