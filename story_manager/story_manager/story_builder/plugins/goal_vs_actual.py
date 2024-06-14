import logging

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Direction,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.story_builder import StoryBuilderBase
from story_manager.story_builder.utils import determine_status_for_value_and_target

logger = logging.getLogger(__name__)


class GoalVsActualStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.GOAL_VS_ACTUAL
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]
    story_direction_map = {
        StoryType.ON_TRACK: Direction.UP.value,
        StoryType.OFF_TRACK: Direction.DOWN.value,
    }

    async def generate_stories(self, metric_id: str, grain: Granularity) -> list[dict]:
        """
        Generate trends stories for the given metric and grain.
        Analyze the process control response DataFrame to identify trends.

        Input:
        The input DataFrame should contain the following columns:
        - date: The date of the metric data point.
        - value: The value of the metric.
        - target: The target for the date.

        Logic:
        - A time series of metric and target values is constructed for the grain and span relevant to the digest.
        - The most recent period’s actual is compared to the corresponding target.
        - If a target is not available for the current period, no story is created.
        - If the actual is at or above the target, an On Track story is created. Else an Off Track story.

        Output:
        A list of stories containing metadata for each identified trend.

        :param metric_id: The metric ID for which trends stories are generated.
        :param grain: The grain of the time series data.
        :return: A list containing a goal_vs_actual story dictionary.
        """
        stories: list[dict] = []

        # get metric details
        metric = await self.query_service.get_metric(metric_id)

        # find the start and end date for the input time series data
        start_date, end_date = self._get_input_time_range(grain)

        # get time series data with targets
        df = await self._get_time_series_data_with_targets(metric_id, grain, start_date, end_date)
        df_len = len(df)

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)
        if df_len < time_durations["min"]:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to insufficient data", metric_id, grain
            )
            return []

        # get growth rate for the series
        df["growth_rate"] = self.analysis_manager.calculate_growth_rates_of_series(df["value"])
        df["growth_rate"] = df["growth_rate"].fillna(value=0)

        # Get story status for the df
        df["status"] = df.apply(determine_status_for_value_and_target, axis=1)

        # data for the most recent date
        ref_data = df.iloc[-1]
        if pd.isnull(ref_data["status"]):
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to no story",
                metric_id,
                grain,
            )
            return []

        # calculate deviation % of value from the target
        value = ref_data["value"].item()
        target = ref_data["target"].item()
        deviation = self.analysis_manager.calculate_percentage_difference(value, target)

        story_type = ref_data["status"]
        growth = ref_data["growth_rate"].item()
        story_details = self.prepare_story_dict(
            story_type,  # type: ignore
            grain=grain,
            metric=metric,
            df=df,
            current_value=value,
            direction=self.story_direction_map.get(story_type),  # noqa
            current_growth=growth,
            target=target,
            deviation=abs(deviation),
            duration=df_len,
        )
        stories.append(story_details)
        logger.info(f"Stories generated for metric '{metric_id}', story details: {story_details}")
        return stories
