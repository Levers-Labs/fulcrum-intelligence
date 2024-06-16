import logging
from itertools import takewhile

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_builder import StoryBuilderBase
from story_manager.story_builder.utils import determine_status_for_value_and_target

logger = logging.getLogger(__name__)


class StatusChangeStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.STATUS_CHANGE
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]
    story_mapping = {
        (StoryType.ON_TRACK, StoryType.OFF_TRACK): StoryType.IMPROVING_STATUS,
        (StoryType.OFF_TRACK, StoryType.ON_TRACK): StoryType.WORSENING_STATUS,
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

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)
        if len(df) < time_durations["min"]:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to insufficient data", metric_id, grain
            )
            return []

        df["status"] = df.apply(determine_status_for_value_and_target, axis=1)

        current_period = df.iloc[-1]
        prev_period = df.iloc[-2]

        if pd.isnull(current_period["status"]) or pd.isnull(prev_period["status"]):
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to no story status",
                metric_id,
                grain,
            )
            return []

        current_status = current_period["status"]
        prev_status = prev_period["status"]
        if current_status == prev_status:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to no status change",
                metric_id,
                grain,
            )
            return []

        story_type = self.story_mapping.get((current_status, prev_status))  # noqa

        value = current_period["value"].item()
        target = current_period["target"].item()
        deviation = self.analysis_manager.calculate_percentage_difference(value, target)

        prev_duration = self.get_previous_status_duration(df, prev_status)  # noqa

        story_details = self.prepare_story_dict(
            story_type,  # type: ignore
            grain=grain,
            metric=metric,
            df=df,
            deviation=deviation,
            prev_duration=prev_duration,
        )
        stories.append(story_details)
        return stories

    @staticmethod
    def get_previous_status_duration(df: pd.DataFrame, target_status: StoryType) -> int:
        """
        Determine the duration of the previous story.
        :param df: the time series data.
        :param target_status: the previous story status.
        :return: the consecutive count of the previous story.
        """
        df_reversed = df.iloc[:-1][::-1]
        # Count consecutive occurrences of the target status
        count = sum(1 for _ in takewhile(lambda status: status == target_status, df_reversed["status"]))
        return count
