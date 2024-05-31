import logging
from itertools import takewhile

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class StatusChangeStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.STATUS_CHANGE
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

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

        df["status"] = df.apply(self.determine_status, axis=1)

        # get the most recent date for the current and previous period
        latest_date, _ = self._get_current_period_range(grain)
        prev_latest_date, _ = self._get_current_period_range(grain, curr_date=latest_date)

        current_period = df[df["date"] == pd.to_datetime(latest_date)]
        prev_period = df[df["date"] == pd.to_datetime(prev_latest_date)]
        if current_period.empty or prev_period.empty:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to no data for the dates",
                metric_id,
                grain,
            )
            return []

        if current_period.isnull().values.any() or prev_period.isnull().values.any():
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to no target / actual value",
                metric_id,
                grain,
            )
            return []

        current_status = current_period["status"].item()
        prev_status = prev_period["status"].item()
        if current_status == prev_status:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to no status change",
                metric_id,
                grain,
            )
            return []

        story_type = StoryType.WORSENING_STATUS if prev_status == StoryType.ON_TRACK else StoryType.IMPROVING_STATUS

        value = current_period["value"].item()
        target = current_period["target"].item()
        deviation = self.analysis_manager.calculate_percentage_difference(value, target)

        prev_duration = self.get_previous_duration(df, prev_status)
        story_details = self.prepare_story_dict(
            story_type,
            grain=grain,
            metric=metric,
            df=df,
            deviation=deviation,
            prev_duration=prev_duration,
        )
        stories.append(story_details)
        return stories

    @staticmethod
    def determine_status(df_row: pd.Series) -> StoryType | None:
        """
        Determine the status of the story.
        :param df_row: each row of the dataframe.
        :return: string indicating the status of the story and None if the target is not available.
        """
        value = df_row["value"]
        target = df_row["target"]
        if pd.isnull(target):
            return None
        elif value >= target:
            return StoryType.ON_TRACK
        else:
            return StoryType.OFF_TRACK

    @staticmethod
    def get_previous_duration(df: pd.DataFrame, target_status: StoryType) -> int:
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
