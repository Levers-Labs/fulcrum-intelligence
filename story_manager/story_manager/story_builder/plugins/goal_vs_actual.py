import logging

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Direction,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class GoalVsActualStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.GOAL_VS_ACTUAL
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]
    date_format = "%Y-%m-%d"

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

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)
        if len(df) < time_durations["min"]:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to insufficient data", metric_id, grain
            )
            return []

        # get growth rate for the series
        df["growth_rate"] = self.analysis_manager.calculate_growth_rates_of_series(df["value"])

        # get the most recent date for the period
        latest_start_date, _ = self._get_current_period_range(grain)
        latest_date = latest_start_date.strftime(self.date_format)

        # data for the most recent date
        ref_data = df.loc[df["date"] == latest_date]
        # validate if data exists for the date
        if ref_data.empty:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to no data for the most "
                "recent period date '%s'",
                metric_id,
                grain,
                latest_date,
            )
            return []

        # Retrieve value, growth and target from ref data and convert to float
        value, growth = ref_data.loc[ref_data.index[0], ["value", "growth_rate"]].apply(float)
        target = ref_data.at[ref_data.index[0], "target"]

        # validate if target exist, exit if no target
        if not target:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to no target for the most "
                "recent period date '%s'",
                metric_id,
                grain,
                latest_date,
            )
            return []

        if value >= target:
            story_type = StoryType.ON_TRACK
            direction = Direction.UP.value
        else:
            story_type = StoryType.OFF_TRACK
            direction = Direction.DOWN.value

        # calculate deviation % of value from the target
        deviation = self.analysis_manager.calculate_percentage_difference(value, target)
        story_details = self.prepare_story_dict(
            story_type,
            grain=grain,
            metric=metric,
            df=df,
            current_value=value,
            direction=direction,
            current_growth=growth,
            target=target,
            deviation=abs(deviation),
            duration=len(df),
        )
        stories.append(story_details)
        logger.info(f"Stories generated for metric '{metric_id}', story details: {story_details}")
        return stories
