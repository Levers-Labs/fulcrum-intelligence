import calendar
import logging
from datetime import date

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Movement,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.story_builder import StoryBuilderBase
from story_manager.story_builder.constants import GRAIN_META
from story_manager.story_builder.utils import get_target_value_for_date

logger = logging.getLogger(__name__)


class RequiredPerformanceStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.REQUIRED_PERFORMANCE
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]
    story_movement_map = {
        StoryType.REQUIRED_PERFORMANCE: Movement.DECREASE.value,
        StoryType.HOLD_STEADY: Movement.INCREASE.value,
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
        is_min_data = False

        # get metric details
        metric = await self.query_service.get_metric(metric_id)

        # find the start and end date for the input time series data
        start_date, end_date = self._get_input_time_range(grain)

        # find the interval and end of current period
        interval, period_end_date = self._get_end_date_of_period(grain)

        # get time series data with targets
        df = await self._get_time_series_data_with_targets(metric_id, grain, start_date, end_date)
        if df.empty:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to no data", metric_id, grain
            )
            return []

        # get growth rate for the series
        df["growth_rate"] = self.analysis_manager.calculate_growth_rates_of_series(df["value"])
        df["growth_rate"] = df["growth_rate"].fillna(value=0)

        # get the target value for the end of the period
        target_df = await self._get_time_series_for_targets(metric_id, grain, end_date, period_end_date)
        target = get_target_value_for_date(target_df, period_end_date)
        if pd.isnull(target) or target is None:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to no target for end of interval",
                metric_id,
                grain,
            )
            return []

        current_period = df.iloc[-1]
        value = current_period["value"].item()

        if value >= target:
            story_type = StoryType.HOLD_STEADY
        else:
            story_type = StoryType.REQUIRED_PERFORMANCE

        # update min_data flag if the data is less then min required
        time_durations = self.get_time_durations(grain)
        if len(df) < time_durations["min"]:
            is_min_data = True

        req_duration = self.calculate_periods_count(end_date, period_end_date, grain)
        required_growth = self.analysis_manager.calculate_required_growth(value, target, req_duration, 2)
        current_growth = current_period["growth_rate"].item()
        growth_deviation = self.analysis_manager.calculate_percentage_difference(current_growth, required_growth)

        # prepare story details
        story_details = self.prepare_story_dict(
            story_type,
            grain=grain,
            metric=metric,
            df=df,
            req_duration=req_duration,
            duration=len(df),
            interval=interval,
            target=target,
            is_min_data=is_min_data,
            required_growth=required_growth,
            current_growth=current_growth,
            growth_deviation=abs(growth_deviation),
            movement=self.story_movement_map[story_type],
        )
        stories.append(story_details)
        logger.info(f"Stories generated for metric '{metric_id}', story details: {story_details}")

        return stories

    @staticmethod
    def _get_end_date_of_period(grain: Granularity, curr_date: date | None = None) -> tuple[Granularity, date]:
        """
        Get the end date of the period of the given grain.

        Logic:
        - for day and week grain,
          End date will be the end of current month with interval month.
        - for month grain,
          End date will be the end of current quarter with interval quarter.

        :param grain: Granularity of the time series data.
        :param curr_date: Date of the grain, default current date.
        :return: interval and end date of the period.
        """

        today = curr_date or date.today()
        if grain == Granularity.DAY or grain == Granularity.WEEK:
            interval = GRAIN_META[Granularity.MONTH]["interval"]
            # End of the month
            end_date = today.replace(day=calendar.monthrange(today.year, today.month)[1])
        elif grain == Granularity.MONTH:
            interval = GRAIN_META[Granularity.QUARTER]["interval"]
            # Determine the end of the current quarter
            quarter_start_month = (today.month - 1) // 3 * 3 + 1
            quarter_end_month = quarter_start_month + 2
            end_date = today.replace(month=quarter_end_month, day=calendar.monthrange(today.year, quarter_end_month)[1])
        else:
            raise ValueError(f"Unsupported grain: {grain}")

        return interval, end_date

    @staticmethod
    def calculate_periods_count(start_date: date, end_date: date, grain: Granularity) -> int:
        """
        Calculate the difference between two dates based on the specified granularity.
        """
        if grain == Granularity.DAY:
            count = abs((start_date - end_date).days)
        elif grain == Granularity.WEEK:
            weeks_difference = (start_date - end_date).days // 7
            # Adjust the weeks difference if the remainder of the days is not zero
            if (start_date - end_date).days % 7 != 0:
                weeks_difference += 1
            count = abs(weeks_difference)
        elif grain == Granularity.MONTH:
            # Calculate the difference in months
            months_difference = (start_date.year - end_date.year) * 12 + (start_date.month - end_date.month)
            count = abs(months_difference)
        else:
            raise ValueError(f"Unsupported grain: {grain}")
        return count
