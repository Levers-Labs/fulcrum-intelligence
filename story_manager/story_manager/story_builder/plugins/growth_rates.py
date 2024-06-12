import logging
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class GrowthStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.GROWTH
    group = StoryGroup.GROWTH_RATES
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    async def generate_stories(self, metric_id: str, grain: Granularity) -> list[dict[str, Any]]:
        """
        Generate growth stories for the given metric and grain.

        The logic for generating growth stories is as follows:
        - A time series of metric values is constructed for the grain and span relevant to the digest.
        - The time series is “once-differenced” to produce a series of growth rates.
            That is, for an ordered series {1, 2, 3, 4, 5}, the once-differenced series is
            {(2-1)/1, (3-2)/2, (4-3)/3, (5-4)/4}.
            The once-differenced time series represents growth rates.
        - The time series is then “twice-differenced” to produce a series representing the
            acceleration of growth rates (the second derivative).
        - The standard deviation is computed for the “twice-differenced” series.
        - If the most recent value in the twice-differenced series is more
            than one standard deviation above the previous value,
            this is an Accelerating Growth story.
            If the most recent value in the twice-differenced series is more than
            1 standard deviation below the previous value, this is a Slowing Growth story.
            Otherwise, no story is produced.

        Example output:
        [
            {
                "metric_id": "NewBizDeals",
                "genre": "GROWTH",
                "group": "GROWTH_RATES",
                "type": "ACCELERATING_GROWTH",
                "grain": "day",
                "text": " The d/d growth rate for NewBizDeals is speeding up. It is currently 15%
                    and up from the 10% average over the past 30 days.",
                "variables": {
                    "grain": "day",
                    "duration": 30,
                    "current_growth": 10.0,
                    "avg_growth": 8.0
                },
                "series": [
                    {"date": "2023-04-10", "value": 100, "growth_rate": 8.0},
                    {"date": "2023-04-11", "value": 110, "growth_rate": 9.0},
                    ...
                ]
            },
            ...
        ]

        :param metric_id: The metric ID for which growth stories are generated
        :param grain: The grain for which growth stories are generated
        :return: A list of generated growth stories
        """
        stories: list[dict] = []

        # get metric details
        metric = await self.query_service.get_metric(metric_id)

        # find the start and end date for the input time series data
        start_date, end_date = self._get_input_time_range(grain)

        # get time series data
        df = await self._get_time_series_data(metric_id, grain, start_date, end_date, set_index=False)
        df_len = len(df)

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)
        if df_len < time_durations["min"]:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to insufficient data", metric_id, grain
            )
            return []

        # Compute once-differenced series (growth rates)
        df["growth_rate"] = self.analysis_manager.calculate_growth_rates_of_series(df["value"])

        # Compute twice-differenced series (acceleration of growth rates)
        df["acceleration"] = self.analysis_manager.calculate_growth_rates_of_series(df["growth_rate"])

        # Compute standard deviation of the twice-differenced series
        std_acceleration = df["acceleration"].std()

        recent_acceleration = df["acceleration"].iloc[-1]
        previous_acceleration = df["acceleration"].iloc[-2]

        # Check if the most recent value in the twice-differenced series is more
        # than one standard deviation above the previous value
        if recent_acceleration > (previous_acceleration + std_acceleration):
            story_type = StoryType.ACCELERATING_GROWTH
        # Check if the most recent value in the twice-differenced series is more
        # than one standard deviation below the previous value
        elif recent_acceleration < (previous_acceleration - std_acceleration):
            story_type = StoryType.SLOWING_GROWTH
        else:
            logger.info(
                "Discarding story generation for metric '%s' with grain '%s' due to no significant growth acceleration",
                metric_id,
                grain,
            )
            return []

        logging.info(
            "%s detected for metric '%s' with grain '%s'. "
            "Recent acceleration: %s, Previous acceleration: %s, Std Dev: %s",
            story_type.value,
            metric_id,
            grain,
            recent_acceleration,
            previous_acceleration,
            std_acceleration,
        )
        current_growth = df["growth_rate"].iloc[-1]
        avg_growth = self.analysis_manager.cal_average_growth(df["value"])
        self.story_date = df["date"].iloc[-1]
        story_details = self.prepare_story_dict(
            story_type,
            grain=grain,
            metric=metric,
            df=df,
            story_date=self.story_date,  # type: ignore
            current_growth=round(current_growth),
            avg_growth=avg_growth,
            duration=df_len,
        )
        stories.append(story_details)
        return stories
