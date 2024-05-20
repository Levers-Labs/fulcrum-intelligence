import logging

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class LongRangeStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.TRENDS
    group = StoryGroup.LONG_RANGE
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    async def generate_stories(self, metric_id: str, grain: str) -> list[dict]:
        """
        Generate trend long range stories for the given metric and grain.

        Each story includes details about the performance of the time series.

        Input:
        The input DataFrame should contain the following columns:
        - date: The date of the metric data point.
        - value: The value of the metric.

        Logic:
        - A time series is constructed at the grain relevant to the digest.
        - Calculate Average Growth for the series
        - Calculate Overall Growth for the series
        - Calculate Slope for the series - slope
        - Based on the slope value identify the type,
            - If slope is positive, then IMPROVING_PERFORMANCE story type is created.
            - If slope is negative, then WORSENING_PERFORMANCE story type is created.

        Output:
        A list of trend stories containing metadata for each identified trend.

        :param metric_id: The metric ID for which trends stories are generated.
        :param grain: The grain of the time series data.
        :return: A list containing a trend story dictionary.
        """

        logging.info("Generating trends long range stories ...")

        # get metric details
        metric = await self.query_service.get_metric(metric_id)

        stories: list[dict] = []

        # find the start and end date for the input time series data
        start_date, end_date = self._get_input_time_range(grain)  # type: ignore

        # get time series data
        series_df = await self._get_time_series_data(
            metric_id, grain, start_date, end_date, set_index=False  # type: ignore
        )

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)  # type: ignore
        if len(series_df) < time_durations["min"]:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s'" "due to in insufficient data.",
                metric_id,
                grain,
            )
            return []

        avg_growth = self.analysis_manager.cal_average_growth(series_df["value"])
        initial_value = series_df["value"].iloc[0]
        final_value = series_df["value"].iloc[-1]
        overall_growth = self.analysis_manager.calculate_percentage_difference(final_value, initial_value)

        slope = self.analysis_manager.calculate_slope_of_series(series_df)

        story_type = StoryType.IMPROVING_PERFORMANCE if slope > 0 else StoryType.WORSENING_PERFORMANCE

        story_details = self.prepare_story_dict(
            story_type=story_type,
            grain=grain,  # type: ignore
            metric=metric,
            df=series_df,
            avg_growth=avg_growth,
            overall_growth=overall_growth,
            duration=len(series_df),
            start_date=start_date.strftime(self.date_text_format),
        )
        stories.append(story_details)
        logger.info("A new long range story created for metric '%s' with grain '%s'")
        logger.info(f"Story details: {story_details}")
        return stories
