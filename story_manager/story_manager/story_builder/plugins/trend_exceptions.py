import logging

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Position,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.story_builder import StoryBuilderBase
from story_manager.story_builder.utils import get_story_date

logger = logging.getLogger(__name__)


class TrendExceptionsStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.TRENDS
    group = StoryGroup.TREND_EXCEPTIONS
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    async def generate_stories(self, metric_id: str, grain: str) -> list[dict]:
        """
        Generate trend exceptions stories for the given metric and grain.

        Each story includes details about the type of anomaly, the deviation,
        and its position relative to control limits.

        Input:
        The input DataFrame should contain the following columns:
        - date: The date of the metric data point.
        - value: The value of the metric.
        - central_line: The central line of the control chart.
        - ucl: The upper control limit.
        - lcl: The lower control limit.
        - slope: The slope of the metric data.
        - slope_change: The slope change between consecutive data points.
        - trend_signal_detected: A boolean indicating if a trend signal is detected.

        Logic:
        - A time series is constructed at the grain relevant to the digest.
        - The time series is evaluated using the Process Control module to get ucl, lcl, central_line etc...
        - Identify story type:
            - If current value > UCL then, Spike story is created.
            - If current value < LCL then, Drop Story Type is created.
            - if current value is within the limits then no story is created.
        - If a story exists, Calculate deviation % of value with ucl and lcl.

        Output:
        A list of trend stories containing metadata for each identified trend.

        :param metric_id: The metric ID for which trends stories are generated.
        :param grain: The grain of the time series data.
        :return: A list containing a trend story dictionary.
        """

        logging.info("Generating trends exceptions stories ...")

        metric = await self.query_service.get_metric(metric_id)
        stories: list[dict] = []

        # find the start and end date for the input time series data
        start_date, end_date = self._get_input_time_range(grain)

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

        pc_df = self.analysis_manager.process_control(df=series_df)

        ref_data = pc_df.iloc[-1]

        story_type = None
        if ref_data["value"] > ref_data["ucl"]:
            deviation = self.analysis_manager.calculate_percentage_difference(
                ref_data["value"].item(), ref_data["ucl"].item()
            )
            story_type = StoryType.SPIKE
            position = Position.ABOVE
        elif ref_data["value"] < ref_data["lcl"]:
            deviation = self.analysis_manager.calculate_percentage_difference(
                ref_data["value"].item(), ref_data["lcl"].item()
            )
            story_type = StoryType.DROP
            position = Position.BELOW

        if story_type:
            story_date = get_story_date(pc_df)
            story_details = self.prepare_story_dict(
                story_type=story_type,
                grain=grain,  # type: ignore
                metric=metric,
                df=pc_df,
                story_date=story_date,
                deviation=deviation,
                position=position.value,
            )
            stories.append(story_details)
            logger.info("A new story created for metric '%s' with grain '%s'")
            logger.info(f"Story details: {story_details}")
        logger.info(f"Trends exceptions stories complete for metric '{metric_id}'")
        return stories
