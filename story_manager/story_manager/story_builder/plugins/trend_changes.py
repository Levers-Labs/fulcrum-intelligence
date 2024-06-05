import logging

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Movement,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class TrendChangesStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.TRENDS
    group = StoryGroup.TREND_CHANGES
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    async def generate_stories(self, metric_id: str, grain: Granularity) -> list[dict]:
        """
        Generate trends stories for the given metric and grain.
        Analyze the process control response DataFrame to identify trends.

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
        - The time series is evaluated using the Process Control module to determine
        if the most recent measurement triggers a Wheeler rule.
        - If a Wheeler rule has not been triggered, a Stable Trend story is created.
        - If a Wheeler rule has been triggered:
            - A New Upward Trend story is created if the slope of the current
            Center Line is greater than the slope of the immediately prior Center Line.
            - A New Downward Trend story is created if the slope of the current
            Center Line is less than the slope of the immediately prior Center Line.
            - A Performance Plateau story is created if the slope of the current
            Center Line is <1%. Note, this logic may result in a
            Performance Plateau story being created in addition to one of the other stories.

        Output:
        A list of trend stories containing metadata for each identified trend.

        :param metric_id: The metric ID for which trends stories are generated.
        :param grain: The grain of the time series data.
        :return: A list containing a trend story dictionary.
        """
        # get metric details
        metric = await self.query_service.get_metric(metric_id)

        # find the start and end date for the input time series data
        start_date, end_date = self._get_input_time_range(grain)

        # get time series data
        df = await self._get_time_series_data(metric_id, grain, start_date, end_date, set_index=False)

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)
        if len(df) < time_durations["min"]:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to insufficient data", metric_id, grain
            )
            return []

        stories: list[dict] = []
        # Run process control analysis over the time series data
        pc_df = self.analysis_manager.process_control(df=df)

        # check if trend signal detected or not within an output period
        grain_durations = self.get_time_durations(grain)
        output_period = grain_durations["output"]
        stories_df = pc_df.tail(output_period)
        # only check if the most recent measurement triggers a Wheeler rule
        # check if trend signal detected in 7 latest data points
        latest_trend_signal_detected = stories_df["trend_signal_detected"].tail(7).any()

        if not latest_trend_signal_detected:
            # Calculate average growth for the stable trend
            avg_growth = self.analysis_manager.cal_average_growth(stories_df["value"])

            # Movement is increase if avg_growth is positive, otherwise decrease
            movement = Movement.INCREASE if avg_growth > 0 else Movement.DECREASE
            # todo: add util that calculates no. of grain periods between start and end date
            trend_duration = len(stories_df)
            logging.info(
                "Following a stable trend for metric '%s' with grain '%s'. Average growth: %s",
                metric_id,
                grain,
                avg_growth,
            )
            story_details = self.prepare_story_dict(
                StoryType.STABLE_TREND,
                grain=grain,
                metric=metric,
                df=stories_df,
                avg_growth=abs(avg_growth),
                trend_duration=trend_duration,
                movement=movement.value,
            )
            stories.append(story_details)
        # trend signal detected stories
        else:
            # Calculate growth rates and average growth for current and previous trends
            # adding trend_id to the dataframe starting from 1
            pc_df["trend_id"] = pc_df["trend_signal_detected"].cumsum() + 1

            # Get trend ids of the current and previous trends
            current_trend_id = pc_df["trend_id"].iloc[-1]
            previous_trend_id = pc_df["trend_id"].unique()[-2]

            # get the current and previous trend dataframes
            current_trend = pc_df[pc_df["trend_id"] == current_trend_id]
            previous_trend = pc_df[pc_df["trend_id"] == previous_trend_id]

            # get the start date of the current trend
            trend_start_date = current_trend["date"].iloc[0]
            trend_start_date_str = trend_start_date.strftime(self.date_text_format)
            previous_trend_duration = len(previous_trend)

            # Calculate the average growth rates for the current and previous trends
            current_avg_growth = self.analysis_manager.cal_average_growth(current_trend["value"])
            previous_avg_growth = self.analysis_manager.cal_average_growth(previous_trend["value"])

            # get the last 2 from bottom unique slope values
            prior_slope, latest_slope = pc_df["slope"].unique()[-2:]

            # Determine the story type based on the slope of the current and prior trend
            if latest_slope > prior_slope:
                story_type = StoryType.NEW_UPWARD_TREND
            else:
                story_type = StoryType.NEW_DOWNWARD_TREND

            # Add an upward/downward trend story
            logger.info("New %s trend detected for metric '%s' with grain '%s'", story_type.value, metric_id, grain)
            story_details = self.prepare_story_dict(
                story_type,
                grain=grain,
                metric=metric,
                df=pc_df,
                current_avg_growth=current_avg_growth,
                previous_avg_growth=previous_avg_growth,
                previous_trend_duration=previous_trend_duration,
                trend_start_date=trend_start_date_str,
            )
            stories.append(story_details)

            # check for performance plateau
            if latest_slope < 1:
                logging.info("Performance Plateau detected for metric '%s' with grain '%s'", metric_id, grain)
                avg_value = round(current_trend["value"].mean())
                story_details = self.prepare_story_dict(
                    StoryType.PERFORMANCE_PLATEAU,
                    grain=grain,
                    metric=metric,
                    df=pc_df,
                    avg_value=avg_value,
                    current_avg_growth=current_avg_growth,
                    trend_start_date=trend_start_date_str,
                )
                stories.append(story_details)

        return stories
