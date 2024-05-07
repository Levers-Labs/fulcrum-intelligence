import logging

import pandas as pd
from scipy.stats import linregress

from commons.models.enums import Granularity
from story_manager.core.enums import STORY_TYPES_META, StoryGenre, StoryType
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class TrendsStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.TRENDS  # type: ignore
    supported_grains = [Granularity.DAY, Granularity.WEEK]
    min_metric_count = 5

    def __init__(self, query_service, analysis_service, db_session):
        super().__init__(query_service, analysis_service, db_session)
        self.persisted_stories = None

    def generate_stories(self, metric_id: str, grain: str) -> list[dict]:
        """
        Generate trends stories for the given metric and grain.

        :param metric_id: The metric ID for which trends stories are generated.
        :param grain: The grain for which trends stories are generated.
        :return: A list of generated trend stories.
        """

        # TODO: After process control api integration use start_date and end_date

        logging.info("Generating trends stories...")

        process_control_response = self.analysis_service.perform_process_control()

        trends_stories = self._analyze_trends(process_control_response, metric_id, grain)
        logging.info(f"Generated trends stories for metric '{metric_id}'")

        return trends_stories

    @staticmethod
    def _calculate_growth_rates(time_series_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the growth rates for each data point in the time series.

        :param time_series_df: The DataFrame containing the time series data.
        :return: The DataFrame with the calculated growth rates calculated.
        """
        time_series_df["growth_rate"] = time_series_df["metric_value"].pct_change() * 100
        return time_series_df

    @staticmethod
    def _calculate_slope_and_slope_change(
        current_data: pd.Series, i: int, pc_resp_df: pd.DataFrame, prev_data: pd.Series
    ):
        """
        Calculate slope and slope change between two data points.

        :param current_data: The data for the current point.
        :param i: The index of the current point in the DataFrame.
        :param pc_resp_df: The DataFrame containing process control data.
        :param prev_data: The data for the previous point.
        :return: The calculated slope and slope change.
        """

        # Calculate slope and slope change
        slope, _, _, _, _ = linregress([i - 1, i], [prev_data["metric_value"], current_data["metric_value"]])

        # Calculate slope change between consecutive data points
        if i > 0:
            prev_slope = pc_resp_df.at[i - 1, "slope"]
            slope_change = ((slope - prev_slope) / prev_slope) * 100 if prev_slope != 0 else 0
        else:
            slope_change = 0  # For the first data point, slope change is 0

        # Update DataFrame with slope
        pc_resp_df.at[i, "slope"] = slope

        return slope, slope_change

    @staticmethod
    def has_discontinuity_condition(pc_resp_df: pd.DataFrame, i: int) -> bool:
        """
        Check for discontinuity based on Wheeler rules.

        :param pc_resp_df: The DataFrame containing process control data.
        :param i: The index of the current point in the DataFrame.
        :return: True if any of the Wheeler rules for discontinuity are met, False otherwise.

        Wheeler Rules:
            - Condition 1: 7 Individual Values in a row are above or below the Center Line
            - Condition 2: 10 out of 12 Individual Values are above or below the Center Line
            - Condition 3: 3 out of 4 Individual Values are closer to the UCL or LCL than the Center Line
        """

        condition1 = (i >= 7) and all(pc_resp_df.iloc[i - 6 : i + 1]["has_discontinuity"])

        condition2 = (i >= 12) and (pc_resp_df.iloc[i - 11 : i + 1]["has_discontinuity"].sum() >= 10)

        abs_diff = abs(pc_resp_df.iloc[i]["central_line"] - pc_resp_df.iloc[i]["metric_value"])
        condition3 = (i >= 4) and ((pc_resp_df.iloc[i - 3 : i + 1]["has_discontinuity"] < abs_diff).sum() >= 3)

        return any([condition1, condition2, condition3])

    @staticmethod
    def _identify_trend_type(prev_data: pd.Series, slope: float, slope_change: float) -> str:
        """
        Identify the trend type based on the given parameters.

        :param prev_data: The data for the previous point.
        :param slope: The slope calculated for the current data point.
        :param slope_change: The slope change calculated for the current data point.
        :return: The type of trend identified.
        """
        # Determine trend type
        if abs(slope_change) < 0.25:
            trend_type = StoryType.NEW_NORMAL
        elif slope > prev_data["slope"]:
            trend_type = StoryType.NEW_UPWARD_TREND
        else:
            trend_type = StoryType.NEW_DOWNWARD_TREND

        return trend_type

    @staticmethod
    def _get_story_text(trend_type: str, trend_data: dict) -> str:
        """
        Get the template text for the specified trend type, filled with the provided trend data.

        :param trend_type: The type of trend for which to retrieve the template text.
        :param trend_data: The data to fill in the template tags.
        :return: The filled template text.
        """

        template_text = STORY_TYPES_META.get(trend_type)
        if not template_text:
            raise ValueError(f"No template found for trend type: {trend_type}")

        filled_template = template_text.get("template", "").format(**trend_data)
        return filled_template

    def _analyze_trends(self, pc_resp_df: pd.DataFrame, metric_id: str, grain: str) -> list[dict]:
        """
        Analyze the process control response DataFrame to identify trends.

        Input:
        The input DataFrame should contain the following columns:
        - "date": The date of the metric data point.
        - "metric_id": The ID of the metric.
        - "metric_value": The value of the metric.
        - "central_line": The central line of the control chart.
        - "ucl": The upper control limit.
        - "lcl": The lower control limit.
        - "grain": The grain of the time series data.

        Logic:
        - Run Wheeler rules to identify discontinuity
        and mark each data point with "has_discontinuity" as True or False.
        - If discontinuity exists:
            - Calculate slope for each data point using stats.linregress from scipy.
            - Determine trend type based on slope changes:
                - "NEW_NORMAL" if all slope changes are less than 0.25%.
                - "NEW_UPWARD_TREND" if the average slope is higher than the previous trend.
                - "NEW_DOWNWARD_TREND" if the average slope is lower than the previous trend.
                - "STICKY_DOWNWARD_TREND" if a downward trend persists for more than 7 periods.

        Steps:
        1. Iterate over the DataFrame to calculate slope and slope changes.
        2. Determine trend type based on the calculated slopes and changes.
        3. Generate trend stories for the last data point if a trend is identified.

        Output:
        A list of trend stories containing metadata for each identified trend.

        :param pc_resp_df: The response DataFrame from the Process Control API.
        :param metric_id: The metric ID.
        :param grain: The grain of the time series data.
        :return: A list containing a single trend story.
        """
        logging.info("Analyzing trends stories...")

        if pc_resp_df.empty:
            logger.warning(f"No data available for metric '{metric_id}' with grain '{grain}'")
            return []

        if len(pc_resp_df) <= self.min_metric_count:
            logger.warning(f"Sufficient data not available for metric '{metric_id}' with grain '{grain}'")
            return []

        # Check for missing values and handle them
        if pc_resp_df.isnull().values.any():
            pc_resp_df.dropna(inplace=True)

        # Aggregate trend information
        trend_type = None
        current_data = pd.Series()
        prev_data = pd.Series()
        story_text = None

        downward_trend_periods = 0  # Counter to track consecutive downward trend periods
        normal_days_count = 0  # Counter to track consecutive days within normal range
        threshold = 0.25

        # Iterate over the DataFrame to analyze trends
        for i in range(1, len(pc_resp_df)):
            current_data = pc_resp_df.iloc[i]
            prev_data = pc_resp_df.iloc[i - 1]

            # Calculate slope and slope change
            slope, slope_change = self._calculate_slope_and_slope_change(current_data, i, pc_resp_df, prev_data)

            # Calculate growth rates
            self._calculate_growth_rates(pc_resp_df)

            # Wheeler rules to identify discontinuity
            if self.has_discontinuity_condition(pc_resp_df, i):
                pc_resp_df.at[i, "has_discontinuity"] = True

                trend_type = self._identify_trend_type(prev_data, slope, slope_change)

                # Check for sticky downward trend
                if trend_type == StoryType.NEW_DOWNWARD_TREND:
                    downward_trend_periods += 1
                else:
                    downward_trend_periods = 0

                if downward_trend_periods >= 7:
                    trend_type = StoryType.STICKY_DOWNWARD_TREND

            # Check if growth rate is within threshold for normal
            if abs(current_data["growth_rate"]) <= threshold:
                normal_days_count += 1
            else:
                # If the streak breaks, update prior_normal_days and reset the streak count
                normal_days_count = 0

        # Extract start and end dates
        start_date = pc_resp_df.iloc[0]["date"]
        end_date = pc_resp_df.iloc[-1]["date"]

        # Get template text for the trend type
        trend_data = {
            "metric": metric_id,
            "start_date": start_date,
            "end_date": end_date,
            "current_growth": current_data["growth_rate"],
            "prior_growth": prev_data["growth_rate"],
            "previous_normal": prev_data["growth_rate"],
            "prior_normal_days": normal_days_count,
            "downward_day_count": downward_trend_periods,
            "grain_comp": grain,
        }
        if trend_type:
            story_text = self._get_story_text(trend_type, trend_data)

        # Generate a single trend story for the last data point if a trend is identified
        last_data_point = pc_resp_df.iloc[-1]
        story_metadata = {
            "metric_id": metric_id,
            "genre": self.genre,  # type: ignore
            "type": trend_type,
            "grain": grain,
            "story_text": story_text,
            "variables": {
                "grain": grain,
                "current_trend_start_date": last_data_point["date"],
                "prior_trend_start_date": prev_data["date"],
                "current_growth": f"{current_data['growth_rate']:.2f}",
                "prior_growth": f"{prev_data['growth_rate']:.2f}",
            },
            # "series": pc_resp_df,
        }
        trends_stories = [story_metadata]
        logging.info(f"Generated trends stories for metric '{metric_id}'")
        logging.info(trends_stories)

        logging.info(f"Trends stories analyze completed for metric {metric_id}")
        return trends_stories
