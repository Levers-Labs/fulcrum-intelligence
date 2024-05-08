import logging
from datetime import date

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

    async def generate_stories(self, metric_id: str, grain: str) -> list[dict]:
        """
        Generate trends stories for the given metric and grain.

        :param metric_id: The metric ID for which trends stories are generated.
        :param grain: The grain for which trends stories are generated.
        :return: A list of generated trend stories.
        """

        logging.info("Generating trends stories...")

        curr_start_date, curr_end_date = self._get_current_period_range(grain)  # type: ignore
        start_date = self._get_sliding_start_date(curr_end_date, grain)

        response = await self.analysis_service.perform_process_control(
            metric_id=metric_id, start_date=start_date, end_date=curr_end_date, grain=grain  # type: ignore
        )

        process_control_df = pd.DataFrame(response)

        process_control_df.rename(columns={"value": "value"}, inplace=True)

        process_control_df["slope"] = 0.0
        process_control_df["has_discontinuity"] = False
        process_control_df["growth_rate"] = 0.0
        process_control_df["trend_type"] = ""

        trends_stories = self._analyze_trends(process_control_df, metric_id, grain, start_date, curr_end_date)
        logging.info(f"Generated trends stories for metric '{metric_id}'")

        return trends_stories

    @staticmethod
    def _get_sliding_start_date(curr_start_date: date, grain: str) -> date:
        """
        Get the start date of the period based on the end date and grain.

        :param curr_start_date: The end date of the last period.
        :param grain: The grain for which the start date is aligned.
        :return: The start date of the period.
        """
        start_date = curr_start_date

        # Determine the grain delta based on grain
        if grain in ["day", "days"]:
            grain_delta = 30
        elif grain in ["week", "weeks"]:
            grain_delta = 8
        elif grain in ["month", "months"]:
            grain_delta = 4  # Assuming 4 months
        elif grain in ["year", "years"]:
            grain_delta = 1  # Assuming 1 year
        else:
            raise ValueError(f"Unsupported grain: {grain}")

        # Go back by the determined grain delta
        delta = pd.DateOffset(days=grain_delta) if grain in ["day", "days"] else pd.DateOffset(weeks=grain_delta)
        start_date -= delta

        return start_date.date() if isinstance(start_date, pd.Timestamp) else start_date

    @staticmethod
    def _calculate_slope_and_slope_change(current_data: float, i: int, series_df: pd.DataFrame, prev_data: float):
        """
        Calculate slope and slope change between two data points.

        :param current_data: The metric value data for the current point.
        :param i: The index of the current point in the DataFrame.
        :param series_df: The DataFrame containing process control data.
        :param prev_data: The metric value data for the previous point.
        :return: The calculated slope and slope change.
        """

        # Calculate slope and slope change
        slope, _, _, _, _ = linregress([i - 1, i], [prev_data, current_data])

        # Calculate slope change between consecutive data points
        if i > 0:
            prev_slope = series_df.at[i - 1, "slope"]
            slope_change = ((slope - prev_slope) / prev_slope) * 100 if prev_slope != 0 else 0
        else:
            slope_change = 0  # For the first data point, slope change is 0

        # Update DataFrame with slope
        series_df.at[i, "slope"] = slope

        return slope, slope_change

    @staticmethod
    def _has_discontinuity_condition(series_df: pd.DataFrame, i: int) -> bool:
        """
        Check for discontinuity based on Wheeler rules.

        :param series_df: The Process Control Response DataFrame containing process control data.
        :param i: The index of the current point in the DataFrame. This parameter indicates the position
              of the current data point within the DataFrame.
        :return: True if any of the Wheeler rules for discontinuity are met, False otherwise.

        Wheeler Rules:
        - Condition 1: Seven Consecutive Points Rule: 7 Individual Values in a row are above or below the Center Line
        - Condition 2: Eight Points Rule: 10 out of 12 Individual Values are above or below the Center Line
        - Condition 3: Trending Rule: 3 out of 4 Individual Values are closer to the UCL or LCL than the Center Line
        """

        seven_consecutive_points = (i >= 7) and all(series_df.iloc[i - 6 : i + 1]["has_discontinuity"])

        eight_points_rule = (i >= 12) and (series_df.iloc[i - 11 : i + 1]["has_discontinuity"].sum() >= 10)

        abs_diff = abs(series_df.iloc[i]["central_line"] - series_df.iloc[i]["value"])
        trending_rule = (i >= 4) and ((series_df.iloc[i - 3 : i + 1]["has_discontinuity"] < abs_diff).sum() >= 3)

        return any([seven_consecutive_points, eight_points_rule, trending_rule])

    @staticmethod
    def _identify_trend_type(prev_slope: float, slope: float, slope_change: float) -> str:
        """
        Identify the trend type based on the given parameters.

        :param prev_slope: The slope for the previous data point.
        :param slope: The slope calculated for the current data point.
        :param slope_change: The slope change calculated for the current data point.
        :return: The type of trend identified.
        """
        # Determine trend type
        if abs(slope_change) < 0.25:
            trend_type = StoryType.NEW_NORMAL
        elif slope > prev_slope:
            trend_type = StoryType.NEW_UPWARD_TREND
        else:
            trend_type = StoryType.NEW_DOWNWARD_TREND

        return trend_type

    def _analyze_trends(
        self, series_df: pd.DataFrame, metric_id: str, grain: str, start_date: date, end_date: date
    ) -> list[dict]:
        """
        Analyze the process control response DataFrame to identify trends.

        Input:
        The input DataFrame should contain the following columns:
        - "date": The date of the metric data point.
        - "metric_id": The ID of the metric.
        - "value": The value of the metric.
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

        :param series_df: The response DataFrame from the Process Control API.
        :param metric_id: The metric ID.
        :param grain: The grain of the time series data.
        :return: A list containing a single trend story.
        """
        logging.info("Analyzing trends stories...")

        if series_df.empty:
            logger.warning(f"No data available for metric '{metric_id}' with grain '{grain}'")
            return []

        if len(series_df) <= self.min_metric_count:
            logger.warning(f"Sufficient data not available for metric '{metric_id}' with grain '{grain}'")
            return []

        # Check for missing values and handle them
        if series_df.isnull().values.any():
            series_df.dropna(inplace=True)

        # Aggregate trend information
        current_data = pd.Series()
        prev_data = pd.Series()
        story_text = None

        downward_trend_periods = 0  # Counter to track consecutive downward trend periods
        normal_days_count = 0  # Counter to track consecutive days within normal range

        # Iterate over the DataFrame to analyze trends
        for i in range(1, len(series_df)):
            current_data = series_df.iloc[i]
            prev_data = series_df.iloc[i - 1]

            # Calculate slope and slope change
            curr_metric_val = float(current_data["value"])
            prev_metric_val = float(prev_data["value"])
            slope, slope_change = self._calculate_slope_and_slope_change(curr_metric_val, i, series_df, prev_metric_val)

            # Calculate growth rates
            self._calculate_growth_rates_of_series(series_df)

            # Wheeler rules to identify discontinuity
            if self._has_discontinuity_condition(series_df, i):
                series_df.at[i, "has_discontinuity"] = True
                prev_slope = float(prev_data["slope"])
                trend_type = self._identify_trend_type(prev_slope, slope, slope_change)
                series_df.at[i, "trend_type"] = trend_type

                # Check if the previous trend type was a sticky downward trend
                # and the current trend type is a new downward trend
                if (
                    prev_data["trend_type"] == StoryType.STICKY_DOWNWARD_TREND
                    and trend_type == StoryType.NEW_DOWNWARD_TREND
                ):
                    # If the previous trend was sticky downward and the current trend is also downward,
                    # consider it as part of the sticky downward trend
                    series_df.at[i, "trend_type"] = StoryType.STICKY_DOWNWARD_TREND

                elif trend_type == StoryType.NEW_DOWNWARD_TREND:
                    downward_trend_periods = (
                        series_df["trend_type"].iloc[max(0, i - 6) : i + 1] == StoryType.NEW_DOWNWARD_TREND
                    ).sum()
                    if downward_trend_periods >= 7:
                        series_df.at[i, "trend_type"] = StoryType.STICKY_DOWNWARD_TREND

            # Check if growth rate is within threshold of 0.25 for normal
            if abs(current_data["growth_rate"]) <= 0.25:
                normal_days_count += 1
            else:
                # If the streak breaks, update prior_normal_days and reset the streak count
                normal_days_count = 0

        # Generate a single trend story for the last data point if a trend is identified
        last_data_point = series_df.iloc[-1]
        trend_type = last_data_point["trend_type"]
        if trend_type:
            story_meta = STORY_TYPES_META[trend_type]  # type: ignore
            story_text = self._render_story_text(
                trend_type,  # type: ignore
                metric=metric_id,
                start_date=start_date,
                end_date=end_date,
                current_growth=current_data["growth_rate"],
                prior_growth=prev_data["growth_rate"],
                previous_normal=prev_data["growth_rate"],
                prior_normal_days=normal_days_count,
                downward_day_count=downward_trend_periods,
                grain_comp=grain,
                pop=self.grain_meta[grain]["comp_label"],
                direction="up" if trend_type == StoryType.NEW_UPWARD_TREND else "down",
            )

        story_metadata = {
            "metric_id": metric_id,
            "genre": self.genre,  # type: ignore
            "type": trend_type,
            "grain": grain,
            "story_text": story_text,
            "template": story_meta["template"],
            "variables": {
                "grain": grain,
                "current_trend_start_date": last_data_point["date"],
                "prior_trend_start_date": prev_data["date"],
                "current_growth": f"{current_data['growth_rate']:.2f}",
                "prior_growth": f"{prev_data['growth_rate']:.2f}",
            },
            "series": series_df.reset_index().astype({"date": str}).to_dict(orient="records"),
        }
        trends_stories = [story_metadata]
        logging.info(f"Generated trends stories for metric '{metric_id}'")
        logging.info(trends_stories)

        logging.info(f"Trends stories analyze completed for metric {metric_id}")
        return trends_stories
