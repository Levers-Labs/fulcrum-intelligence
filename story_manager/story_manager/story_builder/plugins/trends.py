import logging

# from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from scipy.stats import linregress

from commons.models.enums import Granularity
from story_manager.story_manager.config import Paths
from story_manager.story_manager.db.config import get_session
from story_manager.story_manager.story_builder import StoryBuilderBase
from story_manager.story_manager.story_builder.core.enums import StoryGenre, StoryType

# from typing import Dict, List


logger = logging.getLogger(__name__)


class QueryService:
    pass


class AnalysisService:

    @staticmethod
    def get_process_control_df() -> pd.DataFrame:
        """
        Load process control data from a JSON file into a Pandas DataFrame.

        :return: A Pandas DataFrame containing the process control data.
        """
        file_path = Path.joinpath(Paths.BASE_DIR, "data/process_control_upward_trend.json")
        df = pd.read_json(file_path)
        df["slope"] = 0.0
        df["has_discontinuity"] = False
        df["growth_rate"] = 0.0
        return df


query_srv = QueryService()
analysis_srv = AnalysisService()


class TrendsStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.TRENDS  # type: ignore
    supported_grains = [Granularity.DAY, Granularity.WEEK]

    def __init__(self, query_service, analysis_service, db_session=get_session()):
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
        # start_date = datetime.now().date() - timedelta(days=40) # default 40 days
        # end_date = datetime.now().date()

        logging.info("Generating trends stories...")

        process_control_response = analysis_srv.get_process_control_df()

        if process_control_response.empty:
            logger.warning(f"No data available for metric '{metric_id}' with grain '{grain}'")
            return []

        trends_stories = self._analyze_trends(process_control_response, metric_id, grain)
        logging.info(f"Generated trends stories for metric '{metric_id}'")

        return trends_stories

    def _analyze_trends(self, process_control_df: pd.DataFrame, metric_id: str, grain: str) -> list[dict]:
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

        :param process_control_df: The response DataFrame from the Process Control API.
        :param metric_id: The metric ID.
        :param grain: The grain of the time series data.
        :return: A list containing a single trend story.
        """
        logging.info("Analyzing trends stories...")

        # Aggregate trend information
        trends_stories = []
        trend_type = ""
        downward_trend_periods = 0  # Counter to track consecutive downward trend periods

        # Iterate over the DataFrame to analyze trends
        for i in range(1, len(process_control_df)):
            current_data = process_control_df.iloc[i]
            prev_data = process_control_df.iloc[i - 1]

            # Calculate slope and slope change
            slope, _, _, _, _ = linregress([i - 1, i], [prev_data["metric_value"], current_data["metric_value"]])
            slope_change = ((slope - prev_data["slope"]) / prev_data["slope"]) * 100 if prev_data["slope"] != 0 else 0

            # Calculate growth rate
            growth_rate = ((current_data["metric_value"] - prev_data["metric_value"]) / prev_data["metric_value"]) * 100

            # Update DataFrame with slope, slope change, and growth rate
            process_control_df.at[i, "slope"] = slope
            process_control_df.at[i, "slope_change"] = slope_change
            process_control_df.at[i, "growth_rate"] = growth_rate

            # Wheeler rules to identify discontinuity
            if (
                i >= 7
                and all(process_control_df.iloc[i - 6: i + 1]["has_discontinuity"])
                or i >= 12
                and sum(process_control_df.iloc[i - 11: i + 1]["has_discontinuity"]) >= 10
                or i >= 4
                and sum(
                    process_control_df.iloc[i - 3: i + 1]["has_discontinuity"]
                    < abs(process_control_df.iloc[i]["central_line"] - process_control_df.iloc[i]["metric_value"])
                )
                >= 3
            ):
                process_control_df.at[i, "has_discontinuity"] = True

                # Determine trend type
                if abs(slope_change) < 0.25:
                    trend_type = StoryType.NEW_NORMAL
                elif slope > prev_data["slope"]:
                    trend_type = StoryType.NEW_UPWARD_TREND
                else:
                    trend_type = StoryType.NEW_DOWNWARD_TREND

                # Check for sticky downward trend
                if trend_type == StoryType.NEW_DOWNWARD_TREND:
                    downward_trend_periods += 1
                else:
                    downward_trend_periods = 0

                if downward_trend_periods >= 7:
                    trend_type = StoryType.STICKY_DOWNWARD_TREND

            # Generate a single trend story for the last data point if a trend is identified
            last_data_point = process_control_df.iloc[-1]
            story_metadata = {
                "metric_id": metric_id,
                "genre": self.genre,
                "type": trend_type,
                "grain": grain,
                "text": f"Trend story for metric {metric_id} and grain {grain}",
                "variables": {
                    "grain": grain,
                    "current_trend_start_date": last_data_point["date"],
                    "prior_trend_start_date": prev_data["date"],
                    "current_growth": f"{current_data['growth_rate']:.2f}",
                    "prior_growth": f"{prev_data['growth_rate']:.2f}",
                },
            }
            trends_stories = [story_metadata]
            logging.info(f"Generated trends stories for metric '{metric_id}'")
            logging.info(trends_stories)

        logging.info(f"Trends stories analyze completed for metric {metric_id}")
        return trends_stories
