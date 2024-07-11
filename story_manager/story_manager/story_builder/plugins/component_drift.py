import logging

import numpy as np
import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Pressure,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class ComponentDriftStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.ROOT_CAUSES
    group = StoryGroup.COMPONENT_DRIFT
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]
    min_component_count = 2

    async def generate_stories(self, metric_id: str, grain: str) -> list[dict]:
        """
        Generate component drift stories for the given metric and grain.

        Input:
        The input DataFrame should contain the following columns:
        - date: The date of the metric data point.
        - value: The value of the metric.


        Logic:
        - A time series is constructed at the grain relevant to the digest.


        Output:
        A list of trend stories containing metadata for each identified trend.

        :param metric_id: The metric ID for which trends stories are generated.
        :param grain: The grain of the time series data.
        :return: A list containing a trend story dictionary.
        """

        logging.info("Generating trends exceptions stories ...")
        stories: list[dict] = []

        metric = await self.query_service.get_metric(metric_id)

        evaluation_start_date, evaluation_end_date = self._get_input_time_range(
            grain,  # type: ignore
        )

        self.story_date = evaluation_start_date
        comparison_start_date, comparison_end_date = self._get_input_time_range(
            grain,  # type: ignore
        )

        response = await self.analysis_service.get_component_drift(
            metric_id,
            evaluation_start_date,
            evaluation_end_date,
            comparison_start_date,
            comparison_end_date,
        )

        try:
            components = response["components"][0]["components"]
        except Exception as ex:
            logger.error(f"An error occured while fetching components data from response: {ex}")
            return []

        # Extract components data
        components_data = self.extract_components_data(components)

        # Create DataFrame
        df = self.create_ranked_df(components_data)

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)  # type: ignore
        if len(df) < time_durations["min"]:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s'" "due to in insufficient data.",
                metric_id,
                grain,
            )
            return []

        # Get top 4 components
        top_components = self.get_top_components(df, n=4)

        if len(top_components) < self.min_component_count:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s'" "due to insufficient components.",
                metric_id,
                grain,
            )
            return []

        # Create 'story_types' column based on conditions
        top_components["story_types"] = pd.Series(
            np.where(
                top_components["evaluation_value"] > top_components["comparison_value"],
                StoryType.IMPROVING_COMPONENT.value,
                StoryType.WORSENING_COMPONENT.value,
            )
        )

        # Create 'pressures' column based on conditions
        top_components["pressures"] = pd.Series(
            np.where(
                top_components["evaluation_value"] > top_components["comparison_value"],
                Pressure.UPWARD.value,
                Pressure.DOWNWARD.value,
            )
        )

        # Loop over top 4 components and compare evaluation_value and comparison_value
        for index, row in top_components.iterrows():
            story_details = self.prepare_story_dict(
                story_type=top_components.at[index, "story_types"],
                grain=grain,
                metric=metric,
                df=df,
                component=row["metric_id"],
                pressure=top_components.at[index, "pressures"],
                percentage_drift=abs(row["percentage_drift"]),
                relative_impact=row["relative_impact"],
                contribution=row["marginal_contribution_root"],
            )
            stories.append(story_details)

        logger.info(f"Component Drift Stories Execution Complete for metric '{metric_id}'")
        return stories

    @staticmethod
    def extract_components_data(components):
        """
        Extract relevant data from components into a list of dictionaries.
        """
        extracted_data = [
            {
                "metric_id": comp["metric_id"],
                "evaluation_value": comp["evaluation_value"],
                "comparison_value": comp["comparison_value"],
                **comp["drift"],  # Unpacking all keys from 'drift' dictionary
            }
            for comp in components
        ]
        return extracted_data

    @staticmethod
    def create_ranked_df(components_data: list[dict]) -> pd.DataFrame:
        """
        Create a ranked DataFrame of components based on marginal_contribution_root.

        :param components_data: The list of components dictionary.
        :return ranked_df: A ranked by 'marginal_contribution_root' df.
        """
        df = pd.DataFrame(components_data)
        ranked_df = df.sort_values(by="marginal_contribution_root", ascending=False)
        return ranked_df

    @staticmethod
    def get_top_components(df: pd.DataFrame, n=4) -> pd.DataFrame:
        """
        Get the top N components from a DataFrame based on 'marginal_contribution_root' ranking.

        :param df: The Dataframe
        :param n: The number of components
        :return the top n rows
        """
        # Making a copy to avoid modifying original DataFrame
        top_components = df.head(n).copy()

        # Round numerical columns
        top_components["percentage_drift"] = top_components["percentage_drift"].round(2)
        top_components["relative_impact"] = top_components["relative_impact"].round(2)
        top_components["marginal_contribution_root"] = top_components["marginal_contribution_root"].round(2)
        return top_components
