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
        if not response:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s'" "due to no components data.",
                metric_id,
                grain,
            )
            return []

        components = self.fetch_all_components(response)

        # Extract components data
        components_df = self.extract_components_data(components)

        if len(components_df) < self.min_component_count:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s'" "due to insufficient components.",
                metric_id,
                grain,
            )
            return []

        # Create DataFrame
        df = self.create_ranked_df(components_df)

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

        # Loop over top 4 components and compare evaluation_value and comparison_value
        for index, row in top_components.iterrows():
            pct_drift_change = self.analysis_manager.calculate_percentage_difference(
                float(row["evaluation_value"]), float(row["comparison_value"]), 2
            )
            story_details = self.prepare_story_dict(
                story_type=top_components.at[index, "story_type"],
                grain=grain,  # type: ignore
                metric=metric,
                df=df,
                component=row["metric_id"],
                pressure=top_components.at[index, "pressure"],
                percentage_drift=abs(pct_drift_change),
                relative_impact=row["relative_impact"],
                contribution=row["marginal_contribution_root"],
            )
            stories.append(story_details)

        logger.info(f"Component Drift Stories Execution Complete for metric '{metric_id}'")
        return stories

    @staticmethod
    def extract_components_data(components) -> pd.DataFrame:
        """
        Extract relevant data from components into a DataFrame.

        :param components: List of dictionaries containing component data.
        :return: DataFrame containing extracted component data with added 'story_type' and 'pressure' columns.
        """
        extracted_data = [
            {
                "metric_id": comp.get("metric_id", None),
                "evaluation_value": comp.get("evaluation_value", 0),
                "comparison_value": comp.get("comparison_value", 0),
                **comp["drift"],  # Unpacking all keys from 'drift' dictionary
            }
            for comp in components
        ]

        df = pd.DataFrame(extracted_data)

        # Create 'story_type' column based on conditions
        df["story_type"] = np.where(
            df["evaluation_value"] > df["comparison_value"],
            StoryType.IMPROVING_COMPONENT.value,
            StoryType.WORSENING_COMPONENT.value,
        )

        # Create 'pressure' column based on conditions
        df["pressure"] = np.where(
            df["evaluation_value"] > df["comparison_value"],
            Pressure.UPWARD.value,
            Pressure.DOWNWARD.value,
        )

        return df

    @staticmethod
    def create_ranked_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Create a ranked DataFrame of components based on absolute marginal_contribution_root.

        :param df: The Dataframe with components data.
        :return ranked_df: A DataFrame ranked by absolute 'marginal_contribution_root'.
        """
        # Apply absolute value to 'marginal_contribution_root' column
        df["abs_marginal_contribution_root"] = df["marginal_contribution_root"].abs()

        # Sort DataFrame by absolute 'marginal_contribution_root' in descending order
        ranked_df = df.sort_values(by="abs_marginal_contribution_root", ascending=False)

        # Reset index to maintain a clean integer index
        ranked_df = ranked_df.reset_index(drop=True)

        # Drop the temporary absolute column if no longer needed
        ranked_df = ranked_df.drop(columns=["abs_marginal_contribution_root"])

        return ranked_df

    def get_top_components(self, df: pd.DataFrame, n=4) -> pd.DataFrame:
        """
        Get the top N components from a DataFrame based on 'marginal_contribution_root' ranking.

        :param df: The Dataframe
        :param n: The number of components
        :return the top n rows
        """
        # Making a copy to avoid modifying original DataFrame
        top_components = df.head(n).copy()

        # Round numerical columns
        top_components = top_components.round(self.precision)
        return top_components

    @staticmethod
    def fetch_all_components(response):
        """
        Recursively fetches all components from a nested JSON response.

        :param response: The JSON response containing components in a hierarchical structure.
        :return: List of all components fetched recursively.
        """
        components_list = []

        def recursive_fetch(components):
            """
            Recursively fetch components and append them to the components_list.

            :param components: List of components to fetch recursively.
            """
            nonlocal components_list
            if not components:
                return

            for comp in components:
                components_list.append(comp)
                if comp.get("components"):
                    recursive_fetch(comp["components"])

        # Start recursive fetching from the top level components
        recursive_fetch(response["components"])

        return components_list
