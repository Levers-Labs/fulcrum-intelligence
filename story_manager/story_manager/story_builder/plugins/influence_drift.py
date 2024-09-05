import logging
from datetime import date
from typing import Any

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Movement,
    Pressure,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class InfluenceDriftStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.ROOT_CAUSES
    group = StoryGroup.INFLUENCE_DRIFT
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]
    story_movement_map = {
        StoryType.STRONGER_INFLUENCE: Movement.INCREASE.value,
        StoryType.WEAKER_INFLUENCE: Movement.DECREASE.value,
    }
    story_pressure_map = {
        StoryType.IMPROVING_INFLUENCE: Pressure.UPWARD.value,
        StoryType.WORSENING_INFLUENCE: Pressure.DOWNWARD.value,
    }

    async def generate_stories(self, metric_id: str, grain: Granularity) -> list[dict]:
        """
        Generate Influence Drift stories for a given metric and grain.
        Analyze the influence drift to identify changes in the relationship strength between
        influences and the output metric.

        Logic:
        - For each Output Metric, a shape query is issued to find all direct as well as indirect influences.
        - The Influence Drift module is used to re-evaluate the relationship strength between
        the Influences and the Output.
        - For each influence, if the strength has increased, a Stronger Influence story is created.
        If the strength has decreased, a Weaker Influence story is created.
        - After the relationship strength is re-assessed, the change in the aggregate value of the
        influence is multiplied by the strength to determine the marginal contribution.

        :param metric_id: The metric ID for which influence drift stories are generated.
        :param grain: The grain of the time series data.

        :return: A list containing influence drift story dictionaries.
        """
        stories: list = []

        # Fetch metric details
        metric = await self.query_service.get_metric(metric_id)
        logger.debug("Generating Influence Drift stories for metric: %s", metric)

        # Get the time range for the input data based on the specified grain
        start_date, end_date = self._get_input_time_range(grain)

        # Fetch the time series data for the metric within the specified date range and grain
        df = await self.query_service.get_metric_time_series_df(metric_id, start_date, end_date, grain=grain)

        # Fetch the influences for the metric, including indirect influences
        influencers = await self.query_service.get_influencers(metric_id, include_indirect=True)

        # Extract all influencer metric IDs from the nested influencers structure
        influencer_metric_ids = self._extract_influencer_metric_ids(influencers)

        # Prepare input data frames for influences
        input_dfs = []
        for influencer in influencers:
            # Fetch time series data for each influence and its children recursively
            input_dfs.extend(await self.fetch_influence_time_series(influencer, start_date, end_date, grain))

        # Analyze influence drift for the latest data
        latest_influence_drift = self.analysis_manager.influence_drift(
            df=df, input_dfs=input_dfs, target_metric_id=metric_id, influencers=influencers
        )
        logger.info("Influence Drift analysis completed for latest_influence_drift: %s", latest_influence_drift)

        # Adjust the end date to one period before the latest date for previous analysis
        adjusted_df = df.iloc[:-1]
        adjusted_input_dfs = [df.iloc[:-1] for df in input_dfs]

        # Analyze influence drift till one period before the latest date
        previous_influence_drift = self.analysis_manager.influence_drift(
            df=adjusted_df, input_dfs=adjusted_input_dfs, target_metric_id=metric_id, influencers=influencers
        )
        logger.info("Previous Influence Drift analysis completed for metric: %s", previous_influence_drift)

        # Merge all input data frames into a single DataFrame
        merged_input_df = pd.concat(input_dfs, ignore_index=True)

        # Calculate the deviation in the output metric for the latest and previous periods
        output_deviation, prev_output_deviation = self._calculate_output_deviation(df)

        # Iterate over the components of the latest and previous influence drift analysis
        for latest, previous in zip(latest_influence_drift["components"], previous_influence_drift["components"]):
            influence_metric_id = latest["metric_id"]

            # Skip if the influence metric ID is not in the list of influencer metric IDs
            if influence_metric_id not in influencer_metric_ids:
                continue

            # Filter the DataFrame for the current influence metric
            input_df = merged_input_df[merged_input_df["metric_id"] == influence_metric_id]

            # Log a warning if no data is found for the influence metric
            if input_df.empty:
                logger.warning(f"No data found for influence metric: {influence_metric_id}")
                continue

            # Adjust the input DataFrame to exclude the latest period
            adjusted_input_df = input_df.iloc[:-1]

            # Get the strength values for the latest and previous periods
            latest_strength, previous_strength = self._get_strength_values(latest, previous)

            # Calculate the change in influence for the current period
            influence_change = self._calculate_influence_change(input_df, adjusted_input_df)

            # Fetch the metric details for the influence metric
            influence_metric = await self.query_service.get_metric(influence_metric_id)

            # Create influence stories based on the calculated values and append to the stories list
            stories.extend(
                await self._create_influence_stories(
                    grain,
                    metric,
                    input_df,
                    latest_strength,
                    previous_strength,
                    influence_change,
                    output_deviation,
                    prev_output_deviation,
                    influence_metric,
                )
            )

        return stories

    def _extract_influencer_metric_ids(self, influencers: list[dict]) -> set[str]:
        """
        Recursively extract all metric IDs from the nested influencers structure.

        :param influencers: List of influencer dictionaries
        :return: Set of all metric IDs
        """
        metric_ids = set()
        for influencer in influencers:
            metric_ids.add(influencer["metric_id"])
            if "influencers" in influencer:
                metric_ids.update(self._extract_influencer_metric_ids(influencer["influencers"]))
        return metric_ids

    async def fetch_influence_time_series(
        self, influencer: dict[str, Any], start_date: date, end_date: date, grain: Granularity
    ) -> list[pd.DataFrame]:
        """
        Fetches time series data for a given influence and its children recursively.

        :param influencer:
        :param start_date: The start date of the time series data.
        :param end_date: The end date of the time series data.
        :param grain: The granularity of the time series data.

        :return: A list of pandas DataFrames containing the time series data for the influence and its children.
        """
        # Initialize an empty list to hold the input data frames
        input_dfs = []
        # Fetch the time series data for the current influencer and append it to the list
        input_dfs.append(
            await self.query_service.get_metric_time_series_df(
                influencer["metric_id"], start_date, end_date, grain=grain
            )
        )
        # For each child influencer, fetch its time series data and extend the list
        for child_influencer in influencer.get("influencers", []):
            input_dfs.extend(await self.fetch_influence_time_series(child_influencer, start_date, end_date, grain))
        # Return the list of input data frames
        return input_dfs

    def _calculate_output_deviation(self, df: pd.DataFrame):
        """
        Calculate the output deviation and previous output deviation for a given DataFrame.

        This method calculates the percentage difference between the latest value and the previous value,
        as well as the percentage difference between the previous value and the value before it.

        :param df: A pandas DataFrame containing the time series data with a 'value' column.
        :return: A tuple containing the output deviation and the previous output deviation.
        """
        # Get the latest, previous, and value before the previous value from the DataFrame
        latest_value, previous_value, prev_to_prev_value = (
            df["value"].iloc[-1],
            df["value"].iloc[-2],
            df["value"].iloc[-3],
        )

        # Calculate the percentage difference between the latest value and the previous value
        output_deviation = self.analysis_manager.calculate_percentage_difference(latest_value, previous_value)
        # Calculate the percentage difference between the previous value and the value before it
        prev_output_deviation = self.analysis_manager.calculate_percentage_difference(
            previous_value, prev_to_prev_value
        )

        # Return the calculated output deviation and previous output deviation
        return output_deviation, prev_output_deviation

    def _get_strength_values(self, latest: dict, previous: dict) -> tuple[float, float]:
        """
        Extract the relative impact values from the latest and previous model dictionaries.

        This method retrieves the 'relative_impact' values from the 'model' key in both the latest and previous
        dictionaries. These values represent the strength of the influence in the respective time periods.

        :param latest: A dictionary containing the latest model data with a 'relative_impact' key.
        :param previous: A dictionary containing the previous model data with a 'relative_impact' key.
        :return: A tuple containing the latest and previous relative impact values as floats.
        """
        # Extract the relative impact value from the latest and previous model dictionaries
        return latest["model"]["relative_impact"], previous["model"]["relative_impact"]

    def _calculate_influence_change(self, input_df: pd.DataFrame, adjusted_input_df: pd.DataFrame) -> float:
        """
        Calculate the percentage change in influence between the input and adjusted input data frames.

        This method calculates the percentage difference between the latest value in the input data frame
        and the latest value in the adjusted input data frame. This percentage difference represents the
        change in influence over the specified time period.

        :param input_df: A pandas DataFrame containing the original time series data for the influence.
        :param adjusted_input_df: A pandas DataFrame containing the adjusted time series data for the influence.
        :return: The percentage change in influence as a float.
        """
        # Get the latest value from the input data frame and the adjusted input data frame
        latest_value, previous_value = input_df["value"].iloc[-1], adjusted_input_df["value"].iloc[-1]
        # Calculate and return the percentage difference between the latest and previous values
        return self.analysis_manager.calculate_percentage_difference(latest_value, previous_value)

    async def _create_influence_stories(
        self,
        grain: Granularity,
        metric: dict,
        input_df: pd.DataFrame,
        latest_strength: float,
        previous_strength: float,
        influence_change: float,
        output_deviation: float,
        prev_output_deviation: float,
        influence_metric: dict,
    ) -> list[dict]:
        """
        Create influence stories based on the provided metrics and analysis results.

        This method generates stories that describe the changes in influence relationships and their impact on
        the output metric.
        It creates two types of stories:
        1. Stronger/Weaker Influence Relationship story: Indicates whether the influence relationship has become
        stronger or weaker.
        2. Improving/Worsening Influence Metric story: Indicates whether the influence metric has improved or
        worsened based on the marginal contribution.

        :param metric: A dictionary containing the metric details.
        :param input_df: A pandas DataFrame containing the time series data for the metric.
        :param latest_strength: The latest relative impact value of the influence.
        :param previous_strength: The previous relative impact value of the influence.
        :param influence_change: The percentage change in influence.
        :param output_deviation: The percentage deviation of the output metric.
        :param prev_output_deviation: The percentage deviation of the output metric in the previous period.
        :param influence_metric: A dictionary containing the influence metric details.
        :return: A list of dictionaries representing the generated influence stories.
        """
        stories = []

        # Determine the type of influence relationship story based on the strength comparison
        story_type = (
            StoryType.STRONGER_INFLUENCE
            if abs(latest_strength) > abs(previous_strength)
            else StoryType.WEAKER_INFLUENCE
        )
        # Create and append the Stronger/Weaker Influence Relationship story
        stories.append(
            self._create_story(
                story_type,
                grain,
                metric,
                input_df,
                influence_metric,
                metric,
                influence_change,
                output_deviation,
                prev_output_deviation,
                latest_strength,
                previous_strength,
            )
        )

        # Calculate the marginal contribution by multiplying the influence change with the latest strength
        marginal_contribution = influence_change * latest_strength
        # Determine the type of influence metric story based on the marginal contribution
        story_type = StoryType.IMPROVING_INFLUENCE if marginal_contribution > 0 else StoryType.WORSENING_INFLUENCE
        # Create and append the Improving/Worsening Influence Metric story
        stories.append(
            self._create_story(
                story_type,
                grain,
                metric,
                input_df,
                influence_metric,
                metric,
                influence_change,
                output_deviation,
                prev_output_deviation,
                latest_strength,
                previous_strength,
            )
        )

        return stories

    def _create_story(
        self,
        story_type: StoryType,
        grain: Granularity,
        metric: dict,
        df: pd.DataFrame,
        influence_metric: dict,
        output_metric: dict,
        influence_deviation: float,
        output_deviation: float,
        prev_output_deviation: float,
        latest_strength: float,
        previous_strength: float,
    ) -> dict:
        """
        Create a story dictionary based on the provided parameters.

        This method prepares a story dictionary that describes the changes in influence relationships and their impact
        on the output metric.
        It uses the provided parameters to populate the story details, including the type of story, metric details,
        deviations, and strengths.

        :param story_type: The type of the story to be created (e.g., Stronger Influence, Weaker Influence, Improving
        Influence, Worsening Influence).
        :param metric: A dictionary containing the metric details.
        :param df: A pandas DataFrame containing the time series data for the metric.
        :param influence_metric: A dictionary containing the influence metric details.
        :param output_metric: A dictionary containing the output metric details.
        :param influence_deviation: The percentage deviation of the influence metric.
        :param output_deviation: The percentage deviation of the output metric.
        :param prev_output_deviation: The percentage deviation of the output metric in the previous period.
        :param latest_strength: The latest relative impact value of the influence.
        :param previous_strength: The previous relative impact value of the influence.
        :return: A dictionary representing the generated story.
        """
        # Prepare the story dictionary using the provided parameters
        return self.prepare_story_dict(
            story_type,
            grain=grain,
            metric=metric,
            df=df,
            current_value=df["value"].iloc[-1],
            influence_metric=influence_metric,
            output_metric=output_metric,
            influence_deviation=influence_deviation,
            movement=self.story_movement_map.get(story_type, None),
            pressure=self.story_pressure_map.get(story_type, None),
            prev_output_deviation=prev_output_deviation,
            output_deviation=output_deviation,
            latest_strength=latest_strength,
            previous_strength=previous_strength,
        )
