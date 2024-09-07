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
        df_len = len(df)

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)
        if df_len < time_durations["min"]:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to insufficient data", metric_id, grain
            )
            return []

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
        merged_input_df = pd.concat(input_dfs, ignore_index=True)  # noqa

        # Calculate the deviation in the output metric for the latest and previous periods
        output_deviation, prev_output_deviation = self._calculate_output_deviation(df)

        # Iterate over the components of the latest and previous influence drift analysis
        for latest, previous in zip(
            latest_influence_drift["components"], previous_influence_drift["components"]
        ):  # noqa
            influence_metric_id = latest["metric_id"]  # type: ignore

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
            latest_strength, previous_strength = self._get_strength_values(latest, previous)  # type: ignore

            # Calculate the change in influence for the current period
            influence_deviation = self._calculate_influence_deviation(input_df, adjusted_input_df)

            # Fetch the metric details for the influence metric
            influence_metric = await self.query_service.get_metric(influence_metric_id)

            # Determine the type of influence relationship story based on the strength comparison
            story_type = (
                StoryType.STRONGER_INFLUENCE
                if abs(latest_strength) > abs(previous_strength)
                else StoryType.WEAKER_INFLUENCE
            )
            # Create and append the Stronger/Weaker Influence Relationship story
            stories.append(
                self.prepare_story_dict(
                    story_type,
                    grain=grain,
                    metric=metric,
                    df=df,
                    current_value=df["value"].iloc[-1],
                    influence_metric=influence_metric,
                    output_metric=metric,
                    influence_deviation=influence_deviation,
                    movement=self.story_movement_map.get(story_type, None),
                    pressure=self.story_pressure_map.get(story_type, None),
                    prev_output_deviation=prev_output_deviation,
                    output_deviation=output_deviation,
                    latest_strength=latest_strength,
                    previous_strength=previous_strength,
                )
            )

            # Calculate the marginal contribution by multiplying the influence change with the latest strength
            marginal_contribution = influence_deviation * latest_strength
            # Determine the type of influence metric story based on the marginal contribution
            story_type = StoryType.IMPROVING_INFLUENCE if marginal_contribution > 0 else StoryType.WORSENING_INFLUENCE
            # Create and append the Improving/Worsening Influence Metric story
            stories.append(
                self.prepare_story_dict(
                    story_type,
                    grain=grain,
                    metric=metric,
                    df=df,
                    current_value=df["value"].iloc[-1],
                    influence_metric=influence_metric,
                    output_metric=metric,
                    influence_deviation=influence_deviation,
                    movement=self.story_movement_map.get(story_type, None),
                    pressure=self.story_pressure_map.get(story_type, None),
                    prev_output_deviation=prev_output_deviation,
                    output_deviation=output_deviation,
                    latest_strength=latest_strength,
                    previous_strength=previous_strength,
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

    @staticmethod
    def _get_strength_values(latest: dict, previous: dict) -> tuple[float, float]:
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

    def _calculate_influence_deviation(self, input_df: pd.DataFrame, adjusted_input_df: pd.DataFrame) -> float:
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
