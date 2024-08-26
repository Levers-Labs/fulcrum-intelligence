import logging
from datetime import date
from typing import Any

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class InfluenceDriftStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.ROOT_CAUSES
    group = StoryGroup.INFLUENCE_DRIFT
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

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

        # Get the time range for the input data
        start_date, end_date = self._get_input_time_range(grain)

        # Fetch the time series data for the metric
        df = await self.query_service.get_metric_time_series_df(metric_id, start_date, end_date, grain=grain)

        # Fetch the influences for the metric
        influencers = await self.query_service.get_influencers(metric_id, include_indirect=True)

        # Prepare input data frames for influences
        input_dfs = []
        for influencer in influencers:
            input_dfs.extend(await self.fetch_influence_time_series(influencer, start_date, end_date, grain))

        # Analyze influence drift
        latest_influence_drift = self.analysis_manager.influence_drift(
            df=df, input_dfs=input_dfs, target_metric_id=metric_id, influencers=influencers
        )
        logger.info("Influence Drift analysis completed for latest_influence_drift: %s", latest_influence_drift)

        # Adjust the end date to one period before the latest date
        adjusted_df = df.iloc[:-1]
        adjusted_input_dfs = [df.iloc[:-1] for df in input_dfs]

        # Analyze influence drift till one period before the latest date
        previous_influence_drift = self.analysis_manager.influence_drift(
            df=adjusted_df, input_dfs=adjusted_input_dfs, target_metric_id=metric_id, influencers=influencers
        )
        logger.info("Previous Influence Drift analysis completed for metric: %s", previous_influence_drift)

        return stories

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
