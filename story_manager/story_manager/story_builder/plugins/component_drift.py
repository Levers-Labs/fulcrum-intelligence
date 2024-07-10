import logging

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class ComponentDriftStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.ROOT_CAUSES
    group = StoryGroup.COMPONENT_DRIFT
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]
    min_component_count = 4

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

        # metric = await self.query_service.get_metric(metric_id)

        evaluation_start_date, evaluation_end_date = self._get_input_time_range(
            grain,  # type: ignore
        )

        # Need to calc comparison date w.r.t the evaluation date
        self.story_date = evaluation_start_date
        comparison_start_date, comparison_end_date = self._get_input_time_range(
            grain,  # type: ignore
        )
        resp = await self.analysis_service.get_component_drift(
            metric_id,
            evaluation_start_date,
            evaluation_end_date,
            comparison_start_date,
            comparison_end_date,
        )

        df = pd.DataFrame(resp)

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)  # type: ignore
        if len(df) < time_durations["min"]:
            logger.warning(
                "Discarding story generation for metric '%s' with grain '%s'" "due to in insufficient data.",
                metric_id,
                grain,
            )
            return []

        # rank the by components
        # ranked_df = self.rank_components(df)

        logger.info(f"Component Drift Stories Execution Complete for metric '{metric_id}'")
        return stories

    @staticmethod
    def rank_components(df: pd.DataFrame) -> pd.DataFrame:
        return df
