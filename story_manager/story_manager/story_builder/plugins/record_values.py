import logging

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class RecordValuesStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.BIG_MOVES
    group = StoryGroup.RECORD_VALUES
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    async def generate_stories(self, metric_id: str, grain: Granularity) -> list[dict]:
        """
        Generate record values stories for the given metric and grain.

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

        # Initialize variables
        stories: list[dict] = []

        # get metric details
        metric = await self.query_service.get_metric(metric_id)

        # find the start and end date for the input time series data
        start_date, end_date = self._get_input_time_range(grain)

        # get time series data
        df = await self._get_time_series_data(metric_id, grain, start_date, end_date, set_index=False)
        df_len = len(df)

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)
        if len(df) < time_durations["min"]:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to insufficient data", metric_id, grain
            )
            return []

        # Get the reference value
        ref_value = df.at[df.index[-1], "value"]
        ref_date = df.at[df.index[-1], "date"]

        # Sort the DataFrame basis value and date in descending and ascending order resp.
        sorted_df = df.sort_values(by=["value", "date"], ascending=[False, True]).reset_index(drop=True)
        sorted_df["rank"] = sorted_df.index + 1
        sorted_df.set_index("rank", inplace=True)

        # Get the rank for the reference date
        rank = self.get_rank_for_date(sorted_df, ref_date)

        # Get prior rank, story type and if its second rank
        prior_rank, story_type, is_second_rank = self.get_prior_rank_and_story_type(df_len, rank)
        if story_type is None:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' as most recent measurement isn't "
                "in top 2 or bottom 2 rank",
                metric_id,
                grain,
            )
            return []

        # Get prior values
        prior_date, prior_value = self.get_prior_values(prior_rank, sorted_df)

        # Calculate deviation % between reference value and prior value
        deviation = self.analysis_manager.calculate_percentage_difference(ref_value, prior_value)

        story_details = self.prepare_story_dict(
            story_type,
            grain=grain,
            metric=metric,
            df=df,
            value=ref_value,
            deviation=abs(deviation),
            prior_value=prior_value,
            prior_date=prior_date,
            duration=df_len,
            is_second_rank=is_second_rank,
            record_date=ref_date.strftime(self.date_text_format),
            rank=rank,
        )
        stories.append(story_details)
        logger.info(f"Record values stories for metric '{metric_id}': {stories}")
        return stories

    @staticmethod
    def get_prior_rank_and_story_type(df_len: int, rank: int) -> tuple[int, StoryType | None, bool]:
        """
        Get the prior rank and story type based on the current rank.

        :param df_len: The length of dataFrame containing the data.
        :param rank: The current rank.

        :return tuple[int, StoryType | None, bool]: A tuple containing the prior rank, story type, and a flag
        indicating if it's the second rank.
        """

        # mapping of ranks with (prior_rank, story_type, is_second_rank flag)
        rank_mappings = {
            1: (2, StoryType.RECORD_HIGH, False),  # For rank 1
            2: (3, StoryType.RECORD_HIGH, True),  # For rank 2
            df_len: (df_len - 1, StoryType.RECORD_LOW, False),  # For last rank
            df_len - 1: (df_len - 2, StoryType.RECORD_LOW, True),  # For second last rank
        }

        prior_rank, story_type, is_second_rank = rank_mappings.get(rank, (rank, None, False))

        return prior_rank, story_type, is_second_rank

    def get_prior_values(self, prior_rank: int, sorted_df: pd.DataFrame) -> tuple[str, float]:
        """
        Get the prior value and prior date based on the prior rank.

        :param prior_rank: The rank of the prior value.
        :param sorted_df: The sorted DataFrame.

        :return tuple[str, float]: A tuple containing the prior date, and prior value.
        """
        prior_value = sorted_df.at[prior_rank, "value"]
        prior_date = sorted_df.at[prior_rank, "date"].strftime(self.date_text_format)
        return prior_date, prior_value

    @staticmethod
    def get_rank_for_date(sorted_df: pd.DataFrame, ref_date: pd.Timestamp) -> int:
        """
        Determines the rank of the reference value in a DataFrame sorted in descending order.

        :param sorted_df: The Sorted DataFrame containing the data.
        :param ref_date: The reference date to check the rank of.

        :return The rank of the reference date
        """
        rank = sorted_df[sorted_df["date"] == pd.to_datetime(ref_date)].index[0]
        return rank
