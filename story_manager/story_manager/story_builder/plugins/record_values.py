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
        is_second_rank = False

        # get metric details
        metric = await self.query_service.get_metric(metric_id)

        # find the start and end date for the input time series data
        start_date, end_date = self._get_input_time_range(grain)

        # get time series data
        df = await self._get_time_series_data(metric_id, grain, start_date, end_date, set_index=False)

        # validate time series data has minimum required data points
        time_durations = self.get_time_durations(grain)
        if len(df) < time_durations["min"]:
            logging.warning(
                "Discarding story generation for metric '%s' with grain '%s' due to insufficient data", metric_id, grain
            )
            return []

        # Get the reference value
        ref_data = df.iloc[-1]
        ref_value = ref_data["value"].item()

        record_date = ref_data["date"].strftime(self.date_text_format)

        # Sort the DataFrame in descending order based on value
        sorted_df = df.sort_values(by="value", ascending=False).reset_index(drop=True)
        sorted_df["Rank"] = sorted_df.index + 1
        # Set 'Rank' as the index
        sorted_df.set_index("Rank", inplace=True)

        rank = self.rank_recent_value(sorted_df, ref_value)
        df_len = len(df)

        if rank in [1, 2]:
            if rank == 1:
                prior_rank = rank + 1
            else:
                prior_rank = rank
                is_second_rank = True
            prior_date, prior_value = self.get_prior_values(prior_rank, sorted_df)
            growth = self.analysis_manager.calculate_percentage_difference(ref_value, prior_value)
            story_type = StoryType.RECORD_HIGH
            story_details = self.prepare_story_dict(
                story_type,
                grain=grain,
                metric=metric,
                df=df,
                value=ref_value,
                current_growth=growth,
                prior_value=prior_value,
                prior_date=prior_date,
                duration=len(df),
                is_second_rank=is_second_rank,
                record_date=record_date,
            )
            stories.append(story_details)
        elif rank in [df_len - 1, df_len]:
            if rank == df_len:
                prior_rank = rank - 1
            else:
                prior_rank = rank
                is_second_rank = True
            prior_date, prior_value = self.get_prior_values(prior_rank, sorted_df)
            growth = self.analysis_manager.calculate_percentage_difference(prior_value, ref_value)
            story_type = StoryType.RECORD_LOW
            story_details = self.prepare_story_dict(
                story_type,
                grain=grain,
                metric=metric,
                df=df,
                value=ref_data["value"],
                current_growth=growth,
                prior_value=prior_value,
                prior_date=prior_date,
                duration=len(df),
                is_second_rank=is_second_rank,
                record_date=record_date,
            )
            stories.append(story_details)
        logger.info(f"Record values stories for metric '{metric_id}': {stories}")
        return stories

    def get_prior_values(self, prior_rank: int, sorted_df: pd.DataFrame) -> tuple[str, float]:
        """
        Get the prior value and prior date based on the prior rank.

        :param prior_rank: The rank of the prior value.
        :param sorted_df: The sorted DataFrame.

        :return tuple[str, float]: A tuple containing the prior date, and prior value.
        """
        prior_value = sorted_df.loc[prior_rank]["value"].item()
        prior_date = sorted_df.loc[prior_rank]["date"].strftime(self.date_text_format)
        return prior_date, prior_value

    @staticmethod
    def rank_recent_value(sorted_df: pd.DataFrame, ref_value: float) -> int:
        """
        Determines the rank of the reference value in a DataFrame sorted in descending order.

        If the reference value is in the top two values, the rank of the first occurrence is returned.
        If the reference value is in the bottom two values, the rank of the last occurrence is returned.
        If the reference value is not in the top two or bottom two values, rank of the first occurrence is returned.

        :param sorted_df: The Sorted DataFrame containing the data.
        :param ref_value: The reference value to check the rank of.

        :return Optional[int]: The rank of the reference value, or None if not in the top or bottom range.
        """

        # Determine if the reference value is in the top range or bottom range
        is_in_top_range = ref_value in sorted_df.iloc[:2]["value"].values
        is_in_bottom_range = ref_value in sorted_df.iloc[-2:]["value"].values

        if is_in_top_range:
            # Get the rank of the first occurrence
            rank = sorted_df[sorted_df["value"] == ref_value].index[0]
        elif is_in_bottom_range:
            # Get the rank of the last occurrence
            rank = sorted_df[sorted_df["value"] == ref_value].index[-1]
        else:
            # If not in top or bottom range, just return the rank
            rank = sorted_df[sorted_df["value"] == ref_value].index[0]

        return rank
