import logging

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_builder import StoryBuilderBase

logger = logging.getLogger(__name__)


class RecordValuesStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.BIG_MOVES
    group = StoryGroup.RECORD_VALUES
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH, Granularity.QUARTER, Granularity.YEAR]

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
        story_type = None
        rank = False

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

        stories: list[dict] = []

        sorted_df = df.sort_values(by="value", ascending=False)

        ref_data = df.iloc[-1]
        record_date = ref_data["date"].strftime(self.date_text_format)

        top_two_rows = sorted_df.head(2)
        bottom_two_rows = sorted_df.tail(2)

        top_index = self.get_value_index(ref_data, top_two_rows)
        bottom_index = self.get_value_index(ref_data, bottom_two_rows)

        if top_index:
            prior_date, prior_value, rank = self.get_rank_and_prior_values(top_two_rows, 1, top_index)
            growth = (
                self.analysis_manager.calculate_percentage_difference(prior_value, ref_data["value"].item())
                if prior_value
                else None
            )
            story_type = StoryType.RECORD_HIGH

        elif bottom_index:
            prior_date, prior_value, rank = self.get_rank_and_prior_values(bottom_two_rows, 0, bottom_index)
            growth = (
                self.analysis_manager.calculate_percentage_difference(prior_value, ref_data["value"].item())
                if prior_value
                else None
            )
            story_type = StoryType.RECORD_LOW

        if story_type:
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
                rank=rank,
                record_date=record_date,
            )
            stories.append(story_details)
        return stories

    def get_rank_and_prior_values(
        self, row: pd.DataFrame, index: int, val_index: list
    ) -> tuple[str | None, int | None, bool]:
        """
        Retrieve the prior date, value, and rank status based on the row index and value index.

        :param row: The DataFrame containing the data.
        :param index: The index of the current row.
        :param val_index: A list of indices to compare with the row index.

        :return: A tuple containing the prior date (str), prior value (int), and rank status (bool).
        """
        if val_index[index - 1] == row.index[index - 1]:
            rank = True
            prior_value = row.iloc[index]["value"]
            prior_date = row.iloc[index]["date"].strftime(self.date_text_format)
            return prior_date, prior_value, rank
        return None, None, False

    @staticmethod
    def get_value_index(ref_data: pd.Series, df: pd.DataFrame) -> list:
        """
        Get the list of indices in the DataFrame where any column matches the reference value.

        :param ref_data: A Series containing the reference value.
        :param df: The DataFrame to search for the reference value.

        :return: A list of indices where the reference value is found.
        """
        return df.index[df.isin([ref_data["value"].item()]).any(axis=1)].tolist()
