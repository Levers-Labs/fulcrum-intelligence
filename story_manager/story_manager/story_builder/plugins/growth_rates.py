import logging
from datetime import date
from typing import Any

import pandas as pd

from commons.models.enums import Granularity, GranularityOrder
from story_manager.core.enums import (
    STORY_TYPES_META,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.story_builder import StoryBuilderBase
from story_manager.story_builder.constants import GRAIN_META

logger = logging.getLogger(__name__)


class GrowthStoryBuilder(StoryBuilderBase):
    genre = StoryGenre.GROWTH  # type: ignore
    group = StoryGroup.GROWTH_RATES  # type: ignore
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH, Granularity.QUARTER]

    async def generate_stories(self, metric_id: str, grain: Granularity) -> list[dict[str, Any]]:
        """
        Generate growth stories for the given metric and grain.

        The logic for generating growth stories is as follows:
        1. Figure out the end date of the last period based on the grain.
        2. For each reference period (week, month, quarter):
            A. Calculate the period difference between the current grain and the reference period.
            B. Get the time series data for the given metric and grain.
            C. Calculate growth rates for each data point in the time series.
            D. Retrieve current and reference growth rates from the growth rates DataFrame.
            E. Check discard conditions.
            F. Determine the story type based on whether the current growth rate is greater
           than the reference growth rate.
           G. Render the story text using the story type and relevant variables.

        Example output:
        [
            {
                "metric_id": "cac",
                "genre": "GROWTH",
                "group": "GROWTH_RATES",
                "type": "ACCELERATING_GROWTH",
                "grain": "day",
                "text": "The d/d growth rate for CAC has accelerated over the past prior week,
                growing from 8% d/d growth to 10% now.",
                "variables": {
                    "grain": "day",
                    "reference_period": "week",
                    "current_growth": 10.0,
                    "reference_growth": 8.0
                },
                "series": [
                    {"date": "2023-04-10", "value": 100, "growth_rate": 8.0},
                    {"date": "2023-04-11", "value": 110, "growth_rate": 9.0},
                    ...
                ]
            },
            ...
        ]

        :param metric_id: The metric ID for which growth stories are generated
        :param grain: The grain for which growth stories are generated
        :return: A list of generated growth stories
        """
        reference_periods = [Granularity.WEEK, Granularity.MONTH, Granularity.QUARTER]
        metric = await self.query_service.get_metric(metric_id)
        stories: list[dict[str, Any]] = []
        # Get the end date of the last period based on the grain
        curr_start_date, curr_end_date = self._get_current_period_range(grain)

        # compare the current growth rate with the reference growth rate
        # by iterating over the reference periods (week, month, quarter)
        for reference_period in reference_periods:
            # continue if the reference period is greater than the current grain
            if GranularityOrder[reference_period.name].value < GranularityOrder[grain.name].value:
                continue

            # Get the start date of the period based on the end date, reference period, and grain
            start_date = self._get_sliding_start_date(curr_start_date, reference_period, grain)

            # Retrieve time series data for the given metric and grain
            series_df = await self._get_time_series_data(metric_id, grain, start_date, curr_end_date)

            # Interpolate missing values in the time series data
            series_df = series_df.interpolate()

            # calculate growth rates
            series_df = self.analysis_manager.calculate_growth_rates_of_series(series_df)

            if series_df.empty:
                logger.warning(
                    "Skipping story generation for metric '%s' with grain '%s' and reference period '%s' due to "
                    "insufficient data points.",
                    metric_id,
                    grain,
                    reference_period,
                )
                continue

            # Retrieve the reference growth rate from the growth rates DataFrame based on the period difference
            reference_growth = series_df.iloc[0]["growth_rate"]
            # Retrieve the current growth rate from the last row of the growth rates DataFrame
            current_growth = series_df.iloc[-1]["growth_rate"]

            # Check if either the reference growth rate or current growth rate is missing
            if pd.isna(reference_growth) or pd.isna(current_growth):
                logger.warning(
                    "Discarding story generation for metric '%s' with grain '%s' and reference period '%s' "
                    "due to missing growth rates.",
                    metric_id,
                    grain,
                    reference_period,
                )
                continue

            # Check if the reference growth rate is equal to the current growth rate
            if reference_growth == current_growth:
                logger.warning(
                    "Discarding story generation for metric '%s' with grain '%s' and reference period '%s' "
                    "due to equal growth rates.",
                    metric_id,
                    grain,
                    reference_period,
                )
                continue

            story_type = (
                StoryType.ACCELERATING_GROWTH if current_growth > reference_growth else StoryType.SLOWING_GROWTH
            )
            story_meta = STORY_TYPES_META[story_type]
            story_title = ""
            story_detail = ""
            story = {
                "metric_id": metric_id,
                "genre": self.genre,  # type: ignore
                "group": self.group,  # type: ignore
                "type": story_type,
                "grain": grain.value,
                "title": story_title,
                "title_template": story_meta["title"],
                "detail": story_detail,
                "detail_template": story_meta["detail"],
                "variables": {
                    "grain": grain.value,
                    "reference_period": reference_period.value,
                    "current_growth": current_growth,
                    "reference_growth": reference_growth,
                    "metric_id": metric_id,
                    "metric": {"label": metric["label"]},
                },
                "series": series_df.reset_index().astype({"date": str}).to_dict(orient="records"),
            }
            stories.append(story)

        return stories

    def _get_sliding_start_date(self, curr_start_date: date, reference_period: Granularity, grain: Granularity) -> date:
        """
        Get the start date of the period based on the end date, reference period, and grain.

        :param curr_start_date: The end date of the last period.
        :param reference_period: The reference period for which the start date is retrieved.
        :param grain: The grain for which the start date is aligned.
        :return: The start date of the period.
        """
        start_date = curr_start_date

        # Go back by the reference period
        ref_delta = pd.DateOffset(**GRAIN_META[reference_period]["default"])
        start_date -= ref_delta

        # Go back by the grain period
        grain_delta = pd.DateOffset(**GRAIN_META[grain]["default"])
        start_date -= grain_delta

        return start_date.date() if isinstance(start_date, pd.Timestamp) else start_date
