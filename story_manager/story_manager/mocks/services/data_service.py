from datetime import date

from commons.models.enums import Granularity
from commons.utilities.grain_utils import GrainPeriodCalculator
from story_manager.story_builder.constants import STORY_GROUP_TIME_DURATIONS


class MockDataService:
    """Service to provide date utilities for mock generators"""

    def __init__(self, story_date: date | None = None):
        self.story_date = story_date or date.today()
        self.date_format = "%B %d, %Y"  # e.g., "January 01, 2023"

    def get_input_time_range(self, grain: Granularity, group) -> tuple[date, date]:
        """
        Get the time range for the input data based on the grain.

        :param grain: The grain for which the time range is retrieved.
        :param group: group of story
        :return: The start and end date of the time range.
        """
        period_count = STORY_GROUP_TIME_DURATIONS[group][grain]["input"]
        latest_start_date, latest_end_date = GrainPeriodCalculator.get_current_period_range(grain, self.story_date)
        start_date = GrainPeriodCalculator.get_prev_period_start_date(grain, period_count, latest_start_date)
        return start_date, latest_end_date

    def get_formatted_dates(self, grain: Granularity, start_date: date, end_date: date) -> list[str]:
        """Format a list of dates according to the standard format"""
        dates = GrainPeriodCalculator.get_dates_for_range(grain, start_date, end_date)
        return [d.strftime(self.date_format) for d in dates]
