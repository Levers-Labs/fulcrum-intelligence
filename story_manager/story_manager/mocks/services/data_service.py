from datetime import date, timedelta

import pandas as pd

from commons.models.enums import Granularity
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class MockDataService:
    """Service to provide date utilities for mock generators"""

    def __init__(self, story_date: date | None = None):
        self.story_date = story_date or date.today()
        self.date_format = "%B %d, %Y"  # e.g., "January 01, 2023"

    def _get_current_period_range(self, grain: Granularity) -> tuple[date, date]:
        """
        Get the end date of the last period based on the grain.
        Based on the current date, the end date is calculated as follows:
        - For day grain: yesterday
        - For week grain: the last Sunday
        - For month grain: the last day of the previous month
        - For quarter grain: the last day of the previous quarter
        - For year grain: December 31 of the previous year
        For each grain, the start date of the period is calculated based on the end date.

        :param grain: The grain for which the end date is retrieved.
        :return: The start and end date of the period.
        """
        today = self.story_date
        if grain == Granularity.DAY:
            end_date = today - timedelta(days=1)
            start_date = end_date
        elif grain == Granularity.WEEK:
            end_date = today - timedelta(days=today.weekday() + 1)
            start_date = end_date - timedelta(days=6)
        elif grain == Granularity.MONTH:
            end_date = date(today.year, today.month, 1) - timedelta(days=1)
            start_date = date(end_date.year, end_date.month, 1)
        elif grain == Granularity.QUARTER:
            quarter_end_month = (today.month - 1) // 3 * 3
            end_date = date(today.year, quarter_end_month + 1, 1) - timedelta(days=1)
            start_date = date(end_date.year, end_date.month - 2, 1)
        elif grain == Granularity.YEAR:
            end_date = date(today.year - 1, 12, 31)
            start_date = date(end_date.year, 1, 1)
        else:
            raise ValueError(f"Unsupported grain: {grain}")
        return start_date, end_date

    def get_input_time_range(self, grain: Granularity, group) -> tuple[date, date]:
        """
        Get the time range for the input data based on the grain.

        :param grain: The grain for which the time range is retrieved.
        :param group: group of story
        :return: The start and end date of the time range.
        """
        periods = STORY_GROUP_TIME_DURATIONS[group][grain]["input"]
        latest_start_date, latest_end_date = self._get_current_period_range(grain)

        # figure out relevant grain delta .e.g weeks : 1
        delta_eq = GRAIN_META[grain]["delta"]

        # Go back by the determined grain delta
        delta = pd.DateOffset(**delta_eq)
        start_date = (latest_start_date - periods * delta).date()

        return start_date, latest_end_date

    def get_dates_for_range(self, grain: Granularity, start_date: date, end_date: date) -> list[date]:
        """
        Generate a list of dates between start_date and end_date based on grain

        :param grain: The granularity (day, week, month, etc.)
        :param start_date: The start date
        :param end_date: The end date
        :return: List of dates
        """
        dates = []
        current_date = start_date

        # Get the delta for the grain
        delta_eq = GRAIN_META[grain]["delta"]
        delta = pd.DateOffset(**delta_eq)

        # Generate dates
        while current_date <= end_date:
            dates.append(current_date)
            current_date = (current_date + delta).date()  # type: ignore

        return dates

    def get_formatted_dates(self, dates: list[date]) -> list[str]:
        """Format a list of dates according to the standard format"""
        return [d.strftime(self.date_format) for d in dates]
