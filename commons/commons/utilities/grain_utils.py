from datetime import date, timedelta
from typing import Any

import pandas as pd

from commons.models.enums import Granularity

GRAIN_META: dict[str, Any] = {
    Granularity.DAY: {"pop": "d/d", "delta": {"days": 1}, "eoi": "EOD", "interval": "daily"},
    Granularity.WEEK: {"pop": "w/w", "delta": {"weeks": 1}, "eoi": "EOW", "interval": "weekly"},
    Granularity.MONTH: {"pop": "m/m", "delta": {"months": 1}, "eoi": "EOM", "interval": "monthly"},
    Granularity.QUARTER: {"pop": "q/q", "delta": {"months": 3}, "eoi": "EOQ", "interval": "quarterly"},
    Granularity.YEAR: {"pop": "y/y", "delta": {"years": 1}, "eoi": "EOY", "interval": "yearly"},
}


class GrainPeriodCalculator:

    @staticmethod
    def get_current_period_range(grain: Granularity, current_date) -> tuple[date, date]:
        """
        Get the end date of the last period based on the grain.
        Based on the current date, the end date is calculated as follows:
        - For day grain: yesterday
        - For week grain: the last Sunday
        - For month grain: the last day of the previous month
        - For quarter grain: the last day of the previous quarter
        - For year grain: December 31 of the previous year
        For each grain, the start date of the period is calculated based on the end date.

        :param current_date:
        :param grain: The grain for which the end date is retrieved.
        :return: The start and end date of the period.
        """
        if grain == Granularity.DAY:
            end_date = current_date - timedelta(days=1)
            start_date = end_date
        elif grain == Granularity.WEEK:
            end_date = current_date - timedelta(days=current_date.weekday() + 1)
            start_date = end_date - timedelta(days=6)
        elif grain == Granularity.MONTH:
            end_date = date(current_date.year, current_date.month, 1) - timedelta(days=1)
            start_date = date(end_date.year, end_date.month, 1)
        elif grain == Granularity.QUARTER:
            quarter_end_month = (current_date.month - 1) // 3 * 3
            end_date = date(current_date.year, quarter_end_month + 1, 1) - timedelta(days=1)
            start_date = date(end_date.year, end_date.month - 2, 1)
        elif grain == Granularity.YEAR:
            end_date = date(current_date.year - 1, 12, 31)
            start_date = date(end_date.year, 1, 1)
        else:
            raise ValueError(f"Unsupported grain: {grain}")
        return start_date, end_date

    def get_period_dates(
        self, grain: Granularity, include_previous: bool = False
    ) -> tuple[date, date] | tuple[date, date, date, date]:

        current_start, current_end = self.get_current_period_range(grain, date.today())

        if include_previous:
            previous_start, previous_end = self.get_current_period_range(grain, current_start)
            return current_start, current_end, previous_start, previous_end

        return current_start, current_end

    @staticmethod
    def get_period_start_date(grain: Granularity, period_count: int, latest_start_date: date) -> date:
        # figure out relevant grain delta .e.g weeks : 1
        delta_eq = GRAIN_META[grain]["delta"]

        # Go back by the determined grain delta
        delta = pd.DateOffset(**delta_eq)
        start_date = (latest_start_date - period_count * delta).date()

        return start_date

    @staticmethod
    def get_dates_for_range(grain: Granularity, start_date: date, end_date: date) -> list[date]:
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
