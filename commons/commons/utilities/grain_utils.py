from datetime import date, timedelta
from typing import Any

import pandas as pd

from commons.models.enums import Granularity

GRAIN_META: dict[str, Any] = {
    Granularity.DAY: {"pop": "d/d", "label": "day", "delta": {"days": 1}, "eoi": "EOD", "interval": "daily"},
    Granularity.WEEK: {"pop": "w/w", "label": "week", "delta": {"weeks": 1}, "eoi": "EOW", "interval": "weekly"},
    Granularity.MONTH: {"pop": "m/m", "label": "month", "delta": {"months": 1}, "eoi": "EOM", "interval": "monthly"},
    Granularity.QUARTER: {
        "pop": "q/q",
        "label": "quarter",
        "delta": {"months": 3},
        "eoi": "EOQ",
        "interval": "quarterly",
    },
    Granularity.YEAR: {"pop": "y/y", "label": "year", "delta": {"years": 1}, "eoi": "EOY", "interval": "yearly"},
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

    @staticmethod
    def get_prev_period_start_date(grain: Granularity, period_count: int, latest_start_date: date) -> date:
        """
        Calculate the start date of a period that is a specified number of periods before the latest start date.

        This method determines the start date of a period that is a specified number of periods before the
        latest start date, based on the given granularity. It uses the granularity metadata to determine the delta
        (e.g., weeks: 1) and then applies this delta to the latest start date to calculate the start date of the
        previous period.

        :param grain: The granularity of the period (e.g., day, week, month, etc.).
        :param period_count: The number of periods to go back from the latest start date.
        :param latest_start_date: The start date of the latest period.
        :return: The start date of the period that is `period_count` periods before the `latest_start_date`.
        """
        # Retrieve the delta for the specified grain from the GRAIN_META dictionary.
        delta_eq = GRAIN_META[grain]["delta"]

        # Convert the delta dictionary into a pandas DateOffset object.
        delta = pd.DateOffset(**delta_eq)

        # Calculate the start date of the period that is `period_count` periods before the `latest_start_date`.
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

    @staticmethod
    def generate_date_label(grain: Granularity, date: date) -> str:
        if grain == Granularity.DAY:
            return date.strftime("%b %d, %Y")  # Example: "Mar 26, 2025"
        elif grain == Granularity.WEEK:
            # Returns a week label, like: "Week of Mar 26, 2025"
            return "Week of " + date.strftime("%b %d, %Y")
        elif grain == Granularity.MONTH:
            return date.strftime("%b, %Y")  # Example: "Mar, 2025"
        elif grain == Granularity.QUARTER:
            # Returns the quarter and year, like: "Q1, 2025"
            quarter = (date.month - 1) // 3 + 1
            return f"Q{quarter}, {date.year}"
        elif grain == Granularity.YEAR:
            return str(date.year)  # Example: "2025"
        else:
            raise ValueError(f"Unsupported grain: {grain}")
