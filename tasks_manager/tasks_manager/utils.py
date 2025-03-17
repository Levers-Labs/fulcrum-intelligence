from datetime import date, timedelta

import pandas as pd

from commons.clients.auth import ClientCredsAuth
from commons.models.enums import Granularity
from tasks_manager.config import AppConfig


def get_eligible_grains(grains: list[Granularity], today: date) -> list[str]:
    """
    filter grains based on the current date.

    Args:
        grains (list[Granularity]): List of grains to filter.
        today (date): Current date.

    Returns:
        list[str]: Filtered list of grains.
    """
    # Filter grains based on the current date
    return [
        grain
        for grain in grains
        # Include 'week' grain if today is Monday (weekday() == 0)
        if (grain == Granularity.WEEK and today.weekday() == 0)
        # Include 'month' grain if today is the first day of the month
        or (grain == Granularity.MONTH and today.day == 1)
        # Always include 'day' grain
        or (grain == Granularity.DAY)
    ]


def get_client_auth_from_config(config: AppConfig) -> ClientCredsAuth:
    return ClientCredsAuth(
        auth0_issuer=config.auth0_issuer,
        client_id=config.auth0_client_id,
        client_secret=config.auth0_client_secret.get_secret_value(),
        api_audience=config.auth0_api_audience,
    )


def should_update_grain(check_date: date, grain: Granularity) -> bool:
    """
    Determine if a grain should be updated on the given date

    :param check_date: The date to check
    :param grain: The granularity to check
    :return: True if the grain should be updated, False otherwise
    """
    if grain == Granularity.DAY:
        # Update daily grain every day
        return True
    elif grain == Granularity.WEEK:
        # Update weekly grain only on Mondays (weekday 0)
        return check_date.weekday() == 0
    elif grain == Granularity.MONTH:
        # Update monthly grain only on 1st of month
        return check_date.day == 1
    elif grain == Granularity.QUARTER:
        # Update quarterly grain only on first day of quarter (Jan 1, Apr 1, Jul 1, Oct 1)
        return check_date.day == 1 and check_date.month in [1, 4, 7, 10]
    elif grain == Granularity.YEAR:
        # Update yearly grain only on Jan 1
        return check_date.day == 1 and check_date.month == 1

    raise ValueError(f"Unsupported grain: {grain}")


def increment_date_by_grain(current_date: date, grain: Granularity) -> date:
    """
    Increment a date by one grain unit

    :param current_date: The date to increment
    :param grain: The granularity unit to increment by
    :return: The incremented date
    """
    if grain == Granularity.DAY:
        return current_date + timedelta(days=1)
    elif grain == Granularity.WEEK:
        return current_date + timedelta(days=7)
    elif grain == Granularity.MONTH:
        # Use pandas DateOffset for accurate month incrementation
        new_date = pd.Timestamp(current_date) + pd.DateOffset(months=1)
        return new_date.date()
    elif grain == Granularity.QUARTER:
        new_date = pd.Timestamp(current_date) + pd.DateOffset(months=3)
        return new_date.date()
    elif grain == Granularity.YEAR:
        new_date = pd.Timestamp(current_date) + pd.DateOffset(years=1)
        return new_date.date()
    else:
        raise ValueError(f"Unsupported grain: {grain}")
