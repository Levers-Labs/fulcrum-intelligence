from datetime import date

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
