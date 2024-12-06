import logging
from datetime import datetime
from enum import Enum
from typing import Any

import requests

from .config import *

logger = logging.getLogger(__name__)


class Granularity(str, Enum):
    """
    Defines the genre of the story
    """

    DAY = "day"
    WEEK = "week"
    MONTH = "month"


def fetch_auth_token():
    url = f"{AUTH0_ISSUER.rstrip('/')}/oauth/token"
    headers = {"Content-Type": "application/json"}

    data = {
        "client_id": AUTH0_CLIENT_ID,
        "client_secret": AUTH0_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    response = requests.post(url, headers=headers, json=data, timeout=30)
    response_data = response.json()
    return response_data["access_token"]


def fetch_all_metrics(auth_header: dict[str, str]) -> list[str]:
    response = requests.get(f"{QUERY_MANAGER_SERVER_HOST.strip('/')}/metrics", headers=auth_header, timeout=30)
    response.raise_for_status()
    metrics_data = response.json().get("results", [])

    return [metric["metric_id"] for metric in metrics_data]


def fetch_group_meta(group: str, auth_header: dict[str, str]) -> dict[str, Any]:
    url = f"{STORY_MANAGER_SERVER_HOST.strip('/')}/stories/groups/{group}"
    response = requests.get(url, headers=auth_header, timeout=20)
    response.raise_for_status()
    return response.json()


def filter_grains(grains: list[str], today: datetime) -> list[str]:
    """
    Filter grains based on the current date.

    Args:
        grains (list[str]): List of grains to filter.
        today (datetime): Current date.

    Returns:
        list[str]: Filtered list of grains.
    """
    # Filter grains based on the current date
    return [
        grain
        for grain in grains
        # Include 'week' grain if today is Monday (weekday() == 0)
        if (grain == Granularity.WEEK.value and today.weekday() == 0)
        # Include 'month' grain if today is the first day of the month
        or (grain == Granularity.MONTH.value and today.day == 1)
        # Always include 'day' grain
        or (grain == Granularity.DAY.value)
    ]
