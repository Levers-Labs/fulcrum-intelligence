from datetime import datetime
from zoneinfo import ZoneInfo


def convert_datetime_to_utc(dt: datetime) -> str:
    """
    Convert datetime to UTC string
    """
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))

    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")
