from datetime import datetime
from zoneinfo import ZoneInfo

from pydantic import BaseModel as _BaseModel, ConfigDict


def convert_datetime_to_utc(dt: datetime) -> str:
    """
    Convert datetime to UTC string
    """
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))

    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


class BaseModel(_BaseModel):
    """
    Custom Base Model for pydantic models
    """

    model_config = ConfigDict(json_encoders={datetime: convert_datetime_to_utc}, populate_by_name=True)
