from datetime import datetime
from zoneinfo import ZoneInfo

from pydantic import BaseModel as _BaseModel, ConfigDict
from sqlalchemy import text
from sqlmodel import Field, SQLModel


def convert_datetime_to_utc(dt: datetime) -> str:
    """
    Convert datetime to UTC string
    """
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))

    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


class BaseModel(_BaseModel):
    """
    Custom base class for pydantic models
    """

    model_config = ConfigDict(json_encoders={datetime: convert_datetime_to_utc}, populate_by_name=True)


class CustomSQLBase(SQLModel):
    """
    Custom base class for sqlmodel models
    """

    model_config = ConfigDict(  # type: ignore
        json_encoders={datetime: convert_datetime_to_utc},
        populate_by_name=True,
    )


class ModelBase(CustomSQLBase):
    """
    Base class for all models
    """

    id: int | None = Field(default=None, primary_key=True)


class TimeStampedBase(ModelBase):
    """
    Base class for all models with time stamps
    """

    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        nullable=False,
        sa_column_kwargs={"server_default": text("current_timestamp(0)")},
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        nullable=False,
        sa_column_kwargs={
            "server_default": text("current_timestamp(0)"),
            "onupdate": text("current_timestamp(0)"),
        },
    )
