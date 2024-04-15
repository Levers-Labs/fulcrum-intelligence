from datetime import datetime

from pydantic import ConfigDict
from sqlalchemy import text
from sqlmodel import Field, SQLModel

from commons.utilities.date_utils import convert_datetime_to_utc


class BaseSQLModel(SQLModel):
    """
    Custom base class for sqlmodel models
    """

    model_config = ConfigDict(  # type: ignore
        json_encoders={datetime: convert_datetime_to_utc},
        populate_by_name=True,
    )


class BaseDBModel(BaseSQLModel):
    """
    Base class for all models stored in the database
    """

    id: int | None = Field(default=None, primary_key=True)


class BaseTimeStampedModel(BaseDBModel):
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
