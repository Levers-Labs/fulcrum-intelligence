from datetime import datetime

from pydantic import ConfigDict
from sqlalchemy import event, text
from sqlmodel import Field, SQLModel

from commons.utilities.context import get_tenant_id
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


class BaseTenantModel(BaseDBModel):
    """
    Base class for all models containing tenant specific data
    """

    tenant_id: int = Field(index=True, nullable=False, sa_column_kwargs={"index": True})

    @classmethod
    def add_tenant_id(cls, mapper, connection, target):  # pylint: disable=unused-argument
        tenant_id = get_tenant_id()
        if not tenant_id:
            raise ValueError("tenant_id cannot be blank or null")

        target.tenant_id = int(tenant_id)


class BaseTimeStampedTenantModel(BaseTenantModel, BaseTimeStampedModel):
    """
    Base class for all models containing tenant specific data
    """


# Add interceptor to set tenant_id on insert
event.listen(BaseTenantModel, "before_insert", BaseTenantModel.add_tenant_id, propagate=True)
event.listen(BaseTimeStampedTenantModel, "before_insert", BaseTimeStampedTenantModel.add_tenant_id, propagate=True)
