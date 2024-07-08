from sqlalchemy import Column, Enum
from sqlmodel import Field

from commons.models import BaseModel
from insights_backend.core.enums import AuthProviders
from insights_backend.core.models.base import InsightsSchemaBaseModel


class User(InsightsSchemaBaseModel, table=True):  # type: ignore
    name: str = Field(max_length=255, nullable=False)
    provider: AuthProviders = Field(
        sa_column=Column(Enum(AuthProviders, name="provider", inherit_schema=True), index=True)
    )
    email: str = Field(max_length=255, nullable=False)
    external_user_id: str = Field(max_length=100, unique=True, nullable=True)
    profile_picture: str = Field(max_length=255, nullable=True)


class UserList(BaseModel):
    count: int
    results: list[User]
