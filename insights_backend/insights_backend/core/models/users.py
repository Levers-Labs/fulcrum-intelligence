from pydantic import EmailStr
from sqlalchemy import Column, Enum, String
from sqlmodel import Field

from commons.models import BaseModel
from insights_backend.core.enums import AuthProviders
from insights_backend.core.models import InsightsSchemaBaseModel


class User(InsightsSchemaBaseModel, table=True):  # type: ignore
    name: str = Field(max_length=255, nullable=False)
    provider: AuthProviders | None = Field(sa_column=Column(Enum(AuthProviders, name="provider", inherit_schema=True)))
    email: EmailStr = Field(sa_column=Column(String(255), nullable=False, unique=True))
    external_user_id: str | None = Field(max_length=100, unique=True, nullable=True)
    profile_picture: str | None = Field(max_length=255, nullable=True)


class UserCreate(BaseModel):
    name: str
    provider: AuthProviders | None
    email: EmailStr
    external_user_id: str | None
    profile_picture: str | None


class UserList(BaseModel):
    count: int
    results: list[User]
