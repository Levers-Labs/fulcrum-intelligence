from pydantic import EmailStr
from sqlalchemy import Column, Enum, String
from sqlmodel import Field

from commons.db.models import BaseSQLModel
from commons.models import BaseModel
from insights_backend.core.enums import AuthProviders
from insights_backend.core.models import InsightsSchemaBaseModel


class UserBase(BaseSQLModel):
    name: str = Field(max_length=255, nullable=False)
    email: EmailStr = Field(sa_column=Column(String(255), nullable=False, unique=True))
    external_user_id: str | None = Field(max_length=100, nullable=True)
    profile_picture: str | None = Field(max_length=255, nullable=True)


class User(UserBase, InsightsSchemaBaseModel, table=True):  # type: ignore
    provider: AuthProviders | None = Field(sa_column=Column(Enum(AuthProviders, name="provider", inherit_schema=True)))


class UserUpdate(BaseModel):
    name: str
    email: EmailStr
    external_user_id: str | None = None
    profile_picture: str | None = None


class UserCreate(BaseModel):
    name: str
    provider: AuthProviders | None
    email: EmailStr
    tenant_org_id: str
    external_user_id: str | None = None
    profile_picture: str | None = None


class UserRead(UserBase):
    id: int
    tenant_id: int


class UserList(BaseModel):
    count: int
    results: list[UserRead]
