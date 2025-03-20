from pydantic import ConfigDict, EmailStr, field_validator
from sqlalchemy import (
    Column,
    Enum,
    String,
    UniqueConstraint,
)
from sqlmodel import Field, Relationship

from commons.db.models import BaseSQLModel, BaseTimeStampedModel
from commons.models import BaseModel
from insights_backend.core.enums import AuthProviders
from insights_backend.core.models import InsightsSchemaBaseModel


class UserBase(BaseSQLModel):
    name: str = Field(max_length=255, nullable=False)
    email: EmailStr = Field(sa_column=Column(String(255), nullable=False, unique=True))
    external_user_id: str | None = Field(max_length=100, nullable=True)
    profile_picture: str | None = Field(max_length=255, nullable=True)


class User(UserBase, BaseTimeStampedModel, table=True):  # type: ignore
    __table_args__ = {"schema": "insights_store"}
    provider: AuthProviders | None = Field(sa_column=Column(Enum(AuthProviders, name="provider", inherit_schema=True)))
    tenant_ids: list["UserTenant"] = Relationship(
        back_populates="user", sa_relationship_kwargs={"cascade": "all, delete-orphan", "lazy": "selectin"}
    )


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
    tenant_ids: list[int] | None = Field(default_factory=list)

    @field_validator("tenant_ids", mode="before")
    @classmethod
    def convert_tenants_to_ids(cls, v):
        if isinstance(v, list) and v and hasattr(v[0], "tenant_id"):
            return [t.tenant_id for t in v]
        return v

    model_config = ConfigDict(
        from_attributes=True,
    )


class UserList(BaseModel):
    count: int
    results: list[UserRead]


class UserTenant(InsightsSchemaBaseModel, table=True):  # type: ignore

    __table_args__ = (
        UniqueConstraint("user_id", "tenant_id", name="uq_user_tenant"),  # type: ignore
        {"schema": "insights_store"},
    )

    user_id: int = Field(foreign_key="insights_store.user.id")
    user: User = Relationship(back_populates="tenant_ids", sa_relationship_kwargs={"lazy": "selectin"})
