from sqlalchemy import (
    Boolean,
    Column,
    Enum,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy_utils import EncryptedType
from sqlmodel import Field, Relationship

from commons.db.models import BaseTimeStampedModel
from commons.models import BaseModel
from commons.models.enums import SnowflakeAuthMethod
from commons.models.tenant import Tenant as TenantBase, TenantConfig as TenantConfigBase
from insights_backend.config import get_settings
from insights_backend.core.models import InsightsSchemaBaseModel


def get_encryption_key():
    settings = get_settings()
    return settings.SECRET_KEY


class Tenant(TenantBase, BaseTimeStampedModel, table=True):  # type: ignore
    __table_args__ = {"schema": "insights_store"}

    config: "TenantConfig" = Relationship(
        back_populates="tenant",
        sa_relationship_kwargs={
            "cascade": "all, delete",
        },
    )
    snowflake_config: "SnowflakeConfig" = Relationship(
        back_populates="tenant",
        sa_relationship_kwargs={
            "cascade": "all, delete",
        },
    )


class TenantRead(TenantBase):
    id: int


class TenantList(BaseModel):
    results: list[TenantRead]
    count: int


class TenantConfig(TenantConfigBase, InsightsSchemaBaseModel, table=True):  # type: ignore
    tenant_id: int = Field(
        sa_column=Column(Integer, ForeignKey("insights_store.tenant.id", ondelete="CASCADE"), unique=True)
    )

    tenant: Tenant = Relationship(back_populates="config")
    enable_story_generation: bool = Field(default=False, sa_column=Column(Boolean, default=False))
    enable_metric_cache: bool = Field(default=False, sa_column=Column(Boolean, default=False))


class SnowflakeConfig(InsightsSchemaBaseModel, table=True):  # type: ignore
    tenant_id: int = Field(
        sa_column=Column(Integer, ForeignKey("insights_store.tenant.id", ondelete="CASCADE"), unique=True)
    )
    tenant: Tenant = Relationship(back_populates="snowflake_config")
    account_identifier: str = Field(sa_column=Column(String(255)))
    username: str = Field(sa_column=Column(String(255)))
    warehouse: str | None = Field(sa_column=Column(String(255), nullable=True))
    role: str | None = Field(sa_column=Column(String(255), nullable=True))
    database: str = Field(sa_column=Column(String(255)))
    db_schema: str = Field(sa_column=Column(String(255)))
    auth_method: SnowflakeAuthMethod = Field(
        sa_column=Column(Enum(SnowflakeAuthMethod, name="snowflake_auth_method", inherit_schema=True))
    )
    password: str | None = Field(
        sa_column=Column(
            EncryptedType(String, key=get_encryption_key),
            nullable=True,
        )
    )
    private_key: str | None = Field(
        sa_column=Column(
            EncryptedType(String, key=get_encryption_key),
            nullable=True,
        )
    )
    private_key_passphrase: str | None = Field(
        sa_column=Column(
            EncryptedType(String, key=get_encryption_key),
            nullable=True,
        )
    )
