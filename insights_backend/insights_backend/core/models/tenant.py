from sqlalchemy import (
    JSON,
    Column,
    String,
    Text,
)
from sqlmodel import Field

from commons.db.models import BaseDBModel
from insights_backend.core.models import InsightsSchemaBaseModel


class Tenant(BaseDBModel, table=True):  # type: ignore
    __table_args__ = {"schema": "insights_store"}
    name: str = Field(sa_column=Column(String(255), index=True, unique=True))
    description: str = Field(sa_column=Column(String(255), nullable=True))
    domains: list[str] = Field(sa_column=Column(JSON))
    external_org_id: str = Field(sa_column=Column(String(255), index=True, unique=True))


class TenantConfig(InsightsSchemaBaseModel, table=True):  # type: ignore
    config1: str | None = Field(sa_column=Column(Text, nullable=True))
    config2: str | None = Field(sa_column=Column(Text, nullable=True))
    config3: str | None = Field(sa_column=Column(Text, nullable=True))
