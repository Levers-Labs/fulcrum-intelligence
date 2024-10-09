from sqlalchemy import Column, ForeignKey, Integer
from sqlmodel import Field, Relationship

from commons.db.models import BaseTimeStampedModel
from commons.models import BaseModel
from commons.models.tenant import Tenant as TenantBase, TenantConfig as TenantConfigBase
from insights_backend.core.models import InsightsSchemaBaseModel


class Tenant(TenantBase, BaseTimeStampedModel, table=True):  # type: ignore
    __table_args__ = {"schema": "insights_store"}

    config: "TenantConfig" = Relationship(
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
