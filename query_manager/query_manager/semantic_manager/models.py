"""
Models for semantic data storage.
"""

from datetime import date, datetime
from enum import Enum

from sqlalchemy import (
    Column,
    Enum as SAEnum,
    Identity,
    Index,
    Integer,
    PrimaryKeyConstraint,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field
from typing_extensions import TypedDict

from commons.db.models import BaseTimeStampedTenantModel
from commons.models.enums import Granularity


class SyncStatus(str, Enum):
    """Status of metric data synchronization."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RUNNING = "RUNNING"


class SyncType(str, Enum):
    """Type of metric data synchronization."""

    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"


class SyncEvent(TypedDict):
    """Type definition for sync event entries."""

    sync_status: SyncStatus
    sync_type: SyncType
    start_date: str
    end_date: str
    records_processed: int | None
    error: str | None
    last_sync_at: str
    updated_at: str


class TargetCalculationType(str, Enum):
    """Type of target"""

    VALUE = "VALUE"
    GROWTH = "GROWTH"
    POP_GROWTH = "POP_GROWTH"


class MetricSyncStatus(BaseTimeStampedTenantModel, table=True):  # type: ignore
    """
    Stores metadata about metric data synchronization.
    """

    __tablename__ = "metric_sync_status"

    metric_id: str
    grain: Granularity
    dimension_name: str | None = None
    last_sync_at: datetime
    sync_status: SyncStatus = Field(
        default=SyncStatus.RUNNING,
        sa_column=Column(SAEnum(SyncStatus, name="syncstatus", inherit_schema=True)),
    )
    sync_type: SyncType = Field(sa_column=Column(SAEnum(SyncType, name="synctype", inherit_schema=True)))
    start_date: date
    end_date: date
    records_processed: int | None = None
    error: str | None = None
    history: list[SyncEvent] = Field(
        sa_column=Column(JSONB, nullable=False, server_default="[]"),
    )

    # Define table arguments including schema, indexes and constraints
    __table_args__ = (
        # Unique constraint
        UniqueConstraint(
            "metric_id", "tenant_id", "grain", "dimension_name", "sync_type", name="uq_metric_sync_status"
        ),
        # Indexes
        Index("idx_metric_sync_status_metric_tenant", "metric_id", "tenant_id"),
        Index("idx_metric_sync_status_grain_dimension", "grain", "dimension_name"),
        Index("idx_metric_sync_status_last_sync", "last_sync_at", postgresql_ops={"last_sync_at": "DESC"}),
        Index("idx_metric_sync_status_status", "sync_status"),
        Index("idx_metric_sync_status_history", "history", postgresql_using="gin"),
        # Schema definition
        {"schema": "query_store"},
    )


class MetricTimeSeries(BaseTimeStampedTenantModel, table=True):  # type: ignore
    """
    Stores aggregated metric values without dimensions.
    """

    __tablename__ = "metric_time_series"

    # Auto-incrementing id that's not a primary key
    id: int = Field(sa_column=Column(Integer(), Identity(always=True), nullable=False))

    metric_id: str
    date: date
    grain: Granularity
    value: float

    # Define table arguments including schema, indexes and constraints
    __table_args__ = (
        # Primary key is now the composite natural key
        PrimaryKeyConstraint("metric_id", "tenant_id", "date", "grain"),
        # Indexes
        Index("idx_metric_time_series_metric_tenant", "metric_id", "tenant_id"),
        Index("idx_metric_time_series_date", "date", postgresql_ops={"date": "DESC"}),
        Index("idx_metric_time_series_grain", "grain"),
        Index(
            "idx_metric_time_series_metric_tenant_grain_date",
            "metric_id",
            "tenant_id",
            "grain",
            "date",
            postgresql_ops={"date": "DESC"},
        ),
        # Schema definition
        {"schema": "query_store"},
    )


class MetricDimensionalTimeSeries(BaseTimeStampedTenantModel, table=True):  # type: ignore
    """
    Stores aggregated metric values with dimensions.
    """

    __tablename__ = "metric_dimensional_time_series"

    # Auto-incrementing id that's not a primary key
    id: int = Field(sa_column=Column(Integer(), Identity(always=True), nullable=False))
    metric_id: str
    date: date
    grain: Granularity
    dimension_name: str
    # We can have 'None' as slice (i.e. Empty)
    # todo: fix issue where hypertable does not allow null ( as this is part of pk)
    dimension_slice: str | None = None
    value: float

    # Define table arguments including schema, indexes and constraints
    __table_args__ = (
        # Primary key constraint using the composite natural key
        PrimaryKeyConstraint("metric_id", "tenant_id", "date", "grain", "dimension_name", "dimension_slice"),
        # Indexes
        Index("idx_metric_dimensional_ts_metric_tenant", "metric_id", "tenant_id"),
        Index("idx_metric_dimensional_ts_date", "date", postgresql_ops={"date": "DESC"}),
        Index("idx_metric_dimensional_ts_grain", "grain"),
        Index("idx_metric_dimensional_ts_dimension", "dimension_name", "dimension_slice"),
        Index(
            "idx_metric_dimensional_ts_metric_tenant_grain_date",
            "metric_id",
            "tenant_id",
            "grain",
            "date",
            postgresql_ops={"date": "DESC"},
        ),
        # Schema definition
        {"schema": "query_store"},
    )


class MetricTarget(BaseTimeStampedTenantModel, table=True):  # type: ignore
    """
    Stores target values for metrics at different granularities.
    """

    __tablename__ = "metric_target"

    metric_id: str
    grain: Granularity
    target_date: date
    target_value: float
    target_upper_bound: float | None = None
    target_lower_bound: float | None = None
    yellow_buffer: float | None = None
    red_buffer: float | None = None

    # Define table arguments including schema, indexes, and constraints
    __table_args__ = (
        # Unique constraint
        UniqueConstraint("metric_id", "grain", "target_date", "tenant_id", name="uq_metric_target"),
        # Indexes
        Index("idx_metric_target_metric_tenant", "metric_id", "tenant_id"),
        Index("idx_metric_target_date", "target_date", postgresql_ops={"target_date": "DESC"}),
        Index("idx_metric_target_grain", "grain"),
        Index(
            "idx_target_metric_tenant_grain_date",
            "metric_id",
            "tenant_id",
            "grain",
            "target_date",
            postgresql_ops={"target_date": "DESC"},
        ),
        # Schema definition
        {"schema": "query_store"},
    )
