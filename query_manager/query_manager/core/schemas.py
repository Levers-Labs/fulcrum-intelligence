from __future__ import annotations

import datetime
from typing import Literal

from pydantic import ConfigDict, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from commons.models import BaseModel
from commons.models.enums import Granularity
from query_manager.core.enums import Complexity, MetricAim
from query_manager.core.models import (
    Dimension,
    DimensionBase,
    Metric,
    MetricBase,
    MetricExpression,
    MetricMetadata,
)


class DimensionCompact(DimensionBase):
    id: int


class DimensionDetail(DimensionBase):
    id: int


class MetricList(MetricBase):
    id: int


class MetricDetail(MetricBase):
    id: int
    outputs: list[str] | None = Field(default_factory=list)  # type: ignore
    inputs: list[str] | None = Field(default_factory=list)  # type: ignore
    influences: list[str] | None = Field(default_factory=list)  # type: ignore
    influencers: list[str] | None = Field(default_factory=list)  # type: ignore
    dimensions: list[DimensionDetail] | None = Field(default_factory=list)  # type: ignore

    @field_validator("inputs", "outputs", "influences", "influencers", mode="before")
    @classmethod
    def extract_metric_ids(cls, v):
        if isinstance(v, list) and v and hasattr(v[0], "metric_id"):
            return [metric.metric_id for metric in v]
        return v

    def get_dimension(self, dimension_id: str) -> DimensionDetail | None:
        if not self.dimensions:
            return None
        return next((dimension for dimension in self.dimensions if dimension.dimension_id == dimension_id), None)


class MetricValue(BaseModel, extra="allow"):  # type: ignore
    metric_id: str | None = None
    value: int | float | None = 0
    date: datetime.date | None = None


class MetricValuesResponse(BaseModel):
    data: list[MetricValue] | None = Field(
        default=None,
        example=[  # type: ignore
            {
                "metric_id": "CAC",
                "value": 203,
                "date": "2022-09-01",
                "customer_segment": "Enterprise",
                "channel": "Online",
                "region": "Asia",
            },
        ],
    )
    url: str | None = Field(None, description="URL to the Parquet file")


class Target(BaseModel):
    metric_id: str
    grain: Granularity
    target_date: datetime.date
    target_value: float
    target_upper_bound: float | None = None
    target_lower_bound: float | None = None
    yellow_buffer: float | None = None
    red_buffer: float | None = None


class TargetListResponse(BaseModel):
    results: list[Target] | None = Field(
        default=None,
        examples=[
            {
                "metric_id": "NewBizDeals",
                "grain": "day",
                "aim": "Maximize",
                "target_date": "2024-03-10",
                "target_value": 3,
                "target_upper_bound": 3,
                "target_lower_bound": 2,
                "yellow_buffer": 0,
                "red_buffer": 0.2,
            }
        ],
    )
    url: str | None = Field(None, description="URL to the Parquet file")


# Create/Update Schema
class MetricCreate(MetricBase):
    dimensions: list[str] | None = Field(None, description="List of dimension IDs")
    influences: list[str] | None = Field(None, description="List of influencing metric IDs")
    components: list[str] | None = Field(None, description="List of component metric IDs")
    inputs: list[str] | None = Field(None, description="List of input metric IDs")

    model_config = ConfigDict(from_attributes=True)  # type: ignore

    @classmethod
    async def create(cls, db: AsyncSession, validated_data: dict):
        # Create the metric instance
        metric = Metric(
            **{k: v for k, v in validated_data.items() if k not in ["dimensions", "influences", "components", "inputs"]}
        )
        db.add(metric)

        # Handle M2M relationships
        if "dimensions" in validated_data:
            dimensions = await db.execute(
                select(Dimension).where(Dimension.dimension_id.in_(validated_data["dimensions"]))  # type: ignore
            )
            metric.dimensions = dimensions.scalars().all()  # type: ignore

        for field in ["influences", "components", "inputs"]:
            if field in validated_data and validated_data[field]:
                related_metrics = await db.execute(
                    select(Metric).where(Metric.metric_id.in_(validated_data[field]))  # type: ignore
                )
                setattr(metric, field, related_metrics.scalars().all())

        db.add(metric)
        await db.commit()
        await db.refresh(metric)
        return metric


class MetricUpdate(BaseModel):
    label: str | None = None
    abbreviation: str | None = None
    definition: str | None = None
    unit_of_measure: str | None = None
    unit: str | None = None
    terms: list[str] | None = None
    complexity: Complexity | None = None
    metric_expression: MetricExpression | None = None
    periods: list[Granularity] | None = None
    grain_aggregation: str | None = None
    aggregations: list[str] | None = None
    owned_by_team: list[str] | None = None
    meta_data: MetricMetadata | None = None
    dimensions: list[str] | None = Field(None, description="List of dimension IDs")
    influences: list[str] | None = Field(None, description="List of influencing metric IDs")
    influencers: list[str] | None = Field(None, description="List of influencer metric IDs")
    components: list[str] | None = Field(None, description="List of component metric IDs")
    inputs: list[str] | None = Field(None, description="List of input metric IDs")
    aim: MetricAim | None = None

    model_config = ConfigDict(from_attributes=True)

    @classmethod
    async def update(cls, db: AsyncSession, instance: Metric, validated_data: dict):
        # Update simple fields
        for attr, value in validated_data.items():
            if attr not in ["dimensions", "influences", "components", "inputs"]:
                setattr(instance, attr, value)

        # Update M2M relationships
        if "dimensions" in validated_data:
            dimensions = await db.execute(
                select(Dimension).where(Dimension.dimension_id.in_(validated_data["dimensions"]))  # type: ignore
            )
            instance.dimensions = dimensions.scalars().all()  # type: ignore

        metric_relationships = ["influences", "influencers", "components", "inputs"]
        # Update metric relationships
        for field in metric_relationships:
            if field in validated_data and validated_data[field]:
                result = await db.execute(
                    select(Metric).where(Metric.metric_id.in_(validated_data[field]))  # type: ignore
                )
                setattr(instance, field, result.scalars().all())

        await db.commit()
        await db.refresh(instance)
        return instance


class DimensionCreate(DimensionBase):
    model_config = ConfigDict(from_attributes=True)  # type: ignore

    @classmethod
    async def create(cls, db: AsyncSession, validated_data: dict):
        """
        Create a new dimension.

        Args:
            db (AsyncSession): The database session.
            validated_data (dict): The validated data for creating a dimension.

        Returns:
            Dimension: The created dimension.
        """
        dimension = Dimension(**validated_data)
        db.add(dimension)
        await db.commit()
        await db.refresh(dimension)
        return dimension


class DimensionUpdate(DimensionBase):
    dimension_id: str | None = None  # type: ignore
    model_config = ConfigDict(from_attributes=True)  # type: ignore

    @classmethod
    async def update(cls, db: AsyncSession, instance: Dimension, validated_data: dict):
        """
        Update an existing dimension.

        Args:
            db (AsyncSession): The database session.
            instance (Dimension): The dimension to update.
            validated_data (dict): The validated data for updating a dimension.

        Returns:
            Dimension: The updated dimension.
        """
        # Pop the dimension_id from the validated data
        validated_data.pop("dimension_id", None)
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        return instance


class ExpressionParseRequest(BaseModel):
    expression: str


class CubeMember(BaseModel):
    """Base schema for cube members (measures and dimensions)"""

    name: str
    title: str
    short_title: str = Field(alias="shortTitle")
    type: str
    description: str | None = None

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "name": "revenue",
                "title": "Revenue",
                "shortTitle": "Rev",
                "type": "number",
                "description": "Total revenue from all sources",
            }
        },
    )


class CubeMeasure(CubeMember):
    """Schema for cube measures"""

    type: Literal["number"] = "number"
    metric_id: str
    grain_aggregation: str

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "name": "total_revenue",
                "title": "Total Revenue",
                "shortTitle": "Revenue",
                "type": "number",
                "description": "Sum of all revenue",
            }
        },
    )


class CubeDimension(CubeMember):
    """Schema for cube dimensions"""

    type: Literal["time", "string", "number", "boolean"]
    dimension_id: str

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "name": "created_at",
                "title": "Created Date",
                "shortTitle": "Created",
                "type": "time",
                "description": "Date when the record was created",
            }
        },
    )


class Cube(BaseModel):
    name: str
    title: str
    measures: list[CubeMeasure]
    dimensions: list[CubeDimension]


class DeleteResponse(BaseModel):
    message: str


# Snowflake Cache Configuration Schemas
class MetricCacheGrainConfigBase(BaseModel):
    grain: Granularity = Field(description="Granularity level (day, week, month)")
    is_enabled: bool = Field(default=True, description="Whether sync is enabled for this grain")
    initial_sync_period: int = Field(description="Initial sync period in days", ge=1, le=3650)  # Max 10 years
    delta_sync_period: int = Field(description="Delta sync period in days", ge=1, le=365)  # Max 1 year

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {"grain": "day", "is_enabled": True, "initial_sync_period": 730, "delta_sync_period": 90}
        },
    )


class MetricCacheGrainConfigCreate(MetricCacheGrainConfigBase):
    pass


class MetricCacheGrainConfigRead(MetricCacheGrainConfigBase):
    id: int
    tenant_id: int

    model_config = ConfigDict(from_attributes=True)


class MetricCacheGrainConfigUpdate(BaseModel):
    is_enabled: bool | None = None
    initial_sync_period: int | None = Field(None, ge=1, le=3650)
    delta_sync_period: int | None = Field(None, ge=1, le=365)

    model_config = ConfigDict(from_attributes=True)


class BulkGrainConfigUpdate(BaseModel):
    configs: list[dict] = Field(
        description="List of grain configurations with grain and update fields",
        examples=[
            [
                {"grain": "day", "is_enabled": True, "initial_sync_period": 730, "delta_sync_period": 90},
                {"grain": "week", "is_enabled": True, "initial_sync_period": 1095, "delta_sync_period": 90},
                {"grain": "month", "is_enabled": False},
            ]
        ],
    )


class MetricCacheConfigBase(BaseModel):
    is_enabled: bool = Field(default=True, description="Whether caching is enabled for this metric")

    model_config = ConfigDict(from_attributes=True)


class MetricCacheConfigRead(MetricCacheConfigBase):
    id: int
    metric_id: str
    last_sync_date: datetime.datetime | None = None
    sync_status: str | None = None

    model_config = ConfigDict(from_attributes=True)


class MetricCacheConfigUpdate(BaseModel):
    is_enabled: bool

    model_config = ConfigDict(from_attributes=True)


class BulkMetricCacheUpdate(BaseModel):
    metric_ids: list[str] = Field(description="List of metric IDs to update")
    is_enabled: bool = Field(description="Enable or disable caching for all specified metrics")

    model_config = ConfigDict(
        json_schema_extra={"example": {"metric_ids": ["metric_1", "metric_2", "metric_3"], "is_enabled": False}}
    )


# Tenant Sync Status Schemas
class TenantSyncStatusResponse(BaseModel):
    """Response schema for tenant sync status."""

    id: int
    sync_operation: str
    grain: Granularity
    last_sync_at: datetime.datetime
    sync_status: str
    metrics_processed: int | None = None
    metrics_succeeded: int | None = None
    metrics_failed: int | None = None
    error: str | None = None
    run_info: dict = {}
    created_at: datetime.datetime
    updated_at: datetime.datetime

    model_config = ConfigDict(from_attributes=True)


# Comprehensive Cache Information Schema
class CacheInfoResponse(BaseModel):
    """Response schema for comprehensive cache information."""

    table_name: str

    # Table/data information
    date_range_start: datetime.date | None = None
    date_range_end: datetime.date | None = None
    total_records: int | None = None
    unique_dates: int | None = None

    model_config = ConfigDict(from_attributes=True)
