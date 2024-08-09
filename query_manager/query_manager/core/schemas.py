from __future__ import annotations

import datetime

from pydantic import ConfigDict, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from commons.models import BaseModel
from commons.models.enums import Granularity
from query_manager.core.enums import Complexity, TargetAim
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
    outputs: list[str] | None = Field(default_factory=list)
    inputs: list[str] | None = Field(default_factory=list)
    influences: list[str] | None = Field(default_factory=list)
    influencers: list[str] | None = Field(default_factory=list)
    dimensions: list[DimensionDetail] | None = Field(default_factory=list)

    @field_validator("inputs", "outputs", "influences", "influencers", mode="before")
    @classmethod
    def extract_metric_ids(cls, v):
        if isinstance(v, list) and v and hasattr(v[0], "metric_id"):
            return [metric.metric_id for metric in v]
        return v


class MetricValue(BaseModel, extra="allow"):  # type: ignore
    metric_id: str | None = None
    value: int | float
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
    aim: TargetAim
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
                related_metrics = await db.execute(select(Metric).where(Metric.metric_id.in_(validated_data[field])))  # type: ignore
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

        for field in ["influences", "influencers", "components", "inputs"]:
            if field in validated_data:
                related_metrics = await db.execute(select(Metric).where(Metric.metric_id.in_(validated_data[field])))  # type: ignore
                setattr(instance, field, related_metrics.scalars().all())
        db.add(instance)
        await db.commit()
        await db.refresh(instance)
        return instance
