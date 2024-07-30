from __future__ import annotations

import datetime

from pydantic import Field, field_validator

from commons.models import BaseModel
from commons.models.enums import Granularity
from query_manager.core.enums import TargetAim
from query_manager.core.models import DimensionBase, MetricBase


class DimensionCompact(DimensionBase):
    id: int


class DimensionDetail(DimensionBase):
    id: int


class DimensionListResponse(BaseModel):
    count: int
    results: list[DimensionCompact]


class MetricList(MetricBase):
    id: int


class MetricListResponse(BaseModel):
    count: int
    results: list[MetricList]


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
