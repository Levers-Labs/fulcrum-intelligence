"""
Pydantic schemas for semantic manager API requests and responses.
"""

from datetime import date

from pydantic import ConfigDict, field_validator

from commons.models import BaseModel
from commons.models.enums import Granularity
from query_manager.core.enums import MetricAim
from query_manager.semantic_manager.models import MetricDimensionalTimeSeries, MetricTimeSeries


class MetricTimeSeriesResponse(BaseModel):
    """Response schema for a single metric's time series data."""

    results: list[MetricTimeSeries]

    model_config = ConfigDict(from_attributes=True)


class MetricDimensionalTimeSeriesResponse(BaseModel):
    """Response schema for dimensional time series data."""

    results: list[MetricDimensionalTimeSeries]

    model_config = ConfigDict(from_attributes=True)


class TargetBase(BaseModel):
    """Base schema for target data."""

    metric_id: str
    grain: Granularity
    target_date: date
    target_value: float
    target_upper_bound: float | None = None
    target_lower_bound: float | None = None
    yellow_buffer: float | None = None
    red_buffer: float | None = None


class TargetCreate(TargetBase):
    """Schema for creating a new target."""

    model_config = ConfigDict(from_attributes=True)


class TargetUpdate(BaseModel):
    """Schema for updating an existing target."""

    target_value: float | None = None
    target_upper_bound: float | None = None
    target_lower_bound: float | None = None
    yellow_buffer: float | None = None
    red_buffer: float | None = None

    model_config = ConfigDict(from_attributes=True)


class TargetResponse(TargetBase):
    """Response schema for a single target."""

    id: int

    model_config = ConfigDict(from_attributes=True)


class TargetBulkUpsertRequest(BaseModel):
    """Schema for bulk upserting targets."""

    targets: list[TargetCreate]

    model_config = ConfigDict(from_attributes=True)


class TargetBulkUpsertResponse(BaseModel):
    """Response schema for bulk upserting targets."""

    processed: int
    failed: int
    total: int

    model_config = ConfigDict(from_attributes=True)


class TargetStatus(BaseModel):
    """Status of targets for a time period"""

    has_targets: bool
    through_date: date | None


class MetricTargetOverview(BaseModel):
    """Complete target overview for a metric"""

    metric_id: str
    label: str
    aim: MetricAim
    periods: dict[Granularity, TargetStatus]

    model_config = ConfigDict(from_attributes=True)

    @field_validator("periods")
    @classmethod
    def ensure_all_periods(cls, v: dict[Granularity, TargetStatus]) -> dict[Granularity, TargetStatus]:
        """Ensure all granularities have a status"""
        for grain in Granularity:
            if grain not in v:
                v[grain] = TargetStatus(has_targets=False, through_date=None)
        return v


#
# class MetricsTargetList(BaseModel):
#     """API response for metrics target list"""
#     metrics: list[MetricTargetOverview]
