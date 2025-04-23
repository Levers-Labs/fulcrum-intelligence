"""
Pydantic schemas for semantic manager API requests and responses.
"""

from datetime import date
from typing import Literal

from pydantic import ConfigDict, field_validator, model_validator

from commons.models import BaseModel
from commons.models.enums import Granularity
from query_manager.core.enums import MetricAim
from query_manager.semantic_manager.models import MetricDimensionalTimeSeries, MetricTimeSeries, TargetCalculationType


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
    growth_percentage: float | None = None
    pop_growth_percentage: float | None = None

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
    """Status of targets"""

    target_set: bool
    target_end_date: date | None


class MetricTargetOverview(BaseModel):
    """Complete target overview for a metric"""

    metric_id: str
    label: str
    aim: MetricAim | None = None
    periods: dict[Granularity, TargetStatus]

    model_config = ConfigDict(from_attributes=True)

    @field_validator("periods")
    @classmethod
    def ensure_all_periods(cls, v: dict[Granularity, TargetStatus]) -> dict[Granularity, TargetStatus]:
        """Ensure all granularities have a status"""
        for grain in Granularity:
            if grain not in v:
                v[grain] = TargetStatus(target_set=False, target_end_date=None)
        return v


# class TargetCalculationEntry(BaseModel):
#     """Schema for a single target calculation entry."""

#     date: date
#     value: float
#     growth_percentage: float
#     pop_growth_percentage: float

#     model_config = ConfigDict(from_attributes=True)


class TargetCalculationRequest(BaseModel):
    """Schema for target calculation request."""

    current_value: float
    start_date: date
    end_date: date
    grain: Granularity
    calculation_type: TargetCalculationType
    target_value: float | None = None
    growth_percentage: float | None = None
    pop_growth_percentage: float | None = None

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="after")
    def validate_calculation_parameters(self):
        """Validate that required parameters are provided based on calculation_type."""
        if self.calculation_type == "value" and self.target_value is None:
            raise ValueError("target_value is required when calculation_type is 'value'")

        if self.calculation_type == "growth" and self.growth_percentage is None:
            raise ValueError("growth_percentage is required when calculation_type is 'growth'")

        if self.calculation_type == "pop_growth" and self.pop_growth_percentage is None:
            raise ValueError("pop_growth_percentage is required when calculation_type is 'pop_growth'")

        if self.start_date > self.end_date:
            raise ValueError("start_date must be before or equal to end_date")

        return self


class TargetCalculationResponse(BaseModel):
    """Response schema for target calculation."""

    date: date
    value: float
    growth_percentage: float
    pop_growth_percentage: float

    model_config = ConfigDict(from_attributes=True)
