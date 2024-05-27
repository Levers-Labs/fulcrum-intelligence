from datetime import date
from typing import Annotated

from pydantic import Field

from analysis_manager.core.models import UserRead
from commons.models import BaseModel
from commons.models.enums import Granularity


class UserList(BaseModel):
    count: int
    results: list[UserRead]


class DimensionRequest(BaseModel):
    dimension: str
    members: list[str] | None = None


class DescribeResponse(BaseModel):
    metric_id: str | None = None
    dimension: str
    member: str | None
    mean: float | None = None
    median: float | None = None
    variance: float | None = None
    percentile_25: float | None
    percentile_50: float | None
    percentile_75: float | None
    percentile_90: float | None
    percentile_95: float | None
    percentile_99: float | None
    min: float | None = None
    max: float | None = None
    count: int | None = None
    sum: float | None = None
    unique: int | None = None


class DescribeRequest(BaseModel):
    metric_id: str
    start_date: date
    end_date: date
    dimensions: list[DimensionRequest] | None = None
    grain: Granularity


class CorrelateRequest(BaseModel):
    metric_ids: Annotated[list[str], Field(..., min_length=2)]
    start_date: date
    end_date: date
    grain: Granularity


class ProcessControlRequest(BaseModel):
    metric_id: str
    start_date: date
    end_date: date
    grain: Granularity


class ProcessControlResponse(BaseModel):
    date: date
    value: float | None = None
    central_line: float | None = None
    ucl: float | None = None
    lcl: float | None = None
    slope: float | None = None
    slope_change: float | None = None
    trend_signal_detected: bool | None = None


class ForecastRequest(BaseModel):
    metric_id: str
    start_date: date
    end_date: date
    grain: Granularity
    # either forecast_horizon or forecast_till_date should be provided
    forecast_horizon: int | None = None
    # horizon is calculated based on the forecast_till_date
    forecast_till_date: date | None = None
    confidence_interval: float | None = Field(
        None, description="Confidence interval for the forecast, between 0 and 100"
    )


class ForecastResponse(BaseModel):
    date: date
    value: float
    # lower and upper bounds of the confidence interval
    confidence_interval: tuple[float, float] | None = None
