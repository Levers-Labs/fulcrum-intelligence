from datetime import date
from typing import Annotated

from pydantic import Field

from analysis_manager.core.models import UserRead
from analysis_manager.db.models import BaseModel
from analysis_manager.utilities.enums import Granularity


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


class CorrelateRequest(BaseModel):
    metric_ids: Annotated[list[str], Field(..., min_length=2)]
    start_date: date
    end_date: date


class ProcessControlRequest(BaseModel):
    metric_id: str
    start_date: date
    end_date: date
    grains: list[Granularity] = [
        Granularity.DAY,
        Granularity.WEEK,
        Granularity.MONTH,
        Granularity.QUARTER,
        Granularity.YEAR,
    ]


class GrainResult(BaseModel):
    date: date
    metric_value: float | None = None
    central_line: float | None = None
    ucl: float | None = None
    lcl: float | None = None


class ProcessControlResponse(BaseModel):
    metric_id: str
    start_date: date | None = None
    end_date: date | None = None
    grain: Granularity
    results: list[GrainResult]


class ForecastRequest(BaseModel):
    metric_id: str
    start_date: date
    end_date: date
    grain: Granularity


class ForecastResponse(BaseModel):
    metric_id: str
    date: date
    value: float


class LinearModelRequest(BaseModel):
    metric_id: str
    start_date: date
    end_date: date
