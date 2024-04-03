from datetime import date
from typing import Annotated

from pydantic import Field

from analysis_manager.core.models import UserRead
from analysis_manager.db.models import CustomBase
from analysis_manager.utilities.enums import Granularity


class UserList(CustomBase):
    count: int
    results: list[UserRead]


class DimensionRequest(CustomBase):
    dimension: str
    slices: list[str] | None = None


class DescribeResponse(CustomBase):
    metric_id: str | None = None
    dimension: str
    slice: str | None
    mean: float | None = None
    median: float | None = None
    variance: float | None = None
    standard_deviation: float | None
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


class DescribeRequest(CustomBase):
    metric_id: int
    start_date: date
    end_date: date
    dimensions: list[DimensionRequest] | None = None


class CorrelateRequest(CustomBase):
    metric_ids: Annotated[list[str], Field(..., min_length=2)]
    start_date: date
    end_date: date


class ProcessControlRequest(CustomBase):
    metric_id: str
    start_date: date
    end_date: date


class ProcessControlResponse(CustomBase):
    metric_id: str
    start_date: date
    end_date: date
    grain: Granularity
    date: date
    metric_value: float
    central_line: float
    ucl: float
    lcl: float
