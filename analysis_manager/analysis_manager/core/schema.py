from datetime import date
from typing import Annotated, Any

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


class SegmentDriftRequest(BaseModel):
    metric_id: str
    evaluation_start_date: date
    evaluation_end_date: date
    comparison_start_date: date
    comparison_end_date: date
    dimensions: list[str]


class DimensionSlices(BaseModel):
    key: list[dict[str, Any]]
    serialized_key: str
    evaluation_value: dict[str, Any]
    comparison_value: dict[str, Any]
    impact: int
    change_percentage: float
    change_dev: float
    absolute_contribution: float
    confidence: Any
    sort_value: int
    relative_change: float
    pressure: str


class SegmentDriftResponse(BaseModel):
    id: str
    name: str
    total_segments: int
    expected_change_percentage: float
    aggregation_method: str
    evaluation_num_rows: int
    comparison_num_rows: int
    evaluation_value: int
    comparison_value: int
    evaluation_value_by_date: list[dict[str, Any]]
    comparison_value_by_date: list[dict[str, Any]]
    evaluation_date_range: list[str]
    comparison_date_range: list[str]
    dimensions: list[dict[str, Any]]
    key_dimensions: list[str]
    filters: list
    dimension_slices: list[DimensionSlices]
    dimension_slices_permutation_keys: list[str]
