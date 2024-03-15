from datetime import date

from app.core.models import UserRead
from app.db.models import CustomBase


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
