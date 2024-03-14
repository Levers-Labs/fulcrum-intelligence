from datetime import date
from typing import List, Optional

from app.core.models import UserRead
from app.db.models import CustomBase


class UserList(CustomBase):
    count: int
    results: list[UserRead]


class DimensionFilter(CustomBase):
    dimension: str
    slices: Optional[List[str]] = None


class DescribeResponse(CustomBase):
    metric_id: Optional[str] = None
    dimension: str
    slice: Optional[str]
    mean: Optional[float] = None
    median: Optional[float] = None
    variance: Optional[float] = None
    standard_deviation: Optional[float]
    percentile_25: Optional[float]
    percentile_50: Optional[float]
    percentile_75: Optional[float]
    percentile_90: Optional[float]
    percentile_95: Optional[float]
    percentile_99: Optional[float]
    min: Optional[float] = None
    max: Optional[float] = None
    count: Optional[int] = None
    sum: Optional[float] = None
    unique: Optional[int] = None


class DescribeRequest(CustomBase):
    metric_id: int
    start_date: date
    end_date: date
    dimensions:  Optional[List[DimensionFilter]] = None
