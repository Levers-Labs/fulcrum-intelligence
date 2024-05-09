from datetime import date

from sqlmodel import Field

from commons.db.models import BaseTimeStampedModel


class Describe(BaseTimeStampedModel, table=True):  # type: ignore
    metric_id: str = Field(max_length=255, index=True)
    start_date: date
    end_date: date
    dimension: str = Field(max_length=255, index=True)
    mean: float
    median: float
    variance: float
    standard_deviation: float
    percentile_25: float
    percentile_50: float
    percentile_75: float
    percentile_90: float
    percentile_95: float
    percentile_99: float
    min: float
    max: float
    count: int
    sum: float
    unique: int
