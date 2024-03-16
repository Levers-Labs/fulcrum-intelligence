from datetime import date

from sqlmodel import Field

from analysis_manager.db.models import CustomSQLBase, TimeStampedBase


class CorrelateBase(CustomSQLBase):
    metric_id_1: str = Field(max_length=255, index=True)
    metric_id_2: str = Field(max_length=255, index=True)
    correlation_coefficient: float


class Correlate(TimeStampedBase, CorrelateBase, table=True):
    start_date: date
    end_date: date


class CorrelateRead(CorrelateBase):
    pass
