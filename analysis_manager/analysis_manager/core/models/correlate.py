from datetime import date

from sqlmodel import Field

from commons.db.models import BaseSQLModel, BaseTimeStampedModel


class CorrelateBase(BaseSQLModel):
    metric_id_1: str = Field(max_length=255, index=True)
    metric_id_2: str = Field(max_length=255, index=True)
    correlation_coefficient: float


class Correlate(BaseTimeStampedModel, CorrelateBase, table=True):  # type: ignore
    start_date: date
    end_date: date


class CorrelateRead(CorrelateBase):
    pass
