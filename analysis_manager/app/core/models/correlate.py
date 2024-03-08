from sqlmodel import Field
from app.db.models import TimeStampedBase
from datetime import date


class Correlate(TimeStampedBase, table=True):
    metric_id_1: str = Field(max_length=255, index=True)
    metric_id_2: str = Field(max_length=255, index=True)
    start_date: date
    end_date: date
    correlation: float
