from datetime import date

from sqlmodel import Field

from analysis_manager.core.models.base_model import AnalysisSchemaBaseModel
from commons.db.models import BaseSQLModel


class CorrelateBase(BaseSQLModel):

    metric_id_1: str = Field(max_length=255, index=True)
    metric_id_2: str = Field(max_length=255, index=True)
    correlation_coefficient: float


class Correlate(AnalysisSchemaBaseModel, CorrelateBase, table=True):  # type: ignore

    start_date: date
    end_date: date


class CorrelateRead(CorrelateBase):
    pass
