from commons.db.models import BaseTimeStampedModel


class AnalysisSchemaBaseModel(BaseTimeStampedModel):
    __table_args__ = {"schema": "analysis_store"}
