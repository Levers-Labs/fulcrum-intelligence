from commons.db.models import BaseTimeStampedModel


class InsightsSchemaBaseModel(BaseTimeStampedModel):
    __table_args__ = {"schema": "insights_store"}
