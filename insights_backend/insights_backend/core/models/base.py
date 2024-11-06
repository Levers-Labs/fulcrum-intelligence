from commons.db.models import BaseTimeStampedTenantModel


class InsightsSchemaBaseModel(BaseTimeStampedTenantModel):
    __table_args__ = {"schema": "insights_store"}
