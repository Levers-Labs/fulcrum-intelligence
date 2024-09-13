from commons.db.models import BaseTenantModel


class InsightsSchemaBaseModel(BaseTenantModel):
    __table_args__ = {"schema": "insights_store"}
