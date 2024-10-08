from commons.db.models import BaseTimeStampedTenantModel


class AnalysisSchemaBaseModel(BaseTimeStampedTenantModel):
    __table_args__ = {"schema": "analysis_store"}
