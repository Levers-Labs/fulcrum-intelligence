from commons.models import BaseModel


class Tenant(BaseModel):
    tenant_name: str


class TenantConfig(BaseModel):
    config1: str
    config2: str
    config3: str
