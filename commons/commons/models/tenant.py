from commons.models import BaseModel


class Tenant(BaseModel):
    name: str
    description: str
    domains: list[str]


class TenantConfig(BaseModel):
    config1: str
    config2: str
    config3: str
