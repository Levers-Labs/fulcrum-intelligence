from commons.db.filters import BaseFilter, FilterField
from insights_backend.core.models import Tenant, TenantConfig


class TenantConfigFilter(BaseFilter[TenantConfig]):
    enable_story_generation: bool | None = FilterField(
        field=TenantConfig.enable_story_generation,  # type: ignore
        operator="is",
        default=None,
    )
    identifier: str | None = FilterField(
        field=Tenant.identifier,  # type: ignore
        operator="eq",
        default=None,
        select_from=Tenant,
    )
