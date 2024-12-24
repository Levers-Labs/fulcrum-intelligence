from commons.db.filters import BaseFilter, FilterField
from insights_backend.core.models import Tenant, TenantConfig


class TenantConfigFilter(BaseFilter[TenantConfig]):
    enable_story_generation: bool | None = FilterField(
        field=TenantConfig.enable_story_generation,  # type: ignore
        operator="is",
        default=None,
        join_model=TenantConfig,  # Add the join model
        join_condition=lambda: TenantConfig.tenant_id == Tenant.id,  # Add the join condition
    )
