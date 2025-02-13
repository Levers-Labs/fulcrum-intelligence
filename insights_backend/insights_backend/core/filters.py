from commons.db.filters import BaseFilter, FilterField
from commons.models.enums import Granularity
from insights_backend.core.models import Alert, Tenant, TenantConfig
from insights_backend.core.schemas import NotificationType


class TenantConfigFilter(BaseFilter[TenantConfig]):
    enable_story_generation: bool | None = FilterField(
        field=TenantConfig.enable_story_generation,  # type: ignore
        operator="is",
        default=None,
        join_model=TenantConfig,  # Add the join model
        join_condition=lambda: TenantConfig.tenant_id == Tenant.id,  # Add the join condition
    )


class NotificationFilter(BaseFilter[Alert]):
    """Filter for notifications list"""

    type: NotificationType | None = FilterField(Alert.type, operator="eq", default=None)  # type: ignore
    grain: Granularity | None = FilterField(Alert.grain, operator="eq", default=None)  # type: ignore
    tags: str | None = FilterField(Alert.tags, operator="in", default=None)  # type: ignore
