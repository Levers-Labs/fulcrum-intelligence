from commons.db.filters import BaseFilter, FilterField
from commons.models.enums import Granularity
from commons.notifiers.constants import NotificationChannel
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import Alert, NotificationChannelConfig


class NotificationConfigFilter(BaseFilter[NotificationChannelConfig]):
    """Filter parameters for notification channels listing."""

    notification_type: NotificationType | None = FilterField(
        NotificationChannelConfig.notification_type, operator="eq", default=None  # type: ignore
    )

    channel_type: NotificationChannel | None = FilterField(
        NotificationChannelConfig.channel_type, operator="eq", default=None  # type: ignore
    )

    # Update these to handle both Alert and Report fields
    grain: Granularity | None = FilterField(
        Alert.grain,  # type: ignore
        operator="eq",
        default=None,
    )

    is_active: bool | None = FilterField(
        Alert.is_active,  # type: ignore
        operator="eq",
        default=None,
    )
