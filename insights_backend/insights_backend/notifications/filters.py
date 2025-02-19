from sqlalchemy import or_

from commons.db.filters import BaseFilter, FilterField
from commons.models.enums import Granularity
from commons.notifiers.constants import NotificationChannel
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import Alert, NotificationChannelConfig, Report


class NotificationConfigFilter(BaseFilter[NotificationChannelConfig]):
    """Filter parameters for notification channels listing."""

    notification_type: NotificationType | None = FilterField(
        NotificationChannelConfig.notification_type, operator="eq", default=None  # type: ignore
    )

    channel_type: NotificationChannel | None = FilterField(
        NotificationChannelConfig.channel_type, operator="eq", default=None  # type: ignore
    )

    grain: Granularity | None = FilterField(
        or_(Alert.grain, Report.grain),  # type: ignore
        operator="eq",
        default=None,
        join_model=Alert,
        join_condition=lambda: NotificationChannelConfig.alert_id == Alert.id,
    )

    tags: list[str] | None = FilterField(
        or_(Alert.tags, Report.tags),  # type: ignore
        operator="in",
        default=None,
        join_model=Alert,
        join_condition=lambda: NotificationChannelConfig.alert_id == Alert.id,
    )

    is_active: bool | None = FilterField(
        or_(Alert.is_active, Report.is_active),  # type: ignore
        operator="eq",
        default=None,
        join_model=Alert,
        join_condition=lambda: NotificationChannelConfig.alert_id == Alert.id,
    )
