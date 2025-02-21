from pydantic import BaseModel
from sqlalchemy import Select, and_
from sqlalchemy.dialects import postgresql

from commons.models.enums import Granularity
from commons.notifiers.constants import NotificationChannel
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import Alert, NotificationChannelConfig, Report


class NotificationConfigFilter(BaseModel):
    """Filter parameters for notification channels listing."""

    notification_type: NotificationType | None = None
    channel_type: NotificationChannel | None = None
    grain: Granularity | None = None
    is_active: bool | None = None
    tags: list[str] | None = None

    def apply_filters(self, query: Select, model: type[Alert] | type[Report]) -> Select:
        """Apply filters to the query, handling both Alert and Report models."""
        if self.grain is not None:
            query = query.filter(model.grain == self.grain)  # type: ignore

        if self.is_active is not None:
            query = query.filter(model.is_active == self.is_active)  # type: ignore

        if self.tags:
            # Use PostgreSQL array overlap operator
            query = query.filter(model.tags.op("&&")(postgresql.array(self.tags)))  # type: ignore

        # Channel config related filters
        if self.notification_type is not None or self.channel_type is not None:
            conditions = []
            if self.notification_type is not None:
                conditions.append(NotificationChannelConfig.notification_type == self.notification_type)
            if self.channel_type is not None:
                conditions.append(NotificationChannelConfig.channel_type == self.channel_type)

            if conditions:
                # Add conditions to the existing join instead of creating a new one
                query = query.filter(and_(*conditions))  # type: ignore

        return query
