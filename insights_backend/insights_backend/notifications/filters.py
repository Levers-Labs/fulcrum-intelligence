from datetime import datetime

from pydantic import BaseModel
from sqlalchemy import (
    Select,
    and_,
    func,
    or_,
)
from sqlalchemy.dialects import postgresql

from commons.db.filters import BaseFilter, FilterField
from commons.models.enums import ExecutionStatus, Granularity
from commons.notifiers.constants import NotificationChannel
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import (
    Alert,
    NotificationChannelConfig,
    NotificationExecution,
    Report,
)


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


def create_alert_trigger_jsonb_array_filter(json_path: list[str]) -> FilterField:
    """Create a FilterField for alert trigger JSONB array contains operation.

    Example:
        For a JSONB column 'trigger' with structure:
        {
            "condition": {
                "metric_ids": ["metric1", "metric2"],
                "story_groups": ["group1", "group2"]
            }
        }

        json_path = ["condition", "metric_ids"]
        value = ["metric1", "metric3"]

        Generates SQL like:
        trigger -> 'condition' -> 'metric_ids' ?| ARRAY['metric1', 'metric3']

        Which checks if any value in the JSONB array matches any value in the input array.
    """

    def filter_fn(query: Select, value: list[str]) -> Select:
        if not value:
            return query
        # Match if either:
        # 1. The field is NULL or empty array (accepts all values)
        # 2. The field contains any of the specified values
        field = Alert.trigger[json_path[0]][json_path[1]]  # type: ignore
        return query.filter(
            or_(
                field.is_(None),  # NULL means accept all
                func.jsonb_array_length(field) == 0,  # Empty array means accept all
                field.op("?|")(postgresql.array(value)),  # Match any of the specified values
            )
        )

    return FilterField(field=Alert.trigger, filter_fn=filter_fn)  # type: ignore


class AlertFilter(BaseFilter):
    """Filter parameters specific to alerts, including trigger-based filtering."""

    is_active: bool | None = FilterField(Alert.is_active, operator="eq", default=None)  # type: ignore
    is_published: bool | None = FilterField(Alert.is_published, operator="eq", default=None)  # type: ignore
    grains: list[Granularity] | None = FilterField(Alert.grain, operator="in", default=None)  # type: ignore
    metric_ids: list[str] | None = create_alert_trigger_jsonb_array_filter(["condition", "metric_ids"])  # type: ignore
    story_groups: list[str] | None = create_alert_trigger_jsonb_array_filter(["condition", "story_groups"])  # type: ignore  # noqa


class NotificationExecutionFilter(BaseFilter):
    """Filter parameters for notification executions."""

    notification_type: NotificationType | None = FilterField(
        NotificationExecution.notification_type, operator="eq", default=None  # type: ignore
    )
    status: ExecutionStatus | None = FilterField(NotificationExecution.status, operator="eq", default=None)  # type: ignore
    alert_id: int | None = FilterField(NotificationExecution.alert_id, operator="eq", default=None)  # type: ignore
    report_id: int | None = FilterField(NotificationExecution.report_id, operator="eq", default=None)  # type: ignore
    start_date: datetime | None = FilterField(NotificationExecution.executed_at, operator="ge", default=None)  # type: ignore
    end_date: datetime | None = FilterField(NotificationExecution.executed_at, operator="le", default=None)  # type: ignore
