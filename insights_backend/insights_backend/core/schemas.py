from datetime import datetime
from typing import Any

from pydantic import (
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from commons.models import BaseModel
from commons.models.enums import Granularity
from commons.models.slack import SlackChannel
from commons.notifiers.constants import NotificationChannel
from insights_backend.core.models.notifications import (
    AlertTrigger,
    EmailRecipient,
    NotificationChannelConfigBase,
    NotificationStatus,
    NotificationType,
)


class SlackChannelResponse(BaseModel):
    results: list[SlackChannel]
    next_cursor: str | None = None


class NotificationChannelDetail(BaseModel):
    """Configuration for notification channels (Slack/Email) that can be used by alerts and reports"""

    channel_type: NotificationChannel
    recipients: list[SlackChannel | EmailRecipient]


class AlertCreateRequest(BaseModel):
    """Request model for creating alerts"""

    name: str
    description: str | None = None
    grain: Granularity
    trigger: AlertTrigger
    tags: list[str] = []
    is_active: bool = True
    is_published: bool = True
    notification_channels: list[NotificationChannelConfigBase] = []

    @model_validator(mode="after")
    def validate_notification_channels(self) -> "AlertCreateRequest":
        """Ensure notification channels are provided if alert is published"""
        if self.is_published and not self.notification_channels:
            raise ValueError("Published alerts must have at least one notification channel")
        return self

    @field_validator("tags")
    @classmethod
    def validate_unique_tags(cls, v: list[str]) -> list[str]:
        """Ensure tags are unique and non-empty"""
        return list(dict.fromkeys(tag.strip() for tag in v if tag.strip()))

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Daily Sales Alert",
                "description": "Alert for daily sales metrics",
                "grain": "day",
                "trigger": {
                    "type": "METRIC_STORY",
                    "condition": {"metric_ids": ["newInqs"], "story_groups": ["TREND_CHANGES"]},
                },
                "tags": ["sales", "daily"],
                "is_active": True,
                "is_published": True,
                "notification_channels": [
                    {
                        "channel_type": "slack",
                        "recipients": [
                            {"id": "C1234567890", "name": "sales-alerts", "is_channel": "true", "is_private": "false"}
                        ],
                    },
                    {
                        "channel_type": "email",
                        "recipients": [
                            {
                                "email": "team@example.com",
                            }
                        ],
                    },
                ],
            }
        },
        from_attributes=True,
    )


class AlertResponse(BaseModel):
    """Response model for alerts"""

    id: int
    name: str
    description: str | None = None
    type: NotificationType
    grain: Granularity
    trigger: AlertTrigger
    tags: list[str]
    is_published: bool
    is_active: bool
    tenant_id: int
    summary: str | None = None
    notification_channels: list[NotificationChannelDetail] = []

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class AlertUpdateRequest(BaseModel):
    """Request model for updating alerts"""

    name: str | None = None
    description: str | None = None
    grain: Granularity | None = None
    trigger: AlertTrigger | None = None
    tags: list[str] | None = None
    notification_channels: list[NotificationChannelDetail] | None = None

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Updated Alert Name",
                "description": "Updated description",
                "grain": "day",
                "trigger": {
                    "type": "METRIC_STORY",
                    "condition": {"metric_ids": ["newInqs"], "story_groups": ["TREND_CHANGES"]},
                },
                "tags": ["updated", "tag"],
                "notification_channels": [
                    {
                        "channel_type": "slack",
                        "recipients": [
                            {"id": "C1234567890", "name": "sales-alerts", "is_channel": "true", "is_private": "false"}
                        ],
                    },
                    {
                        "channel_type": "email",
                        "recipients": [
                            {
                                "email": "team@example.com",
                            }
                        ],
                    },
                ],
            }
        }
    )


class AlertPublishRequest(BaseModel):
    """Request model for updating alerts"""

    name: str | None = None
    description: str | None = None
    grain: Granularity | None = None
    trigger: AlertTrigger | None = None
    tags: list[str] | None = None
    notification_channels: list[NotificationChannelDetail] | None = None
    is_published: bool = True

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Alert Name",
                "description": "description",
                "grain": "day",
                "is_published": True,
                "trigger": {
                    "type": "METRIC_STORY",
                    "condition": {"metric_ids": ["newInqs"], "story_groups": ["TREND_CHANGES"]},
                },
                "tags": ["updated", "tag"],
                "notification_channels": [
                    {
                        "channel_type": "slack",
                        "recipients": [
                            {"id": "C1234567890", "name": "sales-alerts", "is_channel": "true", "is_private": "false"}
                        ],
                    },
                    {
                        "channel_type": "email",
                        "recipients": [
                            {
                                "email": "team@example.com",
                            }
                        ],
                    },
                ],
            }
        }
    )


class StoryPreviewData(BaseModel):
    """Sample story data for preview"""

    story_group: str
    title: str
    detail: str


class MetricInfo(BaseModel):
    """Model for metric information"""

    id: str = Field(..., example="newInqs")
    label: str = Field(..., example="New Inquiries")


class PreviewRequest(BaseModel):
    """Request model for template preview"""

    template_type: NotificationChannel
    metrics: list[MetricInfo] | None = Field(
        None,
        example=[{"id": "newInqs", "label": "New Inquiries"}],
        description="Optional list of metrics. If not provided, a sample metric will be used.",
    )
    grain: str = Field(..., example="day")
    story_groups: list[str] = Field(..., example=["TREND_CHANGES"])
    recipients: list[str] = Field(..., example=["recipient@example.com"])

    # @field_validator("metrics")
    # def set_default_metrics(self, v):
    #     if not v:
    #         return [MetricInfo(id="sample_metric", label="Sample Metric")]
    #     return v


class EmailPreviewResponse(BaseModel):
    """Response model for email template preview"""

    to_emails: list[str]
    cc_emails: list[str] = Field(default_factory=list)
    subject: str
    body: str


class SlackPreviewResponse(BaseModel):
    """Response model for slack template preview"""

    message: dict[str, Any]
    channels: list[str]


class PreviewResponse(BaseModel):
    """Response model for template preview"""

    email: EmailPreviewResponse | None = None
    slack: SlackPreviewResponse | None = None


class NotificationList(BaseModel):
    """Schema for notification listing (both alerts and reports)"""

    id: int
    name: str
    type: NotificationType
    grain: Granularity
    trigger_schedule: str
    tags: list[str]
    last_execution: datetime | None
    recipients_count: int
    status: NotificationStatus
