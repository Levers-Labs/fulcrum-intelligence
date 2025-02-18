from datetime import datetime
from typing import Any

from pydantic import ConfigDict, Field, field_validator

from commons.models import BaseModel
from commons.models.enums import Granularity
from commons.notifiers.constants import NotificationChannel
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import AlertTrigger, NotificationChannelConfig


class AlertRequest(BaseModel):
    """Request model for creating alerts"""

    name: str
    description: str | None = None
    grain: Granularity
    trigger: AlertTrigger
    tags: list[str] = []
    summary: str
    notification_channels: list[NotificationChannelConfig]

    @field_validator("notification_channels")
    @classmethod
    def validate_unique_channels(cls, channels: list[Any]) -> list[Any]:
        """Validate that each channel type appears only once"""
        channel_types = [channel.channel_type for channel in channels]
        if len(set(channel_types)) < len(channel_types):
            raise ValueError("Each channel type can only be used once")
        return channels

    @field_validator("tags")
    @classmethod
    def validate_unique_tags(cls, v: list[str]) -> list[str]:
        """Ensure tags are unique and non-empty"""
        return list(dict.fromkeys(tag.strip() for tag in v if tag.strip()))

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "name": "alert name",
                "description": "alert description",
                "grain": "day",
                "trigger": {
                    "type": "METRIC_STORY",
                    "condition": {"metric_ids": ["NewBizDeals"], "story_groups": ["TREND_CHANGES"]},
                },
                "summary": "summary",
                "notification_channels": [
                    {
                        "channel_type": "slack",
                        "recipients": [
                            {
                                "name": "#channel",
                                "is_channel": True,
                                "is_group": False,
                                "is_dm": False,
                                "is_private": False,
                            }
                        ],
                    },
                    {"channel_type": "email", "recipients": [{"email": "user@example.com", "location": "to"}]},
                ],
            }
        },
    )


class AlertWithChannelsResponse(BaseModel):
    """Model for reading alert with its notification channels"""

    id: int
    name: str
    description: str | None = None
    type: NotificationType
    grain: Granularity
    trigger: AlertTrigger
    tags: list[str] | None = None
    is_published: bool
    is_active: bool
    summary: str | None = None

    notification_channels: list[NotificationChannelConfig]

    model_config = {
        "from_attributes": True,
        "arbitrary_types_allowed": True,
    }


class NotificationList(BaseModel):
    """Schema for notification listing (both alerts and reports)"""

    id: int
    name: str
    type: NotificationType
    grain: Granularity
    summary: str
    tags: list[str] | None = None
    last_execution: datetime | None = None
    recipients_count: int | None
    status: bool

    class Config:
        from_attributes = True


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
    grain: Granularity
    story_groups: list[str]
    recipients: list[str]


class EmailPreviewResponse(BaseModel):
    """Response model for email template preview"""

    to_emails: list[str]
    cc_emails: list[str] | None = None
    subject: str
    body: str


class SlackPreviewResponse(BaseModel):
    """Response model for slack template preview"""

    message: dict[str, Any]
    channels: list[str] = []


class PreviewResponse(BaseModel):
    """Response model for template preview"""

    email: EmailPreviewResponse | None = None
    slack: SlackPreviewResponse | None = None
