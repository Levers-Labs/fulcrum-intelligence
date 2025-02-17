from datetime import datetime

from commons.models import BaseModel
from commons.models.enums import Granularity
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import AlertTrigger, NotificationChannelConfig, NotificationChannelRead


class AlertRequest(BaseModel):
    """Request model for creating alerts"""

    name: str
    description: str | None = None
    grain: Granularity
    trigger: AlertTrigger
    tags: list[str] = []
    summary: str
    notification_channels: list[NotificationChannelConfig]

    class Config:
        example = {
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
                        {"name": "#channel", "is_channel": True, "is_group": False, "is_dm": False, "is_private": False}
                    ],
                },
                {"channel_type": "email", "recipients": [{"email": "user@example.com", "location": "to"}]},
            ],
        }


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

    notification_channels: list[NotificationChannelRead] | None = None

    class Config:
        from_attributes = True


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


class EmailPreviewResponse(BaseModel):
    """Response model for email template preview"""

    to_emails: list[str]
    cc_emails: list[str] | None = None
    subject: str
    body: str


class SlackPreviewResponse(BaseModel):
    """Response model for slack template preview"""

    message: str
    channels: list[str] = []


class PreviewResponse(BaseModel):
    """Response model for template preview"""

    email: EmailPreviewResponse | None = None
    slack: SlackPreviewResponse | None = None
