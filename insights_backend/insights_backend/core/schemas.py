from datetime import datetime
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

from pydantic import ConfigDict, Field, field_validator

from commons.models import BaseModel
from commons.models.enums import Granularity
from commons.models.slack import SlackChannel
from commons.notifiers.constants import NotificationChannel
from insights_backend.core.models.notifications import (
    AlertTrigger,
    EmailRecipient,
    NotificationChannelConfig,
    NotificationChannelConfigBase,
    NotificationType,
)


class SlackChannelResponse(BaseModel):
    results: list[SlackChannel]
    next_cursor: str | None = None


class NotificationChannelDetail(BaseModel):
    """Configuration for notification channels (Slack/Email) that can be used by alerts and reports"""

    channel_type: NotificationChannel
    recipients: list[SlackChannel | EmailRecipient]
    template: str | None = None
    config: dict | None = None


class AlertCreateRequest(BaseModel):
    """Request model for creating alerts"""

    name: str
    description: str | None = None
    grain: Granularity
    trigger: AlertTrigger
    tags: list[str] = []
    is_active: bool = True
    notification_channels: list[NotificationChannelConfigBase] = []

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
                    "condition": {"metric_ids": ["sales_revenue"], "story_groups": ["TREND"]},
                },
                "tags": ["sales", "daily"],
                "is_active": True,
                "is_published": False,
                "notification_channels": [
                    {
                        "channel_type": "SLACK",
                        "recipients": [
                            {"id": "C1234567890", "name": "sales-alerts", "is_channel": "true", "is_private": "false"}
                        ],
                    },
                    {
                        "channel_type": "EMAIL",
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
    is_active: bool | None = None
    is_published: bool

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Updated Alert Name",
                "description": "Updated description",
                "grain": "day",
                "trigger": {"type": "threshold", "condition": {"operator": "gt", "value": 100}},
                "tags": ["updated", "tag"],
                "notification_channels": [{"channel_type": "slack", "recipients": ["#channel"]}],
                "is_active": True,
                "is_published": False,
            }
        }
    )


class StoryPreviewData(BaseModel):
    """Sample story data for preview"""

    story_group: str
    title: str
    detail: str


class PreviewRequest(BaseModel):
    """Request model for template preview"""

    template_type: NotificationChannel
    metric: dict[str, Any] = Field(..., example={"label": "New Inquiries", "id": "newInqs"})
    grain: str = Field(..., example="day")
    story_groups: list[str] = Field(..., example=["TREND_CHANGES"])
    recipients: list[str] = Field(..., example=["recipient@example.com"])


class PreviewResponse(BaseModel):
    """Response model for template preview"""

    preview_html: str = Field(description="HTML representation of the template for preview")
    raw_content: str = Field(description="Original rendered content (JSON for Slack, HTML for Email)")


class NotificationList(BaseModel):
    """Schema for notification listing (both alerts and reports)"""

    id: int
    name: str
    type: str  # Alert/Report
    grain: str
    trigger_schedule: str  # For Alert: trigger summary, For Report: schedule
    # created_by: str
    tags: list[str]
    last_execution: datetime | None
    recipients_count: int
    status: str  # Active/Inactive
