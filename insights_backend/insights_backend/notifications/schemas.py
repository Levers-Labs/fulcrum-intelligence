from datetime import datetime
from typing import Any

from pydantic import ConfigDict, Field, field_validator

from commons.models import BaseModel
from commons.models.enums import Granularity
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import (
    AlertTrigger,
    NotificationChannelConfig,
    ReportConfig,
    ScheduleConfig,
)


class NotificationBase(BaseModel):
    name: str
    description: str | None = Field(default=None, description="description")
    grain: Granularity
    tags: list[str] = Field(default_factory=list, description="tags for categorizing notifications")
    summary: str
    notification_channels: list[NotificationChannelConfig] = Field(
        default_factory=list, description="notification channel configurations"
    )

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


class AlertRequest(NotificationBase):
    """Request model for creating alerts"""

    trigger: AlertTrigger

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


class AlertDetail(NotificationBase):
    """Model for reading alert with its notification channels"""

    id: int
    trigger: AlertTrigger

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)


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
    is_active: bool

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)


class ReportRequest(NotificationBase):
    """Request model for creating reports"""

    schedule: ScheduleConfig
    config: ReportConfig

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "name": "report name",
                "description": "report description",
                "grain": "day",
                "schedule": {
                    "minute": "0",
                    "hour": "9",
                    "day_of_month": "*",
                    "month": "*",
                    "day_of_week": "MON",
                    "timezone": "America/New_York",
                },
                "config": {"metric_ids": ["NewBizDeals", "NewWins"], "comparisons": ["PERCENTAGE_CHANGE"]},
                "summary": "Daily report summary",
                "notification_channels": [
                    {
                        "channel_type": "slack",
                        "recipients": [
                            {
                                "name": "#daily-metrics",
                                "is_channel": True,
                                "is_group": False,
                                "is_dm": False,
                                "is_private": False,
                            }
                        ],
                    },
                    {
                        "channel_type": "email",
                        "recipients": [
                            {"email": "team@example.com", "location": "to"},
                            {"email": "manager@example.com", "location": "cc"},
                        ],
                    },
                ],
            }
        },
    )


class ReportDetail(NotificationBase):
    id: int
    schedule: ScheduleConfig
    config: ReportConfig

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)
