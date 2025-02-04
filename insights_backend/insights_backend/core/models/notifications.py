from datetime import datetime, time
from enum import Enum
from typing import Any, List, Literal

from pydantic import (
    ConfigDict,
    EmailStr,
    computed_field,
    field_validator,
    model_validator,
)
from pydantic_core.core_schema import ValidationInfo
from sqlalchemy import (
    ARRAY,
    Boolean,
    Column,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel

from commons.models import BaseModel
from commons.models.enums import Granularity
from commons.notifiers.constants import NotificationChannel
from insights_backend.core.enums import DailySchedule, MonthlySchedule, WeeklySchedule
from insights_backend.core.models import InsightsSchemaBaseModel

# ----------------
# Enum Definitions
# ----------------


class NotificationType(str, Enum):
    """Types of notifications supported by the system"""

    ALERT = "ALERT"  # For real-time alerts and notifications
    REPORT = "REPORT"  # For scheduled reports


class TriggerType(str, Enum):
    """Types of triggers that can generate notifications"""

    METRIC_STORY = "METRIC_STORY"  # Story-based triggers for metric insights
    METRIC_THRESHOLD = "METRIC_THRESHOLD"  # Threshold-based triggers for metric monitoring


class Comparisons(str, Enum):
    """Types of comparisons available for metric analysis"""

    PERCENTAGE_CHANGE = "PERCENTAGE_CHANGE"  # Relative change in percentage
    ABSOLUTE_CHANGE = "ABSOLUTE_CHANGE"  # Absolute numerical change


class ExecutionStatus(str, Enum):
    """Status of the notification execution"""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


# ----------------
# Base Models
# ----------------


class ScheduleConfig(BaseModel):
    """Configuration for notification schedule"""

    frequency: str  # e.g., "every_day", "monday", "first_day"
    time: time  # e.g., 09:00:00
    timezone: str  # e.g., "UTC", "America/New_York"

    @computed_field
    @property
    def time_str(self) -> str:
        """Return time as string in HH:MM:SS format"""
        return self.time.strftime("%H:%M:%S")

    @field_validator("frequency")
    @classmethod
    def validate_schedule_value(cls, v: str, info: ValidationInfo) -> str:
        # Get the parent model's grain from context
        grain = info.context.get("grain") if info.context else None

        if grain == Granularity.DAY:
            if v not in [opt for opt in DailySchedule]:
                raise ValueError(f"Invalid daily schedule value: {v}")
        elif grain == Granularity.WEEK:
            if v not in [opt for opt in WeeklySchedule]:
                raise ValueError(f"Invalid weekly schedule value: {v}")
        elif grain == Granularity.MONTH:
            if v not in [opt for opt in MonthlySchedule]:
                raise ValueError(f"Invalid monthly schedule value: {v}")
        return v

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        import pytz

        if v not in pytz.all_timezones:
            raise ValueError(f"Invalid timezone: {v}")
        return v


class NotificationConfigBase(BaseModel):
    """Base configuration for all types of notifications"""

    type: NotificationType
    name: str
    description: str | None = None
    tags: list[str] = Field(sa_column=Column(ARRAY(String), nullable=True, index=True))
    grain: Granularity
    is_active: bool = Field(sa_column=Column(Boolean, default=True))
    is_published: bool = Field(sa_column=Column(Boolean, default=True))
    summary: str | None = None


# ----------------
# Alert Models
# ----------------


class MetricStoryTrigger(BaseModel):
    """Configuration for story-based metric triggers"""

    metric_ids: list[str] = Field(sa_column=Column(ARRAY(String), nullable=True))
    story_groups: list[str] = Field(sa_column=Column(ARRAY(String), nullable=True))

    def __str__(self) -> str:
        """Generate human-readable representation of the trigger"""
        metric_str = ", ".join(self.metric_ids)
        if not self.story_groups:
            return f"Alert new stories of metrics: {metric_str}"
        story_group_str = ", ".join(self.story_groups)
        return f"Alert on New Stories for Groups: {story_group_str} & Metrics: {metric_str} "


class MetricThresholdTrigger(BaseModel):
    """
    Configuration for threshold-based metric triggers
    Placeholder for future implementation
    """

    pass


class AlertTrigger(BaseModel):
    """Defines the trigger configuration for alerts"""

    type: TriggerType
    condition: MetricStoryTrigger | MetricThresholdTrigger  # noqa


class Alert(NotificationConfigBase, InsightsSchemaBaseModel, table=True):
    """Complete alert configuration including trigger details"""

    trigger: AlertTrigger = Field(sa_column=Column(JSONB))


# ----------------
# Report Models
# ----------------


class ReportConfig(BaseModel):
    """Configuration for scheduled reports"""

    metric_ids: list[str]
    comparisons: list[Comparisons] | None = None

    def __str__(self) -> str:
        """Generate human-readable representation of the report"""
        metric_str = ", ".join(self.metric_ids)
        return f"Report metrics: {metric_str}"


# class Report(NotificationConfigBase, InsightsSchemaBaseModel, table=True):
#     """Complete report configuration including schedule and metrics"""
#     schedule: ScheduleConfig = Field(default_factory=dict, sa_type=JSONB)
#     config: ReportConfig = Field(sa_type=JSONB)


# ----------------
# Channel Configuration Models
# ----------------
class SlackChannel(BaseModel):
    id: str
    name: str
    is_channel: bool = False
    is_group: bool = False
    is_im: bool = False
    is_private: bool = False


class EmailRecipient(BaseModel):
    email: EmailStr
    location: Literal["to", "cc"] = "to"


class NotificationChannelConfigBase(BaseModel):
    """Base configuration for notification delivery channels"""

    channel_type: NotificationChannel
    # Stores channel IDs - public/private channel, group, DM, etc. for slack channel
    # Stores email addresses /for email channel
    recipients: list[SlackChannel | EmailRecipient] = Field(sa_type=JSONB)
    template: str | None = Field(sa_column=Column(Text, nullable=True))
    # Extra config specific to each channel type
    config: dict | None = Field(default_factory=dict, sa_type=JSONB)

    def __str__(self) -> str:
        """Generate human-readable representation of the channel configuration"""
        return f"{self.channel_type} - {', '.join(self.recipients)}"


class NotificationChannelConfig(NotificationChannelConfigBase, InsightsSchemaBaseModel, table=True):
    """Database model for storing notification channel configurations"""

    # Foreign key to the alert that this channel is associated with
    alert_id: int | None = Field(foreign_key="insights_store.alert.id", nullable=True)
    # Foreign key to the report that this channel is associated with
    # report_id: int | None = Field(foreign_key="insights_store.report.id", nullable=True)


# ----------------
# Execution Models
# ----------------


class ExecutionError(BaseModel):
    """Error details for notification executions"""

    # capture the essence of the error e.g. Delivery failed, Invalid Recipient, etc.
    error_type: str = Field(default=None, sa_column=Column(String))
    # Error message
    message: str = Field(default=None, sa_column=Column(Text))


class NotificationExecutionBase(BaseModel):
    """
    Alert or Report executions
    In case of alert, the exectution will capture the alert trigger and the result
    In case of report, the exectution will capture the report config and the result
    """

    alert_id: int | None = Field(foreign_key="insights_store.alert.id", nullable=True)
    # report_id: int | None = Field(foreign_key="insights_store.report.id", nullable=True)
    executed_at: datetime
    status: ExecutionStatus
    # List of recipients for the notification
    recipients: list[SlackChannel | EmailRecipient] = Field(sa_type=JSONB)
    # todo: define schema for trigger_meta, report_meta and delivery_meta
    # Metadata about the trigger and content
    # Trigger that caused the execution, e.g. metric_id , story_groups etc.
    trigger_meta: dict = Field(default_factory=dict, sa_type=JSONB)
    # Report info included in the execution e.g. Metric ids etc.
    report_meta: dict = Field(default_factory=dict, sa_type=JSONB)
    # Delivery metadata e.g. slack message id, email message id etc.
    # Also can be used to capture partial delivery failure
    # e.g. slack channel id not found, invalid email address etc.
    delivery_meta: dict = Field(default_factory=dict, sa_type=JSONB)
    # Error details if execution failed
    error_info: ExecutionError | None = Field(default=None, sa_type=JSONB)


class NotificationExecution(NotificationExecutionBase, InsightsSchemaBaseModel, table=True):
    """Database model for storing notification executions"""

    pass


class NotificationExecutionRead(NotificationExecutionBase):
    """Read model for notification executions"""

    id: int
