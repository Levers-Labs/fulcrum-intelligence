from datetime import datetime
from typing import Literal

from pydantic import EmailStr
from pydantic_extra_types.timezone_name import TimeZoneName
from sqlalchemy import (
    ARRAY,
    Boolean,
    Column,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, Relationship

from commons.models import BaseModel
from commons.models.enums import Granularity
from commons.notifiers.constants import NotificationChannel
from insights_backend.core.models import InsightsSchemaBaseModel
from insights_backend.notifications.enums import (
    Comparisons,
    ExecutionStatus,
    NotificationType,
    TriggerType,
)

# ----------------
# Base Models
# ----------------


class NotificationConfigBase(BaseModel):
    """Base configuration for all types of notifications"""

    type: NotificationType
    name: str
    description: str | None = None
    tags: list[str] = Field(sa_type=ARRAY(String), nullable=True)  # type: ignore
    grain: Granularity
    is_active: bool = Field(sa_type=Boolean, default=True)
    is_published: bool = Field(sa_type=Boolean, default=False)
    summary: str | None = None


# ----------------
# Alert Models
# ----------------


class MetricStoryTrigger(BaseModel):
    """Configuration for story-based metric triggers"""

    metric_ids: list[str] = Field(sa_column=Column(ARRAY(String), nullable=False))
    story_groups: list[str] = Field(sa_column=Column(ARRAY(String), nullable=True))

    def __str__(self) -> str:
        """Generate human-readable representation of the trigger"""
        metric_str = ", ".join(self.metric_ids)
        if not self.story_groups:
            return f"Alert new stories of metrics: {metric_str}"
        story_group_str = ", ".join(self.story_groups)
        return f"Alert new stories of metrics: {metric_str} for story groups: {story_group_str}"


class MetricThresholdTrigger(BaseModel):
    """
    Configuration for threshold-based metric triggers
    Placeholder for future implementation
    """

    pass


class AlertTrigger(BaseModel):
    """Defines the trigger configuration for alerts"""

    type: TriggerType
    condition: MetricStoryTrigger | MetricThresholdTrigger


class Alert(NotificationConfigBase, InsightsSchemaBaseModel, table=True):  # type: ignore
    """Complete alert configuration including trigger details"""

    trigger: AlertTrigger = Field(sa_type=JSONB)
    channels: list["NotificationChannelConfig"] = Relationship(back_populates="alert")

    def is_publishable(self) -> bool:
        """
        Verifies if all fields are populated for publishing by checking
        if any field contains None or empty values.
        """
        data = self.model_dump()
        return all(value is not None and value != "" and value != [] for value in data.values())


# ----------------
# Report Models
# ----------------


class ReportConfig(BaseModel):
    """Configuration for scheduled reports"""

    metric_ids: list[str] = Field(sa_column=Column(ARRAY(String), nullable=False))
    comparisons: list[Comparisons] | None = Field(sa_column=Column(ARRAY(String), nullable=True))

    def __str__(self) -> str:
        """Generate human-readable representation of the report"""
        metric_str = ", ".join(self.metric_ids)
        return f"Report metrics: {metric_str}"


class ScheduleConfig(BaseModel):
    """Configuration for notification schedule"""

    cron_expression: str
    timezone: TimeZoneName


class Report(NotificationConfigBase, InsightsSchemaBaseModel, table=True):  # type: ignore
    """Complete report configuration including schedule and metrics"""

    schedule: ScheduleConfig = Field(sa_type=JSONB)
    config: ReportConfig = Field(sa_type=JSONB)
    channels: list["NotificationChannelConfig"] = Relationship(back_populates="report")

    def is_publishable(self) -> bool:
        """
        Verifies if all fields are populated for publishing by checking
        if any field contains None or empty values.
        """
        data = self.model_dump()
        return all(value is not None and value != "" and value != [] for value in data.values())


# ----------------
# Channel Configuration Models
# ----------------
class SlackChannel(BaseModel):
    id: str
    name: str
    is_channel: bool = False
    is_group: bool = False
    is_dm: bool = False
    is_private: bool = False


class EmailRecipient(BaseModel):
    email: EmailStr
    location: Literal["to", "cc"] = "to"


class SlackTemplate(BaseModel):
    """Template configuration for Slack messages"""

    message: str


class EmailTemplate(BaseModel):
    """Template configuration for Email messages"""

    subject: str
    body: str


class NotificationChannelConfigBase(BaseModel):
    """Base configuration for notification delivery channels"""

    channel_type: NotificationChannel
    # Stores channel IDs - public/private chanel, group, DM, etc. for slack channel
    # Stores email addresses for email channel
    recipients: list[SlackChannel | EmailRecipient] = Field(default_factory=list, sa_type=JSONB)
    template: SlackTemplate | EmailTemplate = Field(sa_type=JSONB)
    # Extra config specific to each channel type
    config: dict | None = Field(default_factory=dict, sa_type=JSONB)

    def __str__(self) -> str:
        """Generate human-readable representation of the channel configuration"""
        return f"{self.channel_type} - {', '.join(str(r) for r in self.recipients)}"


class NotificationChannelConfig(NotificationChannelConfigBase, InsightsSchemaBaseModel, table=True):  # type: ignore
    """Database model for storing notification channel configurations"""

    notification_id: int = Field(nullable=False)
    notification_type: NotificationType = Field(nullable=False)

    # Relationships
    alert: "Alert" = Relationship(
        back_populates="channels",
        sa_relationship_kwargs={
            "primaryjoin": "and_(NotificationChannelConfig.notification_id == Alert.id, "
            "NotificationChannelConfig.notification_type == 'ALERT')",
            "foreign_keys": "[NotificationChannelConfig.notification_id]",
        },
    )

    report: "Report" = Relationship(
        back_populates="channels",
        sa_relationship_kwargs={
            "primaryjoin": "and_(NotificationChannelConfig.notification_id == Report.id, "
            "NotificationChannelConfig.notification_type == 'REPORT')",
            "foreign_keys": "[NotificationChannelConfig.notification_id]",
        },
    )

    class Config:
        sa_unique_constraints = [("notification_id", "notification_type", "channel_type")]


# ----------------
# Execution Models
# ----------------


class ExecutionError(BaseModel):
    """Error details for notification executions"""

    # capture the essense of the error e.g. Delivery failed, Invalid Recipient, etc.
    error_type: str = Field(default=None, sa_column=Column(String))
    # Error message
    message: str = Field(default=None, sa_column=Column(Text))


class NotificationExecutionBase(BaseModel):
    """
    Alert or Report executions
    In case of alert, the exectution will capture the alert trigger and the result
    In case of report, the exectution will capture the report config and the result
    """

    # alert_id: int | None = Field(foreign_key="insights_store.alert.id", nullable=True)
    # report_id: int | None = Field(foreign_key="insights_store.report.id", nullable=True)
    executed_at: datetime
    status: ExecutionStatus
    # List of recipients for the notification
    recipients: list[SlackChannel | EmailRecipient] = Field(default_factory=list, sa_type=JSONB)
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


class NotificationExecution(NotificationExecutionBase, InsightsSchemaBaseModel, table=True):  # type: ignore
    """Database model for storing notification executions"""

    pass


class NotificationExecutionRead(NotificationExecutionBase):
    """Read model for notification executions"""

    id: int
