from abc import ABC, abstractmethod
from datetime import datetime
from typing import Literal

from pydantic import EmailStr, conint
from pydantic_extra_types.timezone_name import TimeZoneName
from sqlalchemy import (
    ARRAY,
    Boolean,
    Column,
    ForeignKeyConstraint,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, Relationship

from commons.models import BaseModel
from commons.models.enums import Granularity
from commons.notifiers.constants import NotificationChannel
from insights_backend.core.models.base import InsightsSchemaBaseModel
from insights_backend.notifications.enums import (
    Comparisons,
    DayOfWeek,
    ExecutionStatus,
    Month,
    NotificationType,
    TriggerType,
)

# ----------------
# Base Models
# ----------------


class NotificationConfigBase(BaseModel, ABC):
    """Base configuration for all types of notifications"""

    type: NotificationType
    name: str
    description: str | None = None
    tags: list[str] = Field(sa_type=ARRAY(String), nullable=True)  # type: ignore
    grain: Granularity
    is_active: bool = Field(sa_type=Boolean, default=True)
    is_published: bool = Field(sa_type=Boolean, default=False)
    summary: str | None = None

    @abstractmethod
    def is_publishable(self) -> bool:
        """
        Verifies if all required fields are populated for publishing.
        Must be implemented by child classes with their specific requirements.
        """
        pass


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
    executions: list["NotificationExecution"] = Relationship(back_populates="alert")

    def is_publishable(self) -> bool:
        return bool(self.name) and bool(self.trigger) and bool(self.channels)


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

    minute: conint(ge=0, le=59) | str = Field(default="*", description="Minute (0-59 or */n)")  # type: ignore
    hour: conint(ge=0, le=23) | str = Field(default="*", description="Hour (0-23 or */n)")  # type: ignore
    day_of_month: conint(ge=1, le=31) | str = Field(default="*", description="Day of month (1-31 or */n)")  # type: ignore
    month: Month | str = Field(default="*", description="Month (JAN-DEC or */n)")
    day_of_week: DayOfWeek | str = Field(default="*", description="Day of week (MON-SUN or */n)")
    timezone: TimeZoneName
    cron_expression: str = Field(description="Generated cron expression")

    # TODO: add generate cron expression and validator method


class Report(NotificationConfigBase, InsightsSchemaBaseModel, table=True):  # type: ignore
    """Complete report configuration including schedule and metrics"""

    schedule: ScheduleConfig = Field(sa_type=JSONB)
    config: ReportConfig = Field(sa_type=JSONB)
    channels: list["NotificationChannelConfig"] = Relationship(back_populates="report")
    executions: list["NotificationExecution"] = Relationship(back_populates="report")

    def is_publishable(self) -> bool:
        return bool(self.name) and bool(self.schedule) and bool(self.config) and bool(self.channels)


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

    __table_args__ = (  # type: ignore
        ForeignKeyConstraint(
            ["notification_id"], ["insights_store.alert.id"], name="fk_notification_alert", use_alter=True
        ),
        ForeignKeyConstraint(
            ["notification_id"], ["insights_store.report.id"], name="fk_notification_report", use_alter=True
        ),
        UniqueConstraint("notification_id", "notification_type", "channel_type", name="uq_notification_channel"),
        {"schema": "insights_store"},
    )

    notification_id: int = Field(nullable=False)
    notification_type: NotificationType = Field(nullable=False)

    # Relationships
    alert: "Alert" = Relationship(
        back_populates="channels",
        sa_relationship_kwargs={
            "primaryjoin": "and_(NotificationChannelConfig.notification_id == Alert.id, "
            "NotificationChannelConfig.notification_type == 'ALERT')",
            "viewonly": False,
        },
    )

    report: "Report" = Relationship(
        back_populates="channels",
        sa_relationship_kwargs={
            "primaryjoin": "and_(NotificationChannelConfig.notification_id == Report.id, "
            "NotificationChannelConfig.notification_type == 'REPORT')",
            "viewonly": False,
        },
    )


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

    __table_args__ = (  # type: ignore
        ForeignKeyConstraint(
            ["notification_id"], ["insights_store.alert.id"], name="fk_execution_alert", use_alter=True
        ),
        ForeignKeyConstraint(
            ["notification_id"], ["insights_store.report.id"], name="fk_execution_report", use_alter=True
        ),
        {"schema": "insights_store"},
    )

    notification_id: int = Field(nullable=False)
    notification_type: NotificationType = Field(
        nullable=False,
    )

    # Relationships
    alert: "Alert" = Relationship(
        back_populates="executions",
        sa_relationship_kwargs={
            "primaryjoin": "and_(NotificationExecution.notification_id == Alert.id, "
            "NotificationExecution.notification_type == 'ALERT')",
            "viewonly": False,
        },
    )

    report: "Report" = Relationship(
        back_populates="executions",
        sa_relationship_kwargs={
            "primaryjoin": "and_(NotificationExecution.notification_id == Report.id, "
            "NotificationExecution.notification_type == 'REPORT')",
            "viewonly": False,
        },
    )


class NotificationExecutionRead(NotificationExecutionBase):
    """Read model for notification executions"""

    id: int
