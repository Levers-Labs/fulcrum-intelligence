import logging
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from faker import Faker

from commons.notifiers.constants import NotificationChannel
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.schemas import AlertRequest, ReportRequest
from insights_backend.notifications.services.template_service import TemplateService

logger = logging.getLogger(__name__)

T = TypeVar("T", AlertRequest, ReportRequest)


class BasePreviewService(ABC, Generic[T]):
    """Base class for preview services"""

    COMMON_VARIABLES = {
        "day": {"pop": "d/d", "delta": {"days": 1}, "eoi": "EOD", "interval": "daily"},
        "week": {"pop": "w/w", "delta": {"weeks": 1}, "eoi": "EOW", "interval": "weekly"},
        "month": {"pop": "m/m", "delta": {"months": 1}, "eoi": "EOM", "interval": "monthly"},
    }

    def __init__(self, template_service: TemplateService, notification_type: NotificationType):
        self.template_service = template_service
        self.notification_type = notification_type
        self.faker = Faker()

    @abstractmethod
    async def _generate_context(self, data: T) -> dict[str, Any]:
        """Generate context data for template rendering"""
        pass

    def _get_email_subject(self, context: dict[str, Any]) -> str:
        """Generate email subject based on notification type"""
        if self.notification_type == NotificationType.ALERT:
            return f"[{context['metric']['label']}] New Stories Alert"
        else:  # Report
            return f"📊 {context['data']['interval'].title()} Report for {context['config']['name']}"

    async def preview(self, data: T) -> dict[str, Any]:
        """Generate preview for notification channels"""
        context = await self._generate_context(data)
        preview_data = {}

        for channel in data.notification_channels:
            rendered_template = self.template_service.render_template(
                self.notification_type, channel.channel_type, context
            )

            if channel.channel_type == NotificationChannel.EMAIL:
                preview_data["email"] = {
                    "to_emails": [r.email for r in channel.recipients if r.location == "to"],  # type: ignore
                    "cc_emails": [r.email for r in channel.recipients if r.location == "cc"],  # type: ignore
                    "subject": self._get_email_subject(context),
                    "body": " ".join(rendered_template.split()),
                }
            elif channel.channel_type == NotificationChannel.SLACK:
                preview_data["slack"] = {
                    "message": " ".join(rendered_template.split()),
                    "channels": [r.name for r in channel.recipients],  # type: ignore
                }

        return preview_data
