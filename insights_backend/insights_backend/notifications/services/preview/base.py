import logging
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from faker import Faker

from commons.notifiers.constants import NotificationChannel
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.schemas import AlertRequest
from insights_backend.notifications.services.template_service import TemplateService

logger = logging.getLogger(__name__)

T = TypeVar("T", AlertRequest, None)


class BasePreviewService(ABC, Generic[T]):
    """Base class for preview services"""

    COMMON_VARIABLES = {
        "grain": ["day", "week", "month"],
        "pop": ["d/d", "w/w", "m/m"],
        "eoi": {"day": "EOD", "week": "EOW", "month": "EOM"},
    }

    def __init__(self, template_service: TemplateService, notification_type: NotificationType):
        self.template_service = template_service
        self.notification_type = notification_type
        self.faker = Faker()

    @abstractmethod
    async def _generate_context(self, data: T) -> dict[str, Any]:
        """Generate context data for template rendering"""
        pass

    async def preview(self, data: T) -> Any:
        """Generate preview for notification channels"""
        context = await self._generate_context(data)
        preview_data = {}

        for channel in data.notification_channels:  # type: ignore
            channel_type = channel.channel_type

            # Get and render template with context
            template = self.template_service.prepare_channel_template(self.notification_type, channel_type)
            rendered_template = self.template_service.render_template(
                self.notification_type, channel.channel_type, {"template": template, **context}
            )

            if channel.channel_type == NotificationChannel.EMAIL:
                preview_data["email"] = {
                    "to_emails": [r["email"] for r in channel.recipients if r["location"] == "to"],
                    "cc_emails": [r["email"] for r in channel.recipients if r["location"] == "cc"],
                    "subject": f"[{context['metric']['label']}] New Stories Alert",
                    "body": " ".join(rendered_template.split()),
                }
            elif channel.channel_type == NotificationChannel.SLACK:
                preview_data["slack"] = {
                    "message": " ".join(rendered_template.split()),
                    "channels": [r["name"] for r in channel.recipients],
                }

        return preview_data
