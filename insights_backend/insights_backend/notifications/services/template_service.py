import html
import logging
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, Template

from commons.notifiers.constants import NotificationChannel
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.models import EmailTemplate, SlackTemplate

logger = logging.getLogger(__name__)


class TemplateService:
    """Service for managing notification templates."""

    def __init__(self):
        """
        Initialize template service with template directory.
        """
        self.template_dir = Path(__file__).parent.parent / "templates"
        self.env = Environment(
            loader=FileSystemLoader(self.template_dir), autoescape=True, trim_blocks=True, lstrip_blocks=True
        )

    def _get_template(
        self,
        notification_type: NotificationType,
        channel_type: NotificationChannel,
    ) -> Template:
        """
        Get template for specific notification and channel type.

        Args:
            notification_type: Type of notification (ALERT/REPORT)
            channel_type: Type of channel (EMAIL/SLACK)

        Returns:
            Jinja template object
        """
        file_extension = "json" if channel_type == NotificationChannel.SLACK else "html"
        template_path = Path(channel_type.lower(), f"{notification_type.lower()}.{file_extension}")
        return self.env.get_template(str(template_path))

    def prepare_channel_template(
        self,
        notification_type: NotificationType,
        channel_type: NotificationChannel,
    ) -> SlackTemplate | EmailTemplate:
        """
        Build template model based on channel type.

        Args:
            notification_type: Type of notification (ALERT/REPORT)
            channel_type: Type of channel (EMAIL/SLACK)

        Returns:
            Template model as either SlackTemplate or EmailTemplate
        """
        template = self._get_template(notification_type, channel_type)
        content = self.env.loader.get_source(self.env, template.name)[0]

        if channel_type == NotificationChannel.SLACK:
            content = " ".join(content.split())
            return SlackTemplate(message=content)
        else:
            content = html.unescape(" ".join(content.split()))
            return EmailTemplate(subject="Alert Notification", body=content)

    def render_template(
        self,
        notification_type: NotificationType,
        channel_type: NotificationChannel,
        context: dict,
    ) -> str:
        """
        Render template with provided context.

        Args:
            notification_type: Type of notification (ALERT/REPORT)
            channel_type: Type of channel (EMAIL/SLACK)
            context: Template variables for rendering

        Returns:
            Rendered template as string
        """
        template = self._get_template(notification_type, channel_type)
        return template.render(**context)
