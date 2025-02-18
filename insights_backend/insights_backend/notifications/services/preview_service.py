import html
import json
import logging
from datetime import datetime

from fastapi import HTTPException

from commons.notifiers.constants import NotificationChannel
from insights_backend.notifications.enums import NotificationType

logger = logging.getLogger(__name__)


class PreviewService:
    """Service for previewing notification templates"""

    def __init__(self, template_service):
        self.template_service = template_service

    def generate_mock_data(self, story_groups: list[str], metrics: list[dict], grain: str) -> dict:
        """Generate mock data for previewing notifications."""
        current_time = datetime.utcnow()

        mock_data = {
            "stories": [],
            "metric": {
                "id": metrics[0].id,
                "label": metrics[0].label,
            },
            "grain": grain,
            "time": current_time.strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Just a few representative stories that show how notifications will look
        mock_story = {
            "title": f"Sample story for {metrics[0].label}",
            "detail": f"This is a preview of how your {metrics[0].label} stories will look. "
            f"Real stories will contain actual insights about your metrics.",
        }

        # Add one story per requested group
        for group in story_groups:
            mock_data["stories"].append(
                {"story_group": group, "title": mock_story["title"], "detail": mock_story["detail"]}
            )

        return mock_data

    def preview_template(self, preview_data) -> dict:
        """Generate a preview of the notification template"""

        try:
            context = self._get_base_context(preview_data)
            rendered_content = self.template_service.render_template(
                NotificationType.ALERT, preview_data.template_type, context
            )

            if preview_data.template_type == NotificationChannel.SLACK:
                return {"slack": {"message": json.loads(rendered_content), "channels": preview_data.recipients}}
            else:
                email_html = " ".join(rendered_content.split())
                email_html = html.unescape(email_html)
                return {
                    "email": {
                        "to_emails": preview_data.recipients,
                        "cc_emails": [],
                        "subject": f"[{context['metric_label']}] New Stories Alert",
                        "body": email_html,
                    }
                }

        except Exception as e:
            logger.error("Template rendering failed: %s", str(e))
            raise HTTPException(status_code=400, detail=f"Template rendering failed: {str(e)}") from e

    def _get_base_context(self, preview_data) -> dict:
        """Get base context for template rendering"""

        mock_data = self.generate_mock_data(preview_data.story_groups, preview_data.metrics, preview_data.grain)

        return {
            "metric": mock_data["metric"],
            "grain": mock_data["grain"],
            "stories": mock_data["stories"],
            "time": mock_data["time"],
            "metric_id": mock_data["metric"]["id"],
            "metric_label": mock_data["metric"]["label"],
        }
