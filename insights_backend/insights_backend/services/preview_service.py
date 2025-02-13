import html
import json
import logging
import os
from datetime import datetime

from fastapi import HTTPException
from jinja2 import Environment, FileSystemLoader, Template

from commons.notifiers.constants import NotificationChannel
from insights_backend.core.schemas import MetricInfo

logger = logging.getLogger(__name__)


class PreviewService:
    """Service for previewing notification templates"""

    def __init__(self, template_dir: str):
        self.env = Environment(
            loader=FileSystemLoader(template_dir), autoescape=True, trim_blocks=True, lstrip_blocks=True
        )
        self.alert_template = "alert_template"

    def get_template(self, template_name: str, template_type: str) -> Template:
        """Load template from file"""
        template_path = (
            os.path.join(template_type, f"{template_name}.json")
            if template_type == NotificationChannel.SLACK
            else os.path.join(template_type, f"{template_name}.html")
        )
        return self.env.get_template(template_path)

    def generate_mock_data(self, story_groups: list[str], metrics: list[MetricInfo], grain: str) -> dict:
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
        mock_data = self.generate_mock_data(preview_data.story_groups, preview_data.metrics, preview_data.grain)

        try:
            context = self._get_base_context(mock_data)
            rendered_content = self._render_template(preview_data.template_type, context)

            if preview_data.template_type == NotificationChannel.SLACK:
                return {"slack": {"message": json.loads(rendered_content), "channels": preview_data.recipients}}
            else:
                email_html = " ".join(rendered_content.split())
                email_html = html.unescape(email_html)
                return {
                    "email": {
                        "to_emails": preview_data.recipients,
                        "cc_emails": [],
                        "subject": f"[{mock_data['metric']['label']}] New Stories Alert",
                        "body": email_html,
                    }
                }

        except Exception as e:
            logger.error("Template rendering failed: %s", str(e))
            raise HTTPException(status_code=400, detail=f"Template rendering failed: {str(e)}") from e

    def _render_template(self, template_type: NotificationChannel, context: dict) -> str:
        """Common method to render templates with given context"""
        template = self.get_template(self.alert_template, template_type)
        return template.render(**context)

    def _get_base_context(self, mock_data: dict) -> dict:
        """Get base context for template rendering"""
        return {
            "metric": mock_data["metric"],
            "grain": mock_data["grain"],
            "stories": mock_data["stories"],
            "time": mock_data["time"],
            "metric_id": mock_data["metric"]["id"],
        }
