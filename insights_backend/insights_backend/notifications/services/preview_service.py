import json
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from faker import Faker
from jinja2 import Template

from commons.notifiers.constants import NotificationChannel
from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.schemas import AlertRequest
from insights_backend.notifications.services.template_service import TemplateService

logger = logging.getLogger(__name__)


class PreviewService:
    """Service for previewing notification templates"""

    # Define common variables for story rendering
    STORY_COMMON_VARIABLES = {
        "grain": ["day", "week", "month"],
        "pop": ["d/d", "w/w", "m/m"],
        "eoi": {"day": "EOD", "week": "EOW", "month": "EOM"},
    }

    def __init__(self, template_service: TemplateService):
        """
        Initialize the PreviewService with a TemplateService instance.

        :param template_service: The service for managing and rendering templates.
        """
        self.template_service = template_service
        self.faker = Faker()
        # Load story templates from commons
        commons_path = Path(__file__).parent.parent.parent.parent.parent / "commons" / "commons"
        with open(commons_path / "templates" / "story_templates.json") as f:
            self.story_template = json.load(f)

    def _get_story_groups(self, alert_data: AlertRequest) -> list[str]:
        """
        Extract story groups from alert data with default fallback.

        :param alert_data: The alert request data.
        :return: A list of story groups.
        """
        return getattr(alert_data.trigger.condition, "story_groups", ["GROWTH_RATES"])

    def _get_metric_info(self, metric_ids: list[str] | None = None) -> dict[str, str]:
        """
        Generate metric information, either from provided IDs or mock data.

        :param metric_ids: Optional list of metric IDs.
        :return: A dictionary containing metric information.
        """
        if metric_ids:
            metric_id = random.choice(metric_ids)  # noqa
            return {"id": metric_id, "label": metric_id}

        # Generate mock metric if no IDs provided
        mock_word = self.faker.word()
        return {"id": f"mock_metric_{mock_word}", "label": f"{mock_word.title()} Metric"}

    def _prepare_story_variables(self, grain: str, metric: dict[str, str]) -> dict[str, Any]:
        """
        Prepare all variables needed for story rendering.

        :param grain: The grain of the data (e.g., day, week, month).
        :param metric: The metric information.
        :return: A dictionary containing all variables for story rendering.
        """
        # Get grain-specific variables
        selected_grain = grain.lower() if grain else "day"
        grain_index = self.STORY_COMMON_VARIABLES["grain"].index(selected_grain)  # type: ignore

        common_vars = {
            "grain": selected_grain,
            "pop": self.STORY_COMMON_VARIABLES["pop"][grain_index],  # type: ignore
            "eoi": self.STORY_COMMON_VARIABLES["eoi"][selected_grain],  # type: ignore
        }

        # Generate story-specific variables
        story_vars = {
            "current_growth": random.randint(-90, 90),  # noqa
            "avg_growth": random.randint(10, 500),  # noqa
            "duration": random.randint(5, 30),  # noqa
            "prev_duration": random.randint(20, 40),  # noqa
            "prev_growth": random.randint(100, 400),  # noqa
            "trend_start_date": (datetime.now() - timedelta(days=random.randint(5, 30))).strftime("%B %d, %Y"),  # noqa
            "start_date": (datetime.now() - timedelta(days=random.randint(5, 30))).strftime("%B %d, %Y"),  # noqa
            "overall_growth": random.randint(-50, 50),  # noqa
            "avg_value": random.randint(1, 100),  # noqa
            "deviation": random.randint(-50, 50),  # noqa
            "position": random.choice(["above", "below"]),  # noqa
        }

        # Combine all variables
        return {**common_vars, **story_vars, "metric": metric}

    def _render_story_template(self, template_str: str, variables: dict) -> str:
        """
        Render a story template with given variables.

        :param template_str: The template string.
        :param variables: The variables to replace in the template.
        :return: The rendered story template.
        """
        return Template(template_str).render(**variables)

    async def _generate_story_context(self, alert_data: AlertRequest) -> dict[str, Any]:
        """
        Generate mock context data for template rendering.

        :param alert_data: The alert request data.
        :return: A dictionary containing the mock context data.
        """
        # 1. Process alert request data
        story_groups = self._get_story_groups(alert_data)
        metric = self._get_metric_info(getattr(alert_data.trigger.condition, "metric_ids", []))

        # 2. Prepare all variables for story rendering
        vars_to_replace = self._prepare_story_variables(alert_data.grain, metric)

        # 3. Generate stories with rendered title and detail
        stories = []
        for group in story_groups:
            if group in self.story_template:
                # Randomly select one story type from this group
                story_type = random.choice(list(self.story_template[group].keys()))  # noqa
                story_template = self.story_template[group][story_type]

                # Render both title and detail using helper method
                title = self._render_story_template(story_template["title"], vars_to_replace)
                detail = self._render_story_template(story_template["detail"], vars_to_replace)

                stories.append({"story_group": group, "title": title, "detail": detail})

        return {
            "metric": metric,
            "grain": alert_data.grain,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "stories": stories,
        }

    async def preview_alert(self, alert_data: AlertRequest) -> Any:
        """
        Generate preview for alert notification.

        :param alert_data: The alert request data.
        :return: Preview data for the alert notification.
        """
        # Generate mock context based on alert trigger
        context = await self._generate_story_context(alert_data)

        preview_data = {}
        for channel in alert_data.notification_channels:
            channel_type = channel.channel_type

            # Get and render template with context
            template = self.template_service.prepare_channel_template(NotificationType.ALERT, channel_type)
            rendered_content = self.template_service.render_template(
                NotificationType.ALERT, channel_type, {"template": template, **context}
            )

            # Prepare channel-specific data
            if channel_type == NotificationChannel.EMAIL:
                preview_data["email"] = {
                    "to_emails": [r["email"] for r in channel.recipients if r["location"] == "to"],
                    "cc_emails": [r["email"] for r in channel.recipients if r["location"] == "cc"],
                    "subject": f"[{context['metric']['label']}] New Stories Alert",
                    "body": " ".join(rendered_content.split()),
                }
            elif channel_type == NotificationChannel.SLACK:
                preview_data["slack"] = {
                    "message": " ".join(rendered_content.split()),
                    "channels": [r["name"] for r in channel.recipients if r.get("is_channel")],
                }

        # Let Pydantic handle the model creation
        return preview_data
