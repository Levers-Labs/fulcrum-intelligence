import json
import random
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Template

from insights_backend.notifications.enums import NotificationType
from insights_backend.notifications.schemas import AlertRequest
from insights_backend.notifications.services.preview.base import BasePreviewService


class AlertPreviewService(BasePreviewService[AlertRequest]):
    """Service for previewing alert notifications"""

    def __init__(self, template_service):
        super().__init__(template_service, NotificationType.ALERT)
        # Load story templates
        commons_path = Path(__file__).parent.parent.parent.parent.parent.parent / "commons" / "commons"
        with open(commons_path / "templates" / "story_templates.json") as f:
            self.story_template = json.load(f)

    async def _generate_context(self, alert_data: AlertRequest) -> dict[str, Any]:
        """Generate mock context data for alert template rendering"""
        story_groups = self._get_story_groups(alert_data)
        metric = self._get_metric_info(alert_data)

        # Generate common variables that stories will need
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "start_date": self.faker.date_time_this_month().strftime("%B %d, %Y"),
            "current_growth": self.faker.random_int(min=-90, max=90),  # type: ignore
            "prev_duration": self.faker.random_int(min=1, max=12),  # type: ignore
            "prev_growth": self.faker.random_int(min=-50, max=50),  # type: ignore
            "grain": alert_data.grain.value,
            "pop": self.COMMON_VARIABLES["pop"][self.COMMON_VARIABLES["grain"].index(alert_data.grain.value.lower())],  # type: ignore
            "avg_value": self.faker.random_int(min=100, max=10000),  # type: ignore
            "deviation": self.faker.random_int(min=-50, max=50),  # type: ignore
            "position": self.faker.random_element(["above", "below"]),  # type: ignore
            "duration": self.faker.random_int(min=1, max=12),  # type: ignore
            "avg_growth": self.faker.random_int(min=-50, max=50),  # type: ignore
            "overall_growth": self.faker.random_int(min=-90, max=90),  # type: ignore
        }

        stories = self._generate_stories(story_groups, variables)

        return {
            "metric": metric,
            "grain": alert_data.grain.value,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "stories": stories,
            **variables,
        }

    def _get_story_groups(self, alert_data: AlertRequest) -> list[str]:
        """Get story groups from alert data"""
        if not alert_data.trigger or not alert_data.trigger.condition:
            return []
        return getattr(alert_data.trigger.condition, "story_groups", ["TREND_CHANGES"])

    def _get_metric_info(self, alert_data: AlertRequest) -> dict[str, str]:
        """Get metric information"""

        # If no metrics in alert_data, check trigger condition
        if alert_data.trigger and alert_data.trigger.condition:
            metric_ids = getattr(alert_data.trigger.condition, "metric_ids", [])
            if metric_ids:
                return {"id": metric_ids[0], "label": metric_ids[0]}

        # If no metrics found anywhere, generate fake data
        return {"id": str(self.faker.random_int(min=1, max=100)), "label": self.faker.word().title()}  # type: ignore

    def _generate_stories(self, story_groups: list[str], variables: dict[str, Any]) -> list[dict[str, str]]:
        """Generate stories for each story group"""
        stories = []
        for group in story_groups:
            if group in self.story_template:
                story_type = random.choice(list(self.story_template[group].keys()))  # noqa
                template = self.story_template[group][story_type]
                stories.append(
                    {
                        "story_group": group,
                        "title": Template(template["title"]).render(**variables),
                        "detail": Template(template["detail"]).render(**variables),
                    }
                )
        return stories
