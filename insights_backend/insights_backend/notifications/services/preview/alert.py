import json
import random
from datetime import datetime, timedelta
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

        # Core variable generation with realistic relationships
        current_date = self.faker.date_time_this_month()
        variables = {
            # Metric basics
            "metric": {"metric_id": metric["metric_id"], "label": metric["label"]},
            # Time and grain
            "grain": alert_data.grain.value,
            "current_date": current_date.strftime("%B %d, %Y"),
            "prior_date": (current_date - timedelta(days=30)).strftime("%B %d, %Y"),
            "duration": self.faker.random_int(min=1, max=12),
            "pop": self.COMMON_VARIABLES[alert_data.grain.value.lower()]["pop"],  # type: ignore
            "eoi": self.COMMON_VARIABLES[alert_data.grain.value.lower()]["eoi"],  # type: ignore
            # Value metrics (keeping relationships realistic)
            "current_value": self.faker.random_int(min=100, max=10000),
            "prior_value": self.faker.random_int(min=80, max=9000),  # Slightly lower range
            "target": self.faker.random_int(min=100, max=10000),
            # Growth/Change metrics
            "current_growth": self.faker.random_int(min=-90, max=90),
            "prior_growth": self.faker.random_int(min=-50, max=50),
            "deviation": self.faker.random_int(min=-50, max=50),
            "required_growth": self.faker.random_int(min=-90, max=90),
            # Directional indicators (keeping distinct meanings)
            "position": self.faker.random_element(["above", "below"]),
            "movement": self.faker.random_element(["increase", "decrease"]),
            "pressure": self.faker.random_element(["upward", "downward", "unchanged"]),
            # Segment/Dimension analysis
            "dimension": self.faker.random_element(["Region", "Product", "Channel", "Customer", "Market"]),
            "slice": self.faker.random_element(["North America", "Electronics", "Online", "Enterprise", "Urban"]),
            "component": self.faker.random_element(["Sales Volume", "Price", "Cost", "Margin", "Market Share"]),
            "segment_metrics": {
                "current_share": self.faker.random_int(min=1, max=100),
                "prior_share": self.faker.random_int(min=1, max=100),
                "impact": self.faker.random_int(min=1, max=100),
                "contribution": self.faker.random_int(min=1, max=100),
            },
            # Required Performance specific
            "is_min_data": self.faker.boolean(),  # type: ignore
        }

        stories = self._generate_stories(story_groups, variables)

        context = {
            "data": {
                "stories": stories,
                "metric": metric,
                "fetched_at": datetime.now().strftime("%b %d, %Y"),
                "grain": alert_data.grain.value,
            },
            "config": alert_data,
            **variables,
        }

        return context

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
                return {"metric_id": metric_ids[0], "label": metric_ids[0]}

        # If no metrics found anywhere, generate fake data
        return {"metric_id": str(self.faker.random_int(min=1, max=100)), "label": self.faker.word().title()}  # type: ignore

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
