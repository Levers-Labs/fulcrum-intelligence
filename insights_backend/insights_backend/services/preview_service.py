import html
import json
import logging
import os
import random
from datetime import datetime

from fastapi import HTTPException
from jinja2 import Environment, FileSystemLoader, Template

from commons.notifiers.constants import NotificationChannel

logger = logging.getLogger(__name__)


class PreviewService:
    """Service for previewing notification templates"""

    def __init__(self, template_dir: str):
        self.env = Environment(
            loader=FileSystemLoader(template_dir), autoescape=True, trim_blocks=True, lstrip_blocks=True
        )

    def get_template(self, template_name: str, template_type: str) -> Template:
        """Load template from file"""
        template_path = (
            os.path.join(template_type, f"{template_name}.json")
            if template_type == "slack"
            else os.path.join(template_type, f"{template_name}.html")
        )
        return self.env.get_template(template_path)

    def generate_mock_data(self, story_groups: list[str], metrics: list[str], grain: str) -> dict:
        """Generate mock data for previewing notifications."""
        current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # Get the current UTC time
        mock_data = {
            "stories": [],
            "metric": {
                "id": random.choice(metrics),  # Randomly select a metric ID
                "label": random.choice(metrics),  # Randomly select a metric label
            },
            "grain": grain,
            "time": current_time,  # Use the current time
        }

        for group in story_groups:
            for i in range(random.randint(1, 3)):  # Randomly generate 1 to 3 stories per group
                mock_data["stories"].append(  # type: ignore
                    {
                        "story_group": group,
                        "title": f"Mock Story Title {i + 1} for {group}",  # Generate a mock title
                        "detail": f"This is a detail for story {i + 1} in group {group}.",  # Generate a mock detail
                    }
                )

        return mock_data

    def _render_slack_json_to_html(self, slack_json: dict) -> str:
        """Convert Slack JSON to HTML for preview"""
        html_content = []
        html_content.append("<div class='slack-preview'>")

        for block in slack_json.get("blocks", []):
            if block["type"] == "header":
                text = html.escape(block["text"]["text"])
                html_content.append(f"<h2>{text}</h2>")
            elif block["type"] == "section":
                text = html.escape(block["text"]["text"]).replace("\\n", "")
                html_content.append(f"<p>{text}</p>")
            elif block["type"] == "divider":
                html_content.append("<hr/>")
            elif block["type"] == "context":
                text = html.escape(block["elements"][0]["text"])
                html_content.append(f"<small>{text}</small>")

        html_content.append("</div>")
        return "".join(html_content)

    def preview_template(self, preview_data) -> dict:
        """Generate a preview of the notification template"""
        # Generate mock data based on user input
        mock_data = self.generate_mock_data(preview_data.story_groups, [preview_data.metric["id"]], preview_data.grain)

        try:
            # Fetch the appropriate template based on the type
            if preview_data.template_type == NotificationChannel.SLACK:
                # Handle Slack template (JSON)
                slack_template = self.get_template("alert_template", "slack")
                rendered_json = slack_template.render(
                    metric=mock_data["metric"],
                    grain=mock_data["grain"],
                    stories=mock_data["stories"],
                    time=mock_data["time"],
                    metric_id=mock_data["metric"]["id"],
                    recipients=", ".join(preview_data.recipients),
                )

                slack_json = json.loads(rendered_json)
                slack_html = self._render_slack_json_to_html(slack_json)
                final_html = self.env.get_template("base_template.html").render(content=slack_html)

                # Clean up the final HTML
                final_html = final_html.replace("\n", "").replace("    ", "")

                return {
                    "preview_html": final_html,
                    "raw_content": rendered_json,
                    "recipients": ", ".join(preview_data.recipients),
                }
            else:
                # Handle Email template (HTML)
                email_template = self.get_template("alert_template", "email")
                email_html = email_template.render(
                    from_email="alerts@yourdomain.com",
                    to_emails=preview_data.recipients,
                    metric=mock_data["metric"],
                    story_groups=[s["story_group"] for s in mock_data["stories"]],
                    metrics=[mock_data["metric"]["label"]],
                    dashboard_url="https://app-dev.leverslabs.com",
                )

                final_html = self.env.get_template("base_template.html").render(content=email_html)
                # Clean up the final HTML
                final_html = final_html.replace("\n", "").replace("    ", "")

                return {
                    "preview_html": final_html,
                    "raw_content": email_html,
                    "recipients": ", ".join(preview_data.recipients),
                }
        except Exception as e:
            logger.error("Template rendering failed: %s", str(e))
            raise HTTPException(status_code=400, detail=f"Template rendering failed: {str(e)}")
