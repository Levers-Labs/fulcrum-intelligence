import json
import logging
import os
import random
from datetime import datetime
from typing import Dict, List

from fastapi import HTTPException
from jinja2 import Environment, FileSystemLoader, Template

from commons.notifiers.constants import NotificationChannel

logger = logging.getLogger(__name__)

# Define your base HTML template
BASE_HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Template Preview</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
        }
    </style>
</head>
<body>
    {{ content }}
</body>
</html>
"""

# Create a Template object for the base template
base_template = Template(BASE_HTML_TEMPLATE)


def render_slack_preview(slack_json):
    """Convert Slack JSON to HTML for preview."""
    html_content = "<div class='slack-preview'>"
    for block in slack_json.get("blocks", []):
        if block["type"] == "header":
            html_content += f"<h2>{block['text']['text']}</h2>"
        elif block["type"] == "section":
            html_content += f"<p>{block['text']['text']}</p>"
        elif block["type"] == "divider":
            html_content += "<hr/>"
        elif block["type"] == "context":
            html_content += f"<small>{block['elements'][0]['text']}</small>"
    html_content += "</div>"
    return html_content


class PreviewService:
    def __init__(self, template_dir: str):
        """Initialize the PreviewService with the directory for templates."""
        self.env = Environment(loader=FileSystemLoader(template_dir))
        self.base_template = self.get_template("base_template", "base")  # Load the base template
        self.template_dir = template_dir

    def get_template(self, template_name: str, template_type: str) -> Template:
        """
        Get the template based on the template name and type.
        Template will be HTML in the case of email and JSON in the case of Slack.

        Args:
            template_name (str): The name of the template.
            template_type (str): The type of the template (e.g., 'slack', 'email', or 'base').

        Returns:
            Template: The template object.

        Raises:
            ValueError: If no matching template is found for the given name and type.
        """
        template_extension = "json" if template_type == "slack" else "html"
        template_path = (
            os.path.join(template_type, f"{template_name}.{template_extension}")
            if template_type != "base"
            else f"{template_name}.html"
        )

        logger.debug("Loading %s template", template_path)

        try:
            template = self.env.get_template(template_path)
            return template
        except Exception as e:
            raise ValueError(f"Template '{template_name}' of type '{template_type}' not found.") from e

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

    def preview_template(self, preview_data) -> dict:
        """Generate a preview of the notification template."""
        # Generate mock data based on user input
        mock_data = self.generate_mock_data(preview_data.story_groups, [preview_data.metric["id"]], preview_data.grain)

        try:
            # Fetch the appropriate template based on the type
            if preview_data.template_type == NotificationChannel.SLACK:
                slack_template = self.get_template("alert_template", "slack")
                rendered_json = slack_template.render(
                    metric=mock_data["metric"],
                    grain=mock_data["grain"],
                    stories=mock_data["stories"],
                    time=mock_data["time"],
                    metric_id=mock_data["metric"]["id"],
                    recipients=", ".join(preview_data.recipients),  # Join recipients for Slack
                )

                slack_json = json.loads(rendered_json)
                slack_html = render_slack_preview(slack_json)
                final_html = self.base_template.render(content=slack_html)

                return {
                    "preview_html": final_html,
                    "raw_content": rendered_json,
                    "recipients": ", ".join(preview_data.recipients),  # Include recipients in response
                }
            else:
                email_template = self.get_template("alert_template", "email")
                email_html = email_template.render(
                    from_email="alerts@yourdomain.com",
                    to_emails=", ".join(preview_data.recipients),  # Use the recipients directly
                    metric=mock_data["metric"],
                    story_groups=[s["story_group"] for s in mock_data["stories"]],
                    metrics=[mock_data["metric"]["label"]],
                    dashboard_url="https://app-dev.leverslabs.com",
                )

                final_html = self.base_template.render(content=email_html)

                return {
                    "preview_html": final_html,
                    "raw_content": email_html,
                    "recipients": ", ".join(preview_data.recipients),  # Include recipients in response
                }
        except Exception as e:
            logger.error("Template rendering failed: %s", str(e))
            raise HTTPException(status_code=400, detail=f"Template rendering failed: {str(e)}")
