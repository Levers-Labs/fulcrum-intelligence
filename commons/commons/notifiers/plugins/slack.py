import json
from typing import Any

from jinja2 import Template

from commons.clients.slack import SlackClient
from commons.notifiers import BaseNotifier
from commons.notifiers.constants import NotificationChannel


class SlackNotifier(BaseNotifier):
    channel = NotificationChannel.SLACK

    def get_client(self, config: dict[str, Any]) -> Any:
        """
        Returns a Slack client session based on the provided configuration.

        Args:
            config (Dict[str, Any]): The configuration for the Slack client.

        Returns:
            Any: The Slack client session.

        Raises:
            ValueError: If the webhook_url is not provided in the configuration.

        """
        # slack_token = config.get("bot_token")
        slack_token = "xoxb-7976566008402-7962040521303-EKmzh6oZSFNp6QAXT7OpA3eq"
        if not slack_token:
            raise ValueError("Slack bot token not provided in the configuration.")
        # Initialize Slack client
        client = SlackClient(token=slack_token)

        return client

    def send_notification_using_client(self, client: Any, rendered_template: dict, channel_config: dict):
        """
        sends a notification using the Slack client and returns the response.

        Args:
            client (Any): The Slack client session.
            rendered_template (dict): The rendered notification template.
            channel_config (Dict[str, Any]): The configuration for the Slack channel.

        Returns:
            dict: The response from the Slack client.
        :raises:
            SlackApiError: If there is an error sending the message.

        """
        channel_id = channel_config.get("channel_id")
        if not channel_id:
            raise ValueError("Channel ID is not provided in the configuration.")
        # Send a message
        kwargs = {}
        if "blocks" in rendered_template:
            kwargs["blocks"] = rendered_template["blocks"]
        if "text" in rendered_template:
            kwargs["text"] = rendered_template["text"]
        if "attachments" in rendered_template:
            kwargs["attachments"] = rendered_template["attachments"]
        # Send a message to Slack
        return client.post_message(channel_id=channel_id, **kwargs)

    def render_template(self, template: Template, context: dict[str, Any]) -> str:
        """
        render the template with the provided context.

        Args:
            template (Template): The template object.
            context (Dict[str, Any]): The context dictionary containing data to be
            rendered in the template.

        Returns:
            str: The rendered email template.

        Raises:
            TemplateError: If there is an error rendering the template.
        """
        rendered_string = super().render_template(template, context)
        # Note: don't need to unescape <, > and & as Slack will render them correctly
        return json.loads(rendered_string, strict=False)
