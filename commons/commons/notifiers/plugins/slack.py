import json
from typing import Any

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
        slack_token = config.get("bot_token")
        if not slack_token:
            raise ValueError("Slack bot token not provided in the configuration.")
        # Initialize Slack client
        client = SlackClient(token=slack_token)

        return client

    def get_notification_content(self, template_config: dict[str, Any], context: dict[str, Any]) -> dict:
        """
        Get the notification content by rendering the template with the provided context.

        Args:
            template_config (Dict[str, Any]): The template configuration containing the Slack message template.
            context (Dict[str, Any]): The context dictionary containing data to be
            rendered in the template.

        Returns:
            Dict: The rendered notification content.

        Raises:
            TemplateError: If there is an error rendering the template.
            ValueError: If template content is missing or invalid JSON.
        """
        if not template_config.get("message"):
            raise ValueError("Template configuration must include content field")

        # Create and render the template
        template = self.create_template(template_config["message"])
        rendered_string = self.render_template(template, context)

        # Parse the rendered JSON template
        try:
            return json.loads(rendered_string, strict=False)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON template content: {e}") from e

    def send_notification_using_client(self, client: Any, content: dict, channel_config: dict):
        """
        sends a notification using the Slack client and returns the response.

        Args:
            client (Any): The Slack client session.
            content (dict): The rendered notification template.
            channel_config (Dict[str, Any]): The configuration for the Slack channel.

        Returns:
            dict: The response from the Slack client.
        :raises:
            SlackApiError: If there is an error sending the message.
            ValueError: If channel ID is missing.
        """
        channel_id = channel_config.get("id")
        if not channel_id:
            raise ValueError("Channel ID is not provided in the configuration.")

        # Send a message
        kwargs = {}
        if "blocks" in content:
            kwargs["blocks"] = content["blocks"]
        if "text" in content:
            kwargs["text"] = content["text"]
        if "attachments" in content:
            kwargs["attachments"] = content["attachments"]

        # Send a message to Slack
        return client.post_message(channel_id=channel_id, **kwargs)
