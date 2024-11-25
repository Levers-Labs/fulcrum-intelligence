import json
import logging
import os.path
from abc import ABC, abstractmethod
from typing import Any

from jinja2 import (
    Environment,
    FileSystemLoader,
    Template,
    TemplateError,
)
from slack_sdk import WebClient

from commons.notifiers.constants import NotificationChannel

logger = logging.getLogger(__name__)


class BaseNotifier(ABC):
    """
    Base class for all notifiers.
    """

    channel: NotificationChannel

    def __init__(self, template_dir: str):
        self.env = Environment(loader=FileSystemLoader(template_dir))  # noqa

    def get_template(self, template_name: str) -> Template:
        """
        get the template based on the template name and channel.
        template will be html in the case of email and json in the case of Slack.

        Args:
            template_name (str): The name of the template.

        Returns:
            Template: The template object.

        Raises:
            ValueError: If no matching template is found for the given name and channel.
        """
        template_path = (
            os.path.join(self.channel.value, f"{template_name}.json")
            if self.channel == NotificationChannel.SLACK
            else os.path.join(self.channel.value, f"{template_name}.html")
        )
        logger.debug("Loading %s template", template_path)
        template = self.env.get_template(template_path)
        return template

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
            TemplateError: If an error occurs while rendering the template.
        """
        try:
            return template.render(context)
        except Exception as ex:
            logger.exception(
                "Unable to render %s template",
                template,
            )
            raise TemplateError("Error while rendering template") from ex

    def send_notification(
        self,
        template_name: str,
        config: dict[str, Any],
        channel_config: dict[str, Any],
        context: dict[str, Any],
    ) -> dict:
        """
        send a notification using the specified template, channel, config, and context.

        Args:
            template_name (str): The name of the template to use for the notification.
            config (Dict[str, Any]): The configuration dictionary containing
            notifier-specific settings.
            channel_config (Dict[str, Any]): The configuration dictionary containing
            channel-specific settings
            context (Dict[str, Any]): The context dictionary containing data
            to be rendered in the template.

        Returns:
            bool: True if the notification was sent successfully, False otherwise.
        """
        # Load the template
        template = self.get_template(template_name)
        # Render the template
        rendered_str = self.render_template(template, context)
        # Get the client
        client = self.get_client(config)
        # Send the notification using the client
        meta = self.send_notification_using_client(client, rendered_str, channel_config=channel_config)
        return meta

    @abstractmethod
    def get_client(self, config: dict[str, Any]) -> Any:
        """
        Get the notifier client based on the provided configuration.

        Args:
            config (Dict[str, Any]): The configuration dictionary containing
            notifier-specific settings.

                Returns:
            Any: The notifier client object.
        """
        client = WebClient(token="xoxb-7976566008402-7962040521303-EKmzh6oZSFNp6QAXT7OpA3eq")
        return client

    @abstractmethod
    def send_notification_using_client(
        self, client: Any, rendered_template: Any, channel_config: dict[str, Any]
    ) -> dict:
        """
        send the notification using the provided client and rendered template.

        Args:
            client (Any): The notifier client object.
            rendered_template (Any): The rendered template.
            channel_config (Dict[str, Any]): The configuration dictionary containing
            channel-specific settings

        Returns:
            dict: The metadata of the notification.
        """
        temp = json.loads(rendered_template)
        _ = client.chat_postMessage(channel="C07UT2BPC92", blocks=temp["blocks"])
        return {}
