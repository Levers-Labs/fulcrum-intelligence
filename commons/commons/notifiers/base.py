import logging
from abc import ABC, abstractmethod
from typing import Any

from jinja2 import Template, TemplateError

from commons.notifiers.constants import NotificationChannel

logger = logging.getLogger(__name__)


class BaseNotifier(ABC):
    """
    Base class for all notifiers.
    """

    channel: NotificationChannel

    def __init__(self, config: dict[str, Any]):
        self.config = config

    def create_template(self, template_content: str) -> Template:
        """
        Create a template from the provided template content string.

        Args:
            template_content (str): The template content as a string.

        Returns:
            Template: The template object.
        """
        return Template(template_content)

    def render_template(self, template: Template, context: dict[str, Any]) -> str:
        """
        render the template with the provided context.

        Args:
            template (Template): The template object.
            context (Dict[str, Any]): The context dictionary containing data to be
            rendered in the template.

        Returns:
            str: The rendered template content.

        Raises:
            TemplateError: If an error occurs while rendering the template.
        """
        try:
            return template.render(context)
        except Exception as ex:
            logger.exception(
                "Unable to render template",
            )
            raise TemplateError("Error while rendering template") from ex

    @abstractmethod
    def get_notification_content(self, template_config: dict[str, Any], context: dict[str, Any]) -> dict:
        """
        Get the content of the notification based on the context.

        Args:
            template_config (Dict[str, Any]): The template configuration containing template content.
            context (Dict[str, Any]): The context dictionary containing data to be
            rendered in the template.

        Returns:
            dict: The content of the notification.
        """

    def send_notification(
        self,
        template_config: dict[str, Any],
        config: dict[str, Any],
        channel_config: dict[str, Any],
        context: dict[str, Any],
    ) -> dict:
        """
        Send a notification using the specified template config, channel, config, and context.

        Args:
            template_config (Dict[str, Any]): The template configuration containing template content.
            config (Dict[str, Any]): The configuration dictionary containing
            notifier-specific settings.
            channel_config (Dict[str, Any]): The configuration dictionary containing
            channel-specific settings
            context (Dict[str, Any]): The context dictionary containing data
            to be rendered in the template.

        Returns:
            dict: Metadata about the sent notification.
        """
        # Get the notification content
        content = self.get_notification_content(template_config, context)
        # Get the client
        client = self.get_client(config)
        # Send the notification using the client
        meta = self.send_notification_using_client(client, content, channel_config=channel_config)
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

    @abstractmethod
    def send_notification_using_client(self, client: Any, content: Any, channel_config: dict[str, Any]) -> dict:
        """
        send the notification using the provided client and rendered template.

        Args:
            client (Any): The notifier client object.
            content (Any): The content of the notification.
            channel_config (Dict[str, Any]): The configuration dictionary containing
            channel-specific settings

        Returns:
            dict: The metadata of the notification.
        """
