from typing import Any

from jinja2 import Template

from commons.clients.email import EmailClient
from commons.notifiers import BaseNotifier
from commons.notifiers.constants import EMAIL_TEMPLATES, NotificationChannel


class EmailNotifier(BaseNotifier):
    channel = NotificationChannel.EMAIL

    def __init__(self, template_dir: str, sender: str):
        super().__init__(template_dir)
        self.sender = sender

    def get_client(self, config: dict[str, Any]) -> Any:
        """
        Returns an Email client session based on the provided configuration.

        Args:
            config (Dict[str, Any]): The configuration for the Email client.

        Returns:
            Any: The Email client session.

        Raises:
            ValueError: If required configuration is missing.
        """
        region_name = config.get("region_name")
        if not region_name:
            raise ValueError("AWS region not provided in the configuration.")

        # Initialize Email client
        client = EmailClient(region_name=region_name)
        return client

    def get_notification_content(self, template_name: str, context: dict[str, Any]) -> dict:
        """
        Get the content of the notification based on the context.

        Args:
            template_name (str): The name of the template to use for the notification.
            context (Dict[str, Any]): The context dictionary containing data to be
                rendered in the template.

        Returns:
            dict: The content of the notification containing subject, html and text versions.
        """
        # Get email template configuration
        email_template = EMAIL_TEMPLATES.get(template_name)  # type: ignore
        if not email_template:
            raise ValueError(f"Email template {template_name} not found")

        # Render subject
        subject = email_template["subject"].format(**context)

        # Create a template object from the body string
        body_template = Template(email_template["body"])
        html_content = self.render_template(body_template, context)

        # Prepare content with subject and body
        content = {"subject": subject, "html": html_content}
        return content

    def send_notification_using_client(self, client: EmailClient, content: dict, channel_config: dict):
        """
        sends a notification using the Email client and returns the response.

        Args:
            client (EmailClient): The Email client session.
            content (dict): The rendered notification content (subject, body).
            channel_config (Dict[str, Any]): The configuration for the email channel, to and cc etc.

        Returns:
            dict: The response from the Email client.
        Raises:
            ValueError: If required configuration is missing.
        """
        recipients = channel_config.get("to")
        cc = channel_config.get("cc")
        if not recipients:
            raise ValueError("Recipients are not provided in the configuration.")

        is_success, res = client.send_email(
            sender=self.sender,
            recipients=recipients,
            cc=cc,
            subject=content["subject"],
            body_html=content["html"],
        )
        if res:
            res["status"] = is_success
        else:
            res = {"status": False}
        return res
