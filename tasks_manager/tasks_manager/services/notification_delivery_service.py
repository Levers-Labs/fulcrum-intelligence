from typing import Any

from commons.clients.insight_backend import InsightBackendClient
from commons.models.enums import ExecutionStatus
from commons.notifiers import BaseNotifier
from commons.notifiers.constants import NotificationChannel
from commons.notifiers.factory import NotifierFactory
from tasks_manager.config import AppConfig


class NotificationDeliveryService:
    """Service for preparing and delivering notifications across different channels."""

    def __init__(self, insights_client: InsightBackendClient, config: AppConfig):
        self.insights_client = insights_client
        self._notifiers: dict[NotificationChannel, BaseNotifier] = {}
        self.config = config

    async def _get_channel_config(self, channel: NotificationChannel) -> dict[str, Any]:  # type: ignore
        """
        Get configuration for the specified channel.
        """
        if channel == NotificationChannel.SLACK:
            return await self.insights_client.get_slack_config()
        if channel == NotificationChannel.EMAIL:
            return {"sender": self.config.sender_email, "region_name": self.config.aws_region}

    async def _get_notifier(self, channel: NotificationChannel) -> BaseNotifier:
        """
        Get or create a notifier for the specified channel using NotifierFactory.
        """
        if channel not in self._notifiers:
            config = await self._get_channel_config(channel)
            self._notifiers[channel] = NotifierFactory.create_notifier(channel=channel, config=config)

        return self._notifiers[channel]

    def _prepare_delivery_config(  # type: ignore
        self, channel: NotificationChannel, recipients: list[dict]
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """
        Prepare channel-specific configuration.

        Args:
            channel: The notification channel type
            recipients: List of recipient configurations
                For Slack: [{"id": "channel_id"}, ...]
                For Email: [{"email": "email@example.com", "location": "to/cc"}, ...]

        Returns:
            For Slack: List of configs for individual channel deliveries
            For Email: Single config with grouped recipients
        """
        if channel == NotificationChannel.SLACK:
            # Return list of individual channel configs
            return [{"id": recipient["id"]} for recipient in recipients]

        if channel == NotificationChannel.EMAIL:
            return {
                "to": [recipient["email"] for recipient in recipients if recipient["location"] == "to"],
                "cc": [recipient["email"] for recipient in recipients if recipient["location"] == "cc"],
            }

    def _is_delivery_successful(self, channel: NotificationChannel, result: dict) -> bool:  # type: ignore
        """
        Check if delivery was successful based on channel-specific response format.
        """
        if channel == NotificationChannel.SLACK:
            return result.get("ok", False)
        if channel == NotificationChannel.EMAIL:
            return result.get("status", False)

    async def _deliver_slack_notifications(
        self,
        notifier: BaseNotifier,
        delivery_configs: list[dict[str, Any]],
        template_config: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Handle Slack deliveries individually for each channel.
        """
        delivery_results = []
        successful_deliveries = 0

        for delivery_config in delivery_configs:
            try:
                result = notifier.send_notification(
                    template_config=template_config,
                    config=notifier.config,
                    context=context,
                    channel_config=delivery_config,
                )

                is_successful = self._is_delivery_successful(NotificationChannel.SLACK, result)
                result.update({"status": ExecutionStatus.COMPLETED if is_successful else ExecutionStatus.FAILED})
                delivery_results.append(result)
                if is_successful:
                    successful_deliveries += 1
            except Exception as e:
                delivery_results.append(
                    {"channel": delivery_config["id"], "status": ExecutionStatus.FAILED, "error": str(e)}
                )

        return {
            "channel": NotificationChannel.SLACK.value,
            "status": (
                ExecutionStatus.COMPLETED
                if successful_deliveries == len(delivery_configs)
                else ExecutionStatus.FAILED if successful_deliveries == 0 else ExecutionStatus.PARTIAL
            ),
            "success_count": successful_deliveries,
            "total_count": len(delivery_configs),
            "delivery_results": delivery_results,
        }

    async def deliver_channel_notification(
        self, channel_config: dict[str, Any], context: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Prepare and deliver notifications through a specific channel.
        For Slack: Delivers to each channel separately.
        For Email: Delivers to all recipients in a single call.

        Args:
            channel_config: Configuration for the notification channel including:
                - channel_type: The type of channel (slack, email, etc.)
                - template: The template configuration
                - recipients: List of recipients
            context: The context data to use for template rendering

        Returns:
            Dictionary containing:
                - channel: Channel type
                - status: COMPLETED/FAILED/PARTIAL
                - success_count: Number of successful deliveries
                - total_count: Total number of deliveries attempted
                - delivery_results: List of delivery results
                - error: Error message if failed (optional)
        """
        channel = NotificationChannel(channel_config["channel_type"])
        try:
            notifier = await self._get_notifier(channel)
            delivery_configs = self._prepare_delivery_config(channel=channel, recipients=channel_config["recipients"])

            if channel == NotificationChannel.SLACK:
                return await self._deliver_slack_notifications(
                    notifier=notifier,
                    delivery_configs=delivery_configs,  # type: ignore
                    template_config=channel_config["template"],
                    context=context,
                )

            # Email and other channels
            try:
                result = notifier.send_notification(
                    template_config=channel_config["template"],
                    config=notifier.config,
                    context=context,
                    channel_config=delivery_configs,  # type: ignore
                )
                # Check if delivery was successful
                is_successful = self._is_delivery_successful(channel, result)
                # Update result with recipients and status
                result.update(
                    {
                        "recipients": delivery_configs,
                        "status": ExecutionStatus.COMPLETED if is_successful else ExecutionStatus.FAILED,
                    }
                )

                return {
                    "channel": channel.value,
                    "status": ExecutionStatus.COMPLETED if is_successful else ExecutionStatus.FAILED,
                    "success_count": 1 if is_successful else 0,
                    "total_count": 1,
                    "delivery_results": [result],
                }
            except Exception as e:
                return {
                    "channel": channel.value,
                    "status": ExecutionStatus.FAILED,
                    "success_count": 0,
                    "total_count": 1,
                    "delivery_results": [
                        {"recipients": delivery_configs, "status": ExecutionStatus.FAILED, "error": str(e)}
                    ],
                }

        except Exception as e:
            return {
                "channel": channel.value,
                "status": ExecutionStatus.FAILED,
                "success_count": 0,
                "total_count": 0,
                "error": str(e),
            }
