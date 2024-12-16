import logging
from datetime import date, datetime
from typing import Any

from commons.clients.query_manager import QueryManagerClient
from commons.models.enums import Granularity
from story_manager.core.crud import CRUDStory
from story_manager.core.dependencies import SlackNotifierDep, get_slack_notifier
from story_manager.core.models import Story
from story_manager.db.config import get_async_session
from story_manager.story_builder.constants import STORY_GROUP_META

logger = logging.getLogger(__name__)


class SlackAlertsService:
    """
    Service for managing and sending Slack alerts for stories.

    Handles the end-to-end process of retrieving stories from the database,
    formatting them for Slack, and sending notifications through configured channels.
    Includes error handling and logging throughout the notification pipeline.

    Key responsibilities:
    - Story retrieval and formatting for Slack messages
    - Channel configuration validation
    - Notification delivery with error handling
    - Tenant-specific configuration management
    """

    SLACK_MSG_TEMPLATE = "stories_slack_template"

    def __init__(self, query_client: QueryManagerClient, slack_connection_config: dict) -> None:
        """
        Initialize the Slack alerts service with required API clients.

        Args:
            query_client: Client for accessing metric metadata and notification settings
        """
        self.query_client = query_client
        self.slack_connection_config = slack_connection_config

    async def _get_stories(self, metric_id: str, grain: Granularity, tenant_id: int, created_date: date) -> list:
        """
        Retrieve and format stories for Slack notifications.

        Queries the database for stories matching the specified criteria and formats them
        for display in Slack messages. Only retrieves heuristic stories by default.

        Args:
            metric_id: The metric identifier to fetch stories for
            grain: Temporal granularity of the stories (e.g., daily, weekly)
            tenant_id: Tenant identifier for data isolation
            created_date: Creation date to filter stories by

        Returns:
            list: Formatted story dictionaries containing:
                - story_group: The categorization of the story
                - metric_id: The associated metric identifier
                - title: The story headline
                - detail: The detailed story content
        """
        async with get_async_session() as db_session:
            # Query stories using CRUD operations
            story_crud = CRUDStory(model=Story, session=db_session)
            stories = await story_crud.get_stories(metric_id, grain, created_date, tenant_id, is_heuristic=True)

            logger.debug(
                "Fetched %d stories for metric_id=%s, grain=%s, tenant_id=%d, created_date=%s",
                len(stories),
                metric_id,
                grain,
                tenant_id,
                created_date,
            )

            # Format stories for template rendering
            formatted_stories = [
                {
                    "story_group": STORY_GROUP_META[story.story_group]["label"],
                    "metric_id": story.metric_id,
                    "title": story.title,
                    "detail": story.detail,
                }
                for story in stories
            ]
            formatted_stories.sort(key=lambda x: x["story_group"])

        return formatted_stories

    async def _prepare_context(self, stories, grain, metric_id):
        """
        Build the context dictionary for Slack message templates.

        Creates a dictionary containing all necessary data for rendering
        the Slack message template, including story content and metadata.

        Args:
            stories: List of formatted story dictionaries
            grain: Time granularity of the stories
            metric_id: Metric identifier for retrieving additional metadata

        Returns:
            dict: Template context containing:
                - stories: The formatted story list
                - grain: Time granularity value
                - time: Current UTC timestamp
                - metric_id: Metric identifier
                - metric_label: Human-readable metric name
        """
        metric = await self.query_client.get_metric(metric_id)
        return {
            "stories": stories,
            "grain": grain.value,
            "time": datetime.utcnow(),
            "metric": metric,
        }

    async def _send_slack_alerts(
        self,
        client: SlackNotifierDep,
        context: dict,
        channel_config: dict,
        slack_config: dict,
    ) -> Any:
        """
        Send a formatted Slack notification to a specific channel.

        Handles the actual transmission of the notification using the provided
        Slack client, including error handling and logging.

        Args:
            client: The Slack notification client instance
            context: Template context with story data and metadata
            channel_config: Channel-specific configuration settings
            slack_config: Global Slack connection configuration

        Returns:
            Any: Slack API response on success, None on failure
        """
        response = await client.send_notification(  # type: ignore
            template_name=self.SLACK_MSG_TEMPLATE,
            config=slack_config,
            channel_config=channel_config,
            context=context,
        )
        logger.debug("Slack notification sent successfully to channel %s", channel_config["id"])
        return response

    async def _get_slack_config(self, metric_id: str) -> list:
        """
        Get and validate Slack notification settings for a metric.

        Retrieves the notification configuration and performs validation checks
        to ensure notifications can be sent.

        Args:
            metric_id: Metric identifier to get configuration for

        Returns:
            list: List of validated channel configurations

        Raises:
            ValueError: If notifications are disabled or no channels are configured
        """
        slack_notification_config = await self.query_client.get_metric_slack_notification_details(metric_id)

        if not slack_notification_config.get("slack_enabled"):
            logger.info("Slack notifications disabled for metric_id=%s", metric_id)
            raise ValueError("Slack notifications disabled")

        channels_config = slack_notification_config.get("slack_channels", [])
        if not channels_config:
            logger.info("No Slack channels configured for metric_id=%s", metric_id)
            raise ValueError("No Slack channels configured")

        return channels_config

    async def send_metric_stories_notification(
        self,
        grain: Granularity,
        tenant_id: int,
        created_date: date,
        metric_id: str,
    ) -> None:
        """
        Orchestrate the end-to-end process of sending story notifications.

        This is the main entry point for the notification service. It coordinates
        the entire notification workflow including:
        1. Configuration validation and retrieval
        2. Story fetching and formatting
        3. Notification dispatch to all configured channels

        The method includes comprehensive error handling and logging throughout
        the process.

        Args:
            grain: Time granularity for story filtering
            tenant_id: Tenant identifier for configuration and data isolation
            created_date: Date filter for story selection
            metric_id: Metric identifier for story retrieval
        """
        logger.info(
            "Processing Slack alerts for metric_id=%s, grain=%s, tenant_id=%d, created_date=%s",
            metric_id,
            grain,
            tenant_id,
            created_date,
        )

        if not self.slack_connection_config:
            logger.error("Slack connection not configured for tenant_id=%d", tenant_id)
            return

        try:
            channels_config = await self._get_slack_config(metric_id)
        except ValueError:
            return

        # Fetch stories and prepare notifications
        stories = await self._get_stories(metric_id, grain, tenant_id, created_date)
        if not stories:
            logger.info("No stories found for metric_id=%s", metric_id)
            return

        context = await self._prepare_context(stories, grain, metric_id)
        notifier_client = await get_slack_notifier()
        for channel_config in channels_config:
            await self._send_slack_alerts(
                client=notifier_client,
                context=context,
                channel_config=channel_config,
                slack_config=self.slack_connection_config,
            )

        logger.info("Successfully processed alerts for metric_id=%s", metric_id)
