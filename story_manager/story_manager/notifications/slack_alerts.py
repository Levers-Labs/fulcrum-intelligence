import logging
from datetime import date, datetime
from typing import Any

from commons.clients.insight_backend import InsightBackendClient
from commons.clients.query_manager import QueryManagerClient
from commons.models.enums import Granularity
from story_manager.core.crud import CRUDStory
from story_manager.core.dependencies import SlackNotifierDep, get_slack_notifier
from story_manager.core.models import Story
from story_manager.db.config import get_async_session

logger = logging.getLogger(__name__)


class StorySlackAlerts:
    """
    Handles fetching and sending story alerts via Slack.
    """

    SLACK_MSG_TEMPLATE = "stories_slack_template"

    def __init__(
        self,
        query_client: QueryManagerClient,
        insights_client: InsightBackendClient,
        metric_id: str,
    ) -> None:
        """
        Initialize StorySlackAlerts.

        Args:
            query_client: Client for querying metrics data
            insights_client: Client for insights backend
            metric_id: ID of the metric to fetch stories for
        """
        self.query_client = query_client
        self.insights_client = insights_client
        self.metric_id = metric_id

    async def get_stories_context(self, grain: Granularity, tenant_id: int, created_date: date) -> dict:
        """
        Fetch stories and build template context for a metric.

        Args:
            grain: Time granularity for stories
            tenant_id: ID of the tenant
            created_date: Date stories were created

        Returns:
            Dict containing formatted stories and template context
        """
        try:
            metric = await self.query_client.get_metric(self.metric_id)

            async with get_async_session() as db_session:
                story_crud = CRUDStory(model=Story, session=db_session)
                stories = await story_crud.get_stories(self.metric_id, grain, created_date, tenant_id)

                logger.debug(
                    "Fetched %d stories for metric_id=%s, grain=%s, tenant_id=%d, created_date=%s",
                    len(stories),
                    self.metric_id,
                    grain,
                    tenant_id,
                    created_date,
                )

                formatted_stories = [
                    {
                        "story_group": story.story_group.value,
                        "metric_id": story.metric_id,
                        "title": story.title,
                        "detail": story.detail,
                    }
                    for story in stories
                ]

            return {
                "stories": formatted_stories,
                "grain": grain.value,
                "time": datetime.utcnow(),
                "metric_id": self.metric_id,
                "metric_label": metric["label"],
            }

        except Exception as e:
            logger.exception("Failed to fetch stories for metric_id=%s: %s", self.metric_id, str(e))
            return {}

    async def _send_slack_alerts(
        self,
        client: SlackNotifierDep,
        context: dict,
        channel_config: dict,
        slack_config: dict,
    ) -> Any:
        """
        Send Slack notification for stories.

        Args:
            client: Slack notification client
            context: Story context to send
            channel_config: Channel configuration
            slack_config: Slack connection configuration

        Returns:
            Response from Slack API
        """
        try:
            response = await client.send_notification(
                template_name=self.SLACK_MSG_TEMPLATE,
                config=slack_config,
                channel_config=channel_config,
                context=context,
            )
            logger.debug("Slack notification sent successfully to channel %s", channel_config["channel_id"])
            return response

        except Exception as e:
            logger.exception(
                "Failed to send Slack notification for metric_id=%s to channel=%s: %s",
                context["metric_id"],
                channel_config["channel_id"],
                str(e),
            )
            return None

    async def process_and_send_alerts(
        self,
        grain: Granularity,
        tenant_id: int,
        created_date: date,
    ) -> None:
        """
        Process stories and send Slack alerts.

        Args:
            grain: Time granularity for stories
            tenant_id: ID of the tenant
            created_date: Date stories were created
        """
        logger.info(
            "Processing Slack alerts for metric_id=%s, grain=%s, tenant_id=%d, created_date=%s",
            self.metric_id,
            grain,
            tenant_id,
            created_date,
        )

        try:
            # Get notification config
            slack_notification_config = await self.query_client.get_metric_slack_notification_details(self.metric_id)
            if not slack_notification_config.get("slack_enabled"):
                logger.info("Slack notifications disabled for metric_id=%s", self.metric_id)
                return

            channels_config = slack_notification_config.get("slack_channels", [])
            if not channels_config:
                logger.info("No Slack channels configured for metric_id=%s", self.metric_id)
                return

            # Get tenant Slack config
            tenant_config = await self.insights_client.get_tenant_config()
            slack_connection_config = tenant_config.get("slack_connection")
            if not slack_connection_config:
                logger.error("Slack connection not configured for tenant_id=%d", tenant_id)
                return

            # Get stories and send notifications
            stories = await self.get_stories_context(grain, tenant_id, created_date)
            if not stories:
                logger.info("No stories found for metric_id=%s", self.metric_id)
                return

            notifier = await get_slack_notifier()
            for channel_config in channels_config:
                await self._send_slack_alerts(
                    client=notifier,
                    context=stories,
                    channel_config=channel_config,
                    slack_config=slack_connection_config,
                )

            logger.info("Successfully processed alerts for metric_id=%s", self.metric_id)

        except Exception as e:
            logger.exception("Failed to process alerts for metric_id=%s: %s", self.metric_id, str(e))
