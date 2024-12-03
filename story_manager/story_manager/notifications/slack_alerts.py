import logging
from datetime import date, datetime
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.crud import CRUDStory
from story_manager.core.dependencies import (
    SlackNotifierDep,
    get_insights_backend_client,
    get_query_manager_client,
    get_slack_notifier,
)
from story_manager.core.models import Story
from story_manager.db.config import get_async_session

# Initialize logger for the class
logger = logging.getLogger(__name__)


class StorySlackAlerts:
    """
    This class handles the fetching and sending of story alerts.
    """

    SLACK_MSG_TEMPLATE = "stories_slack_template"

    @staticmethod
    async def get_all_stories(metric_id: str, grain: Granularity, tenant_id: int, created_date: date) -> dict:
        """
        Fetches all stories based on the given metric_id, granularity, tenant_id, and created_date.
        Formats the stories into a dictionary for easier processing.
        """
        try:
            # Establish a database session
            async with get_async_session() as db_session:
                # Create a CRUDStory instance
                story_crud = CRUDStory(model=Story, session=db_session)
                # Fetch stories from the database
                stories = await story_crud.get_stories(metric_id, grain, created_date, tenant_id)
                # Log the number of stories fetched
                logger.info(
                    f"Fetched {len(stories)} stories for metric_id: {metric_id}, "
                    f"grain: {grain}, tenant_id: {tenant_id}, created_date: {created_date}"
                )
                # Format the stories into a dictionary
                formatted_stories = [
                    {
                        "story_group": story.story_group.value,
                        "metric_id": story.metric_id,
                        "title": story.title,
                        "detail": story.detail,
                    }
                    for story in stories
                ]
            # Create a context dictionary
            context = {
                "stories": formatted_stories,
                "grain": grain.value,
                "time": datetime.utcnow(),
                "metric_id": metric_id,
            }
            return context
        except Exception as e:
            logger.error(
                f"Failed to fetch stories for metric_id: {metric_id}, grain: {grain}, "
                f"tenant_id: {tenant_id}, created_date: {created_date}. Error: {e}"
            )
            return {}

    async def _send_slack_alerts(
        self, client: SlackNotifierDep, context: dict, channel_config: dict, slack_config: dict
    ) -> Any:
        """
        Sends Slack notifications using the provided client and context.
        """
        try:
            # Send a notification using the client and context
            response = await client.send_notification(
                template_name=self.SLACK_MSG_TEMPLATE,
                config=slack_config,
                channel_config=channel_config,
                context=context,
            )
            # Log the response of the Slack notification
            logger.info(f"Slack notification response: {response}")
            return response
        except Exception as e:
            logger.error(
                f"Failed to send Slack notification for metric_id: {context['metric_id']}, "
                f"grain: {context['grain']}, channel: {channel_config['channel_id']}. Error: {e}"
            )
            return None

    async def process_and_send_alerts(self, metric_id: str, grain: Granularity, tenant_id: int, created_date: date):
        """
        Fetches all stories for the given parameters and sends Slack alerts.
        """
        try:
            logger.info(
                f"Starting to process and send Slack alerts for metric_id: {metric_id}, "
                f"grain: {grain}, tenant_id: {tenant_id}, created_date: {created_date}"
            )
            # Get the query manager client
            query_service = await get_query_manager_client()
            # Get the Slack notification details for the metric
            slack_notification_config = await query_service.get_metric_slack_notification_details(metric_id)
            # Check if Slack notifications are enabled
            slack_enabled = slack_notification_config.get("slack_enabled")
            if not slack_enabled:
                logger.info(f"Slack notifications are not enabled for Metric: {metric_id}")
                return
            # Log the Slack notifications enabled status
            logger.info(f"Slack notifications are enabled for Metric: {metric_id}")
            # Get the Slack channels configuration
            channels_config = slack_notification_config.get("slack_channels", [])

            # Fetch stories created for the date
            stories = await self.get_all_stories(metric_id, grain, tenant_id, created_date)

            # Get the insights backend client
            insights_client = await get_insights_backend_client()
            # Get the tenant config
            tenant_config = await insights_client.get_tenant_config()
            # Get the Slack connection config
            slack_connection_config = tenant_config.get("slack_connection", None)
            if not slack_connection_config:
                logger.info("Slack connection is not set for the tenant.")
                return

            # Get the Slack notifier client
            notifier = await get_slack_notifier()
            # Call the method to send alerts
            for channel_config in channels_config:
                _ = await self._send_slack_alerts(
                    client=notifier,
                    context=stories,
                    channel_config=channel_config,
                    slack_config=slack_notification_config,
                )
        except Exception as e:
            logger.error(
                f"Failed to process and send Slack alerts for metric_id: {metric_id}, grain: {grain}, "
                f"tenant_id: {tenant_id}, created_date: {created_date}. Error: {e}"
            )
