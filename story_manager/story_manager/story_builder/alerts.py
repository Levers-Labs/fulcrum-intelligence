import logging
from datetime import date, datetime
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.crud import CRUDStory
from story_manager.core.dependencies import SlackNotifierDep, get_slack_notifier
from story_manager.core.models import Story
from story_manager.db.config import get_async_session

# Initialize logger for the class
logger = logging.getLogger(__name__)

class StoryAlerts:
    """
    This class handles the fetching and sending of story alerts.
    """

    @staticmethod
    async def get_all_stories(
            metric_id: str, grain: Granularity, tenant_id: int, created_date: date
    ) -> dict:
        """
        Fetches all stories based on the given metric_id, granularity, tenant_id, and created_date.
        Formats the stories into a dictionary for easier processing.
        """
        async with get_async_session() as db_session:
            story_crud = CRUDStory(model=Story, session=db_session)
            stories = await story_crud.get_stories(metric_id, grain, created_date, tenant_id)
            formatted_stories = [
                {
                    "story_group": story.story_group.value,
                    "metric_id": story.metric_id,
                    "title": story.title,
                    "detail": story.detail,
                }
                for story in stories
            ]
        context = {
            "stories": formatted_stories,
            "grain": grain.value,
            "time": datetime.utcnow(),
        }
        return context

    @staticmethod
    async def _send_slack_alerts(client: SlackNotifierDep, context: dict) -> Any:
        """
        Sends Slack notifications using the provided client and context.
        """
        response = await client.send_notification(
            template_name="stories_slack_template",
            config={"token": "xoxb-7976566008402-7962040521303-EKmzh6oZSFNp6QAXT7OpA3eq"},
            channel_config={"channel_id": "C07UT2BPC92"},
            context=context
        )
        return response

    async def process_and_send_alerts(self, metric_id: str, grain: Granularity, tenant_id: int, created_date: date):
        """
        Fetches all stories for the given parameters and sends Slack alerts.
        """
        try:
            # Fetch stories created today
            stories = await self.get_all_stories(metric_id, grain, tenant_id, created_date)

            # Get the Slack notifier client
            notifier = await get_slack_notifier()
            # Call the method to send alerts
            _ = await self._send_slack_alerts(notifier, stories)
        except Exception as e:
            # Log any errors that occur during the process
            logger.error(f"Failed to process and send alerts: {e}")
