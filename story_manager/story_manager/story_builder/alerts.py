import logging
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.crud import CRUDStory
from story_manager.core.enums import StoryGroup
from story_manager.core.models import Story
from story_manager.db.config import get_async_session

logger = logging.getLogger(__name__)


class StoryAlerts:

    @staticmethod
    async def get_all_stories(
        group: StoryGroup, metric_id, grain: Granularity, tenant_id: int, created_date: date
    ) -> Any:
        async with get_async_session() as db_session:
            story_crud = CRUDStory(model=Story, session=db_session)
            stories = await story_crud.get_stories(metric_id, group, grain, created_date, tenant_id)
        return stories

    # async def _send_slack_alerts(self) -> None:
    #     # TODO: response = send_notification(
    #     #     template_name,
    #     #     config,
    #     #     channel_config,
    #     #     context)
    #     return None
