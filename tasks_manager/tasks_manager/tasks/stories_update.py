import os
from datetime import date
from typing import Any

from prefect import get_run_logger, task

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from story_manager.core.crud import CRUDStory
from story_manager.core.models import Story
from story_manager.db.config import get_async_session
from tasks_manager.config import AppConfig
from tasks_manager.utils import increment_date_by_grain, should_update_grain


@task(
    retries=3,
    retry_delay_seconds=60,
    tags=["story-update", "db-operation"],
    task_run_name="update_story_dates_tenant:{tenant_id}",
)
async def update_story_dates_for_tenant(tenant_id: int, current_date: date) -> dict[str, Any]:
    """
    Update story dates for a specific tenant

    :param tenant_id: The tenant ID
    :param current_date: The current date to check for updates
    :return: Dictionary with update results
    """
    logger = get_run_logger()
    config = await AppConfig.load("default")
    # Setup tenant context
    set_tenant_id(tenant_id)
    os.environ["SERVER_HOST"] = config.story_manager_server_host

    logger.info(f"Updating story dates for tenant {tenant_id} as of {current_date}")

    updates_by_grain = {
        Granularity.DAY: 0,
        Granularity.WEEK: 0,
        Granularity.MONTH: 0,
        Granularity.QUARTER: 0,
        Granularity.YEAR: 0,
    }

    stories_updated = 0

    async with get_async_session() as session:
        # Get all stories for this tenant
        # stories = await fetch_all_stories(tenant_id=tenant_id)
        story_crud = CRUDStory(model=Story, session=session)
        stories = await story_crud.get_stories(tenant_id=tenant_id)

        for story_dict in stories:
            grain = Granularity(story_dict["grain"])
            story_date = story_dict["story_date"]

            # Check if we should update this grain today
            if should_update_grain(current_date, grain):
                # Get a new story date by incrementing by one grain
                new_story_date = increment_date_by_grain(story_date, grain)
                # Update the story with the new date
                updated = await story_crud.update_story_date(story_id=story_dict["id"], new_date=new_story_date)
                if updated:
                    updates_by_grain[grain] += 1
                    stories_updated += 1

        # Commit all updates
        await session.commit()

    # Clean up tenant context
    reset_context()

    logger.info(f"Updated {stories_updated} stories for tenant {tenant_id}")
    logger.info(
        f"Updates by grain: Day: {updates_by_grain[Granularity.DAY]}, "
        f"Week: {updates_by_grain[Granularity.WEEK]}, "
        f"Month: {updates_by_grain[Granularity.MONTH]}, "
        f"Quarter: {updates_by_grain[Granularity.QUARTER]}, "
        f"Year: {updates_by_grain[Granularity.YEAR]}"
    )

    return {"tenant_id": tenant_id, "stories_updated": stories_updated, "updates_by_grain": updates_by_grain}
