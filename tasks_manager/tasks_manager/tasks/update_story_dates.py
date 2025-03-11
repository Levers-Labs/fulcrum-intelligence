from datetime import date
from typing import Dict, List, Tuple

from prefect import get_run_logger, task

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from story_manager.core.enums import StoryGroup
from story_manager.story_builder.manager import StoryManager

# from story_manager.mocks.services.data_service import MockDataService


@task(
    retries=3,
    retry_delay_seconds=60,
    tags=["story-date-update", "db-operation"],
    task_run_name="update_story_dates_tenant:{tenant_id}",
)
async def update_story_dates_for_tenant(
    tenant_id: int, current_date: date, num_periods: int = 4  # Number of periods to generate (current + previous)
) -> list[dict]:
    """Update story dates for a specific tenant"""
    logger = get_run_logger()

    # Setup tenant context
    set_tenant_id(tenant_id)
    logger.info(f"Updating story dates for tenant {tenant_id}, current date {current_date}")

    # Get all story groups
    all_groups = list(StoryGroup.__members__.values())

    # Get all grains
    all_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    # Determine which grains to update based on the current date
    grains_to_update = []
    # Always update day grain
    grains_to_update.append(Granularity.DAY)

    # Update week grain on Mondays
    if current_date.weekday() == 0:  # Monday is 0
        grains_to_update.append(Granularity.WEEK)

    # Update month grain on the 1st of the month
    if current_date.day == 1:
        grains_to_update.append(Granularity.MONTH)

    # Create a mock data service to use its date functions
    mock_service = MockDataService(story_date=current_date)

    # Fetch all stories for this tenant
    all_stories = await StoryManager.get_stories_by_tenant(tenant_id=tenant_id)

    # Group stories by grain, story_group, and metric_id
    story_groups: dict[tuple[Granularity, StoryGroup, int], list[dict]] = {}
    for story in all_stories:
        grain = Granularity(story["grain"])
        if grain not in grains_to_update:
            continue

        story_group = StoryGroup(story["story_group"])
        metric_id = story["metric_id"]
        key = (grain, story_group, metric_id)

        if key not in story_groups:
            story_groups[key] = []
        story_groups[key].append(story)

    # Update story dates
    updated_stories = []
    for (grain, story_group, metric_id), stories in story_groups.items():
        # Get the date range for this grain and story group
        start_date, end_date = mock_service._get_input_time_range(grain, story_group)

        # Get all dates in the range
        dates = mock_service.get_dates_for_range(grain=grain, start_date=start_date, end_date=end_date)

        # Sort dates in descending order (newest first)
        dates.sort(reverse=True)

        # Sort stories by story_type to keep stories of the same type together
        stories_by_type: dict[str, list[dict]] = {}
        for story in stories:
            story_type = story["story_type"]
            if story_type not in stories_by_type:
                stories_by_type[story_type] = []
            stories_by_type[story_type].append(story)

        # Update dates for each story type
        for story_type, type_stories in stories_by_type.items():
            # Sort stories by id (or another criteria)
            sorted_stories = sorted(type_stories, key=lambda s: s["id"])

            # Update dates for up to num_periods stories or as many dates as we have
            for i, story_date in enumerate(dates[:num_periods]):
                if i < len(sorted_stories):
                    story = sorted_stories[i]
                    # Update story date
                    story["story_date"] = story_date
                    # Save updated story
                    await StoryManager.update_story(story["id"], {"story_date": story_date})
                    updated_stories.append(story)

    # Clean up tenant context
    reset_context()

    logger.info(f"Updated {len(updated_stories)} story dates for tenant {tenant_id}")

    return updated_stories
