from datetime import date

from prefect import flow, get_run_logger, unmapped
from prefect.artifacts import create_table_artifact
from prefect.futures import PrefectFutureList

from story_manager.core.enums import StoryGroup
from tasks_manager.tasks.common import fetch_tenants
from tasks_manager.tasks.stories import generate_stories_for_tenant


@flow(  # type: ignore
    name="generate-stories",
    description="Generate stories for all tenants, metrics and groups",
)
async def generate_stories(story_date: date | None = None, groups: list[StoryGroup] | None = None):
    """
    Main flow to generate stories for all combinations

    Args:
        story_date: The date for which to generate stories. Defaults to today if not provided.
        groups: The list of story groups for which to generate stories.
                Defaults to all groups if not provided.
    """
    story_date = story_date or date.today()
    tenants = await fetch_tenants(enable_story_generation=True)  # type: ignore
    tenants_ids = [tenant["id"] for tenant in tenants]
    logger = get_run_logger()
    tenant_futures: PrefectFutureList = generate_stories_for_tenant.map(
        tenant_id=tenants_ids, story_date=story_date, groups=unmapped(groups)
    )

    # Wait for all stories to be generated
    results = tenant_futures.result()
    # Flatten the results
    stories = []
    for result in results:
        stories.extend(result)
    # Add stories as table artifact
    today = date.today()
    await create_table_artifact(
        key=f"{today}-stories",
        table=stories,
        description=f"Heuristic stories generated for {today}",
    )

    logger.info("All stories generated for %s", today)

    return stories
