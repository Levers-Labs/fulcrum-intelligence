from datetime import date

from prefect import flow, get_run_logger, unmapped
from prefect.artifacts import create_table_artifact
from prefect.futures import PrefectFutureList

from story_manager.core.enums import StoryGroup
from tasks_manager.tasks.common import fetch_tenants
from tasks_manager.tasks.stories import generate_stories_for_tenant, update_demo_tenant_stories


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
    await create_table_artifact(  # type: ignore
        key=f"{today}-stories",
        table=stories,
        description=f"Heuristic stories generated for {today}",
    )

    logger.info("All stories generated for %s", today)

    return stories


@flow(
    name="update-demo-stories",
    description="Update demo stories dates for demo tenant based on granularity",
)
async def update_demo_stories(tenant_identifier: str):
    """
    Flow to update story dates for a specific tenant

    This flow increments story dates by one unit of their granularity:
    - Day grain: +1 day (updated daily)
    - Week grain: +1 week (updated only on Mondays)
    - Month grain: +1 month (updated only on 1st of month)
    - Quarter grain: +3 months (updated only on 1st day of quarter)
    - Year grain: +1 year (updated only on Jan 1st)

    Args:
        tenant_identifier: The tenant Identifier to run updates for.
    """
    current_date = date.today()
    logger = get_run_logger()

    # Convert tenant_id to int
    # tenant_id_int = await get_tenant_id(tenant_identifier=tenant_identifier)
    tenant_id_int = 3
    logger.info(f"Attempting to convert tenant identifier to int for {tenant_identifier}")

    logger.info(f"Starting story date updates for tenant id {tenant_id_int} as of {current_date}")

    try:
        # Process the specified tenant
        result = await update_demo_tenant_stories(tenant_id=tenant_id_int, current_date=current_date)
        logger.info(f"update_demo_tenant_stories call completed for tenant {tenant_id_int}")

        logger.info(
            f"Completed story date updates for tenant {tenant_id_int}: {result['stories_updated']} stories " f"updated"
        )
        return result

    except Exception as e:
        logger.error(f"Error updating story dates for tenant {tenant_id_int}: {e}")
        raise
