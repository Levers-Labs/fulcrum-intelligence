from datetime import date

from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact

from tasks_manager.tasks.common import fetch_tenants
from tasks_manager.tasks.stories import generate_stories_for_tenant


@flow(  # type: ignore
    name="Generate Stories",
    description="Generate stories for all tenants, metrics and groups",
)
async def generate_stories(group: str):
    """Main flow to generate stories for all combinations"""
    tenants = await fetch_tenants(enable_story_generation=True)  # type: ignore
    logger = get_run_logger()
    logger.info("Fetched %d tenants", len(tenants))
    tenant_futures = []
    for tenant in tenants:
        logger.info("Generating stories for tenant %d and group %s", tenant["id"], group)
        tenant_future = generate_stories_for_tenant.submit(tenant_id=tenant["id"], group=group)  # type: ignore
        tenant_futures.append(tenant_future)

    stories = []
    for future in tenant_futures:
        stories.extend(future.result())
    # Add stories as table artifact
    today = date.today()
    group_str = group.replace("_", "-").lower()
    await create_table_artifact(
        key=f"{today}-{group_str}-stories",
        table=stories,
        description=f"Heuristic stories generated for group {group} for {today}",
    )

    logger.info("All stories generated for group %s", group)

    return stories
