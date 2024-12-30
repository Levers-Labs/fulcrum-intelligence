import os

from prefect import get_run_logger, task

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from story_manager.core.enums import StoryGroup
from story_manager.story_builder.manager import StoryManager
from tasks_manager.config import AppConfig
from tasks_manager.tasks.common import get_grains
from tasks_manager.tasks.query import fetch_metrics_for_tenant


@task(  # type: ignore
    retries=3,
    retry_delay_seconds=60,
    tags=["story-generation"],
    task_run_name="gen_story_tenant:{tenant_id}_metric:{metric_id}_grain:{grain}_group:{group}",
)
async def generate_story(tenant_id: int, metric_id: int, grain: str, group: str) -> dict:
    """Generate a story for specific tenant, metric and group"""
    logger = get_run_logger()
    config = await AppConfig.load("default")
    set_tenant_id(tenant_id)
    os.environ["SERVER_HOST"] = config.story_manager_server_host
    # Setup tenant context
    logger.info("Setting up tenant context for tenant %s", tenant_id)
    logger.info(f"Generating story for tenant {tenant_id}, metric {metric_id}, grain {grain}")

    # Generate a story for tenant, metric and grain
    await StoryManager.run_builder_for_story_group(
        group=StoryGroup(group), metric_id=metric_id, grain=Granularity(grain)
    )
    logger.info("Story generated for tenant %s, metric %s, grain %s", tenant_id, metric_id, grain)
    reset_context()
    return {
        "tenant_id": tenant_id,
        "metric_id": metric_id,
        "grain": grain,
        "group": group,
        "story_generated": True,
    }


@task(task_run_name="gen_stories_tenant:{tenant_id}_group:{group}")  # type: ignore
async def generate_stories_for_tenant(tenant_id: int, group: str):
    """Generate stories for a specific tenant and group"""
    # Fetch metrics and groups concurrently using submit
    metrics_future = fetch_metrics_for_tenant.submit(tenant_id)  # type: ignore
    grains_future = get_grains.submit(tenant_id, group)  # type: ignore
    # Get results from futures
    metrics = metrics_future.result()
    grains = grains_future.result()
    logger = get_run_logger()
    logger.info("Metrics: %s, grains: %s", metrics, grains)

    # Generate and save stories concurrently
    story_futures = []
    for metric in metrics:
        for grain in grains:
            # Submit story generation
            story_future = generate_story.submit(  # type: ignore
                tenant_id=tenant_id,
                metric_id=metric["metric_id"],
                grain=grain,
                group=group,
            )
            story_futures.append(story_future)

    # Wait for all save operations to complete
    if story_futures:
        for future in story_futures:
            future.wait()
