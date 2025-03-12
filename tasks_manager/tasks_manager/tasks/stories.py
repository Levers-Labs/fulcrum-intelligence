import os
from datetime import date

from prefect import get_run_logger, task, unmapped
from prefect.events import emit_event
from prefect.futures import PrefectFutureList

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
    tags=["story-generation", "db-operation"],
    task_run_name="gen_story_tenant:{tenant_id}_metric:{metric_id}_grain:{grain}_group:{group}",
)
async def generate_story(
    tenant_id: int, metric_id: int, grain: Granularity, group: StoryGroup, story_date: date | None = None
) -> dict:
    """Generate a story for specific tenant, metric and group"""
    logger = get_run_logger()
    config = await AppConfig.load("default")
    # Setup tenant context
    set_tenant_id(tenant_id)
    os.environ["SERVER_HOST"] = config.story_manager_server_host
    logger.info(
        "Generating story for tenant %s, metric %s, grain %s, group %s",
        tenant_id,
        metric_id,
        grain.value,
        group.value,
    )

    # Generate a story for tenant, metric and grain
    story_date = story_date or date.today()
    story_dicts = await StoryManager.run_builder_for_story_group(
        group=StoryGroup(group), metric_id=metric_id, grain=Granularity(grain), story_date=story_date
    )
    # Need to keep these keys in the story Artifact
    artifact_keys = [
        "id",
        "tenant_id",
        "genre",
        "story_group",
        "story_type",
        "grain",
        "metric_id",
        "title",
        "detail",
        "story_date",
    ]
    # Filter story dicts to keep only the keys we want and heuristic stories
    story_records = [{key: story[key] for key in artifact_keys} for story in story_dicts if story["is_heuristic"]]
    # Clean up tenant context
    reset_context()

    if story_records:
        logger.info(
            "Story generated for tenant %s, metric %s, grain %s, group %s",
            tenant_id,
            metric_id,
            grain.value,
            group.value,
        )
    else:
        logger.info(
            "No story generated for tenant %s, metric %s, grain %s, group %s",
            tenant_id,
            metric_id,
            grain.value,
            group.value,
        )
    return {
        "tenant_id": tenant_id,
        "metric_id": metric_id,
        "grain": grain,
        "group": group,
        "story_date": story_date,
        "stories": story_records,
    }


@task(task_run_name="gen_stories_tenant:{tenant_id}_metric:{metric_id}")  # type: ignore
async def generate_stories_for_metric(
    tenant_id: int, metric_id: str, story_date: date | None = None, groups: list[StoryGroup] | None = None
):
    """Generate stories for a specific tenant and metric across all grains and groups"""
    logger = get_run_logger()
    story_date = story_date or date.today()
    grains = get_grains(tenant_id, story_date)  # type: ignore
    # In the future, we can fetch from tenant config
    groups = groups or list(StoryGroup.__members__.values())
    logger.info(
        "Generating stories for tenant %s, metric %s, grains %s, groups %s, story_date %s",
        tenant_id,
        metric_id,
        grains,
        groups,
        story_date,
    )

    # Generate stories concurrently
    story_futures = []
    for grain in grains:
        for group in groups:
            story_future = generate_story.submit(  # type: ignore
                tenant_id=tenant_id, metric_id=metric_id, grain=grain, group=group, story_date=story_date
            )
            story_futures.append(story_future)

    # Collect stories
    stories = []
    if story_futures:
        for future in story_futures:
            res = future.result()
            stories.extend(res["stories"])  # type: ignore

    # If we have stories, we can emit an event
    if stories:
        emit_event(
            event="metric.story.generated",
            resource={
                "prefect.resource.id": f"{tenant_id}.{metric_id}",
                "metric_id": metric_id,
                "tenant_id": str(tenant_id),
                "story_date": story_date.isoformat(),
            },
            payload={"stories": stories},
        )

    logger.info("Generated %d stories for tenant %s, metric %s", len(stories), tenant_id, metric_id)
    return stories


@task(task_run_name="gen_stories_tenant:{tenant_id}")  # type: ignore
async def generate_stories_for_tenant(
    tenant_id: int, story_date: date | None = None, groups: list[StoryGroup] | None = None
):
    """Generate stories for a specific tenant and group"""
    story_date = story_date or date.today()
    # Fetch metrics and groups concurrently using submit
    metrics_future = fetch_metrics_for_tenant.submit(tenant_id)  # type: ignore
    # Get results from futures
    metrics = metrics_future.result()
    metric_ids = [metric["metric_id"] for metric in metrics]  # type: ignore
    logger = get_run_logger()
    logger.info("Tenant %s, metrics %s", tenant_id, metric_ids)

    # Generate and save stories concurrently
    metric_story_futures: PrefectFutureList = generate_stories_for_metric.map(
        tenant_id=tenant_id, metric_id=metric_ids, story_date=story_date, groups=unmapped(groups)
    )

    # Wait for all save operations to complete
    results = metric_story_futures.result()
    all_stories: list[dict] = []
    for result in results:
        all_stories.extend(result)
    return all_stories
