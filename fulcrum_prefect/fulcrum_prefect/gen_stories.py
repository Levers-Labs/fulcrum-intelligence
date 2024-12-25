import os
from datetime import datetime, timedelta

from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash

from commons.clients.insight_backend import InsightBackendClient
from commons.clients.query_manager import QueryManagerClient
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from fulcrum_prefect.config import AppConfig
from fulcrum_prefect.utils import get_client_auth_from_config, get_eligible_grains
from story_manager.core.enums import StoryGroup
from story_manager.story_builder.manager import StoryManager


@task(  # type: ignore
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    task_run_name="fetch_tenants",
)
async def fetch_tenants() -> list[dict]:
    """Fetch all active tenants"""
    logger = get_run_logger()
    config = await AppConfig.load("default")
    auth = get_client_auth_from_config(config)
    insights_client = InsightBackendClient(config.insights_backend_server_host, auth=auth)
    response = await insights_client.get_tenants()
    tenants = response["results"]
    names = [tenant["name"] for tenant in tenants]
    logger.info("Fetched %d tenants: %s", len(tenants), names)
    return tenants


@task(  # type: ignore
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    task_run_name="fetch_metrics_tenant:{tenant_id}",
)
async def fetch_metrics_for_tenant(tenant_id: int) -> list[dict]:
    """Fetch metrics for a specific tenant"""
    set_tenant_id(tenant_id)
    # Replace with your actual metric fetching logic
    logger = get_run_logger()
    logger.info(f"Fetching metrics for tenant {tenant_id}")
    config = await AppConfig.load("default")
    auth = get_client_auth_from_config(config)
    query_manager = QueryManagerClient(config.query_manager_server_host, auth=auth)
    metrics = await query_manager.list_metrics()
    reset_context()
    return [{"id": metric["id"], "metric_id": metric["metric_id"]} for metric in metrics]


@task(task_run_name="get_grains_tenant:{tenant_id}_group:{group}")  # type: ignore
def get_grains(tenant_id: int, group: str) -> list[str]:
    """Get available grains for a tenant and group."""
    set_tenant_id(tenant_id)
    logger = get_run_logger()
    logger.info(f"Getting grains for tenant {tenant_id} and group {group}")
    today = datetime.now()
    grains = get_eligible_grains(list(Granularity.__members__.values()), today)
    logger.info("Eligible grains: %s, day: %s", grains, today)
    reset_context()
    return grains


@task(  # type: ignore
    retries=3,
    retry_delay_seconds=60,
    tags=["story_generation"],
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


@flow(  # type: ignore
    name="Generate Stories",
    description="Generate stories for all tenants, metrics and groups",
)
async def generate_stories(group: str):
    """Main flow to generate stories for all combinations"""
    tenants = await fetch_tenants()  # type: ignore
    logger = get_run_logger()
    logger.info("Fetched %d tenants", len(tenants))
    tenant_futures = []
    for tenant in tenants:
        logger.info("Generating stories for tenant %d and group %s", tenant["id"], group)
        tenant_future = generate_stories_for_tenant.submit(tenant_id=tenant["id"], group=group)  # type: ignore
        tenant_futures.append(tenant_future)

    for future in tenant_futures:
        future.wait()

    logger.info("All stories generated for group %s", group)
