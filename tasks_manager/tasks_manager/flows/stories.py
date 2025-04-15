import ast
from datetime import date

from prefect import flow, get_run_logger, unmapped
from prefect.artifacts import create_table_artifact
from prefect.events import emit_event
from prefect.futures import PrefectFutureList

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGroup
from tasks_manager.tasks.common import fetch_tenants, get_tenant_by_identifier
from tasks_manager.tasks.stories import generate_stories_for_tenant, process_pattern_stories, update_demo_tenant_stories


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

    # fetch tenant_id for the identifier
    tenant_id_int = await get_tenant_by_identifier(identifier=tenant_identifier)
    logger.info(f"Starting story date updates for tenant id {tenant_id_int} as of {current_date}")

    try:
        # Process the specified tenant
        result = update_demo_tenant_stories.submit(tenant_id=tenant_id_int, current_date=current_date)
        logger.info(f"update_demo_tenant_stories call completed for tenant {tenant_id_int}")

        logger.info(f"Completed story date updates for tenant {tenant_id_int}.")
        return result

    except Exception as e:
        logger.error(f"Error updating story dates for tenant {tenant_id_int}: {e}")
        raise


@flow(
    name="process-metric-pattern-stories",
    flow_run_name="process-metric-pattern-stories:tenant={tenant_id_str}_pattern={pattern}_metric={metric_id}_grain={grain}",  # noqa
    description="Process stories based on pattern run output",
    retries=2,
    retry_delay_seconds=15,
)
async def process_metric_pattern_stories(
    pattern: str, tenant_id_str: str, metric_id: str, grain: str, pattern_run_str: str
):
    """
    Process stories based on pattern run output.
    This flow is triggered by the pattern.run.success event.

    Args:
        pattern: Pattern name
        tenant_id_str: Tenant ID as string
        metric_id: Metric ID
        grain: Granularity (day, week, month)
        pattern_run_str: JSON string containing pattern run results
    """
    logger = get_run_logger()
    tenant_id = int(tenant_id_str)
    grain_enum = Granularity(grain)
    pattern_run = ast.literal_eval(pattern_run_str)
    # Extract the run result from pattern_run
    pattern_run_result = pattern_run["run_result"]

    logger.info(
        "Starting story generation for tenant %s, pattern %s, metric %s, grain %s",
        tenant_id,
        pattern,
        metric_id,
        grain,
    )

    # Emit start event
    emit_event(
        event="metric.story.generation.started",
        resource={
            "prefect.resource.id": f"metric.{metric_id}",
            "metric_id": metric_id,
            "tenant_id": tenant_id_str,
            "grain": grain,
            "pattern": pattern,
        },
        payload={
            "timestamp": date.today().isoformat(),
            "pattern_run": pattern_run_result,
        },
    )

    try:
        # Process pattern results and generate stories
        stories = await process_pattern_stories(
            pattern=pattern,
            tenant_id=tenant_id,
            metric_id=metric_id,
            grain=grain_enum,
            pattern_run=pattern_run_result,
        )

        # Create table artifact with stories
        if stories:
            key = f"{metric_id.lower()}-{grain.lower()}-{pattern.lower()}-stories"
            # replace underscore, space with dashes
            key = key.replace("_", "-")
            key = key.replace(" ", "-")
            await create_table_artifact(key=key, table=stories)  # type: ignore

        # Emit success event
        emit_event(
            event="metric.story.generation.success",
            resource={
                "prefect.resource.id": f"metric.{metric_id}",
                "metric_id": metric_id,
                "tenant_id": tenant_id_str,
                "grain": grain,
                "pattern": pattern,
            },
            payload={
                "timestamp": date.today().isoformat(),
                "count": len(stories),
                "results": stories,
            },
        )

        logger.info(
            "Successfully processed stories for tenant %s, pattern %s, metric %s, grain %s. Generated %d stories.",
            tenant_id,
            pattern,
            metric_id,
            grain,
            len(stories),
        )

        return stories
    except Exception as e:
        logger.error(
            "Failed to process stories for tenant %s, pattern %s, metric %s, grain %s: %s",
            tenant_id,
            pattern,
            metric_id,
            grain,
            str(e),
            exc_info=True,
        )

        # Emit failure event
        emit_event(
            event="metric.story.generation.failed",
            resource={
                "prefect.resource.id": f"metric.{metric_id}",
                "metric_id": metric_id,
                "tenant_id": tenant_id_str,
                "grain": grain,
                "pattern": pattern,
            },
            payload={
                "timestamp": date.today().isoformat(),
                "error": {
                    "message": str(e),
                    "type": type(e).__name__,
                },
            },
        )
        raise
