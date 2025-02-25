import ast
from datetime import date
from typing import Any

from prefect import flow, get_run_logger, unmapped
from prefect.futures import PrefectFutureList

from tasks_manager.tasks.alerts import execute_alert, fetch_relevant_alerts


@flow(name="metric-story-alerts", description="Process metric stories event for alerts")  # type: ignore
async def process_metric_stories_event(metric_id: str, tenant_id: str, story_date: date, stories_str: str):
    """
    Process metric stories event for alerts
    Args:
        metric_id: The metric ID for which to send alerts
        tenant_id: The tenant ID for which to send alerts
        story_date: The date (YYYY-MM-DD) for which to send alerts. Defaults to today if not provided.
        stories_str: The string representation of the list of stories.
    """
    tenant_id = int(tenant_id)  # type: ignore
    logger = get_run_logger()

    # Safely evaluate the string as a list
    stories: list[dict[str, Any]] = ast.literal_eval(stories_str)

    logger.info(
        "Processing metric stories event for metric %s for tenant %s on %s",
        metric_id,
        tenant_id,
        story_date,
    )

    # Get unique grains and story groups from the stories
    grains = {story.get("grain") for story in stories}
    story_groups = {story.get("story_group") for story in stories}

    # Fetch relevant alerts for the event
    alerts = await fetch_relevant_alerts(  # type: ignore
        tenant_id=tenant_id,
        metric_id=metric_id,
        grains=list(grains),
        story_groups=list(story_groups),
    )

    # If no alerts are found, skip the rest of the flow
    if not alerts:
        logger.info(
            "No relevant alerts found for metric %s for tenant %s on %s",
            metric_id,
            tenant_id,
            story_date,
        )
        return

    # Prepare execution parameters for alerts
    execution_params = {"story_date": story_date, "metric_id": metric_id, "story_groups": story_groups}

    # Execute alerts concurrently
    execute_alert_futures: PrefectFutureList = execute_alert.map(
        tenant_id=tenant_id, alert_id=[alert["id"] for alert in alerts], execution_params=unmapped(execution_params)
    )

    # Wait for all save operations to complete
    execute_alert_futures.wait()

    logger.info(
        "Successfully processed metric stories event for metric %s for tenant %s on %s",
        metric_id,
        tenant_id,
        story_date,
    )
