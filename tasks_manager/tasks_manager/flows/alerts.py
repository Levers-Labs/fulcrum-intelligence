from datetime import date

from prefect import flow, get_run_logger

from tasks_manager.tasks.alerts import send_metric_grain_stories_alert
from tasks_manager.tasks.common import get_grains


@flow(name="metric-story-alerts", description="Send metric stories alert to slack")  # type: ignore
async def send_metric_stories_alert(metric_id: str, tenant_id: str, story_date: date):
    """
    Send metric stories alert to slack
    Args:
        metric_id: The metric ID for which to send Slack alerts
        tenant_id: The tenant ID for which to send Slack alerts
        story_date: The date (YYYY-MM-DD) for which to send alerts. Defaults to today if not provided.
    """
    tenant_id = int(tenant_id)  # type: ignore
    logger = get_run_logger()
    logger.info("Sending metric stories alert for metric %s for tenant %s on %s", metric_id, tenant_id, story_date)

    # Get grains for the given tenant and story date
    grains = get_grains(tenant_id, story_date)  # type: ignore

    # Invoke the task for each grain
    task_futures = send_metric_grain_stories_alert.map(  # type: ignore
        grain=grains, metric_id=metric_id, tenant_id=tenant_id, story_date=story_date
    )
    # Wait for all tasks to complete
    task_futures.wait()
    logger.info("Successfully processed alerts for metric %s for tenant %s on %s", metric_id, tenant_id, story_date)
