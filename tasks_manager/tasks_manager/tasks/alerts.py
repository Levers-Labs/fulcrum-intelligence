import os
from datetime import date

from prefect import task

from commons.utilities.context import reset_context, set_tenant_id
from story_manager.core.dependencies import get_insights_backend_client, get_query_manager_client
from story_manager.notifications.slack_alerts import SlackAlertsService
from tasks_manager.config import AppConfig


@task(  # type: ignore
    task_run_name="send_slack_alert_tenant:{tenant_id}_grain:{grain}_metric:{metric_id}",
    tags=["db-operation", "alerts", "slack-alerts"],
)
async def send_metric_grain_stories_alert(metric_id: str, tenant_id: int, grain: str, story_date: date):
    config = await AppConfig.load("default")
    os.environ["SERVER_HOST"] = config.story_manager_server_host

    # Initialize tenant context for proper data isolation
    set_tenant_id(tenant_id)

    # Initialize API clients
    query_client = await get_query_manager_client()
    insights_backend = await get_insights_backend_client()
    slack_connection_config = await insights_backend.get_slack_config()

    # Create alert handler and process alerts
    slack_alert_service = SlackAlertsService(query_client, slack_connection_config)
    await slack_alert_service.send_metric_stories_notification(
        grain=grain, tenant_id=tenant_id, created_date=story_date, metric_id=metric_id  # type: ignore
    )

    # Clean up tenant context after processing
    reset_context()
