from prefect import task

from commons.clients.insight_backend import InsightBackendClient
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from tasks_manager.config import AppConfig
from tasks_manager.utils import get_client_auth_from_config


@task(  # type: ignore
    task_run_name="fetch_relevant_alerts_tenant:{tenant_id}_metric:{metric_id}",
    tags=["db-operation", "notifications", "alerts"],
)
async def fetch_relevant_alerts(
    tenant_id: int, metric_id: str, grains: list[Granularity], story_groups: list[str] | None = None
):
    """Fetch relevant alerts for a specific metric and tenant"""
    # Set tenant context for proper data isolation
    set_tenant_id(tenant_id)

    # Initialize API clients
    config = await AppConfig.load("default")
    auth = get_client_auth_from_config(config)
    insights_client = InsightBackendClient(config.insights_backend_server_host, auth=auth)
    # Fetch relevant alerts
    alerts = await insights_client.list_alerts(metric_ids=[metric_id], grains=grains, story_groups=story_groups)  # type: ignore
    # Clean up tenant context after processing
    reset_context()
    return alerts["results"]


@task(  # type: ignore
    task_run_name="fetch_alert_by_id:{alert_id}",
    tags=["notifications", "alerts"],
)
async def fetch_alert_by_id(alert_id: int):
    """Fetch an alert by its ID"""
    # Initialize API clients
    config = await AppConfig.load("default")
    auth = get_client_auth_from_config(config)
    insights_client = InsightBackendClient(config.insights_backend_server_host, auth=auth)

    # Fetch the alert by alert_id
    alert = await insights_client.get(f"/notification/alerts/{alert_id}")

    return alert
