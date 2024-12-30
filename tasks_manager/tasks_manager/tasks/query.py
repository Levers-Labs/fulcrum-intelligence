from datetime import timedelta

from prefect import get_run_logger, task
from prefect.tasks import task_input_hash

from commons.clients.query_manager import QueryManagerClient
from commons.utilities.context import reset_context, set_tenant_id
from tasks_manager.config import AppConfig
from tasks_manager.utils import get_client_auth_from_config


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
