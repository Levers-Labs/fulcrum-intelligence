from datetime import date, timedelta

from prefect import get_run_logger, task
from prefect.tasks import task_input_hash

from commons.clients.insight_backend import InsightBackendClient
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from tasks_manager.config import AppConfig
from tasks_manager.utils import get_client_auth_from_config, get_eligible_grains


@task(  # type: ignore
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    task_run_name="fetch_tenants",
)
async def fetch_tenants(**params) -> list[dict]:
    """Fetch all active tenants"""
    logger = get_run_logger()
    config = await AppConfig.load("default")
    auth = get_client_auth_from_config(config)
    insights_client = InsightBackendClient(config.insights_backend_server_host, auth=auth)
    response = await insights_client.get_tenants(**params)
    tenants = response["results"]
    names = [tenant["name"] for tenant in tenants]
    logger.info("Fetched %d tenants: %s", len(tenants), names)
    return tenants


@task(task_run_name="get_grains_tenant:{tenant_id}")  # type: ignore
def get_grains(tenant_id: int, day: date | None = None) -> list[str]:
    """Get available grains for a tenant and group."""
    set_tenant_id(tenant_id)
    logger = get_run_logger()
    logger.info(f"Getting grains for tenant {tenant_id}")
    today = day or date.today()
    grains = get_eligible_grains(list(Granularity.__members__.values()), today)
    logger.info("Eligible grains: %s, day: %s", grains, today)
    reset_context()
    return grains
