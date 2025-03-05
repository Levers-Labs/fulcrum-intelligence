from prefect import get_run_logger, task

from commons.clients.insight_backend import InsightBackendClient
from commons.clients.query_manager import QueryManagerClient
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from tasks_manager.config import AppConfig
from tasks_manager.services.report_data_service import ReportDataService
from tasks_manager.utils import get_client_auth_from_config


@task(  # type: ignore
    task_run_name="fetch_report_by_id:{report_id}",
    tags=["notifications", "reports"],
)
async def fetch_report_by_id(report_id: int):
    """Fetch a report by its ID"""
    # Initialize API clients
    config = await AppConfig.load("default")
    auth = get_client_auth_from_config(config)
    insights_client = InsightBackendClient(config.insights_backend_server_host, auth=auth)

    # Fetch the report by report_id
    report = await insights_client.get(f"/notification/reports/{report_id}")

    return report


@task(  # type: ignore
    task_run_name="prepare_report_metrics_data:{tenant_id}:{metric_ids}",
    tags=["query-manager", "metrics", "reports"],
    retries=3,
    retry_delay_seconds=60,
)
async def prepare_report_metrics_data(
    tenant_id: int, metric_ids: list[str], grain: Granularity, comparisons: list[str] | None = None
) -> dict:
    """
    Prepare metrics data for report generation

    Args:
        tenant_id: The tenant ID
        metric_ids: List of metric IDs to fetch
        grain: Time granularity for the report
        comparisons: List of comparison types (e.g., ["PERCENTAGE_CHANGE"])

    Returns:
        Dictionary containing formatted metrics data for the report
    """
    logger = get_run_logger()
    logger.info("Preparing report metrics data for tenant %s, metrics %s", tenant_id, metric_ids)
    # Set tenant context
    set_tenant_id(tenant_id)

    try:
        # Initialize API clients
        config = await AppConfig.load("default")
        auth = get_client_auth_from_config(config)
        query_client = QueryManagerClient(config.query_manager_server_host, auth=auth)

        # Initialize report data service
        report_data_service = ReportDataService(query_client)

        # Prepare metrics data with or without comparisons
        metrics_data = await report_data_service.prepare_report_metrics_data(
            metric_ids=metric_ids, grain=Granularity(grain), include_raw_data=True, comparisons=comparisons
        )

        logger.info("Prepared report metrics data for tenant %s, metrics %s", tenant_id, metric_ids)
    except Exception as e:
        logger.error("Failed to prepare report metrics data, error: %s", str(e))
        raise e
    finally:
        # Clear tenant context
        reset_context()

    return metrics_data
