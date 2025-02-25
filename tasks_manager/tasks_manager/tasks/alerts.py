from prefect import get_run_logger, task

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
    task_run_name="execute_alert_tenant:{tenant_id}_alert:{alert_id}",
    tags=["db-operation", "notifications", "alerts"],
)
async def execute_alert(tenant_id: int, alert_id: int, execution_params: dict):
    """Send metric stories alert for a specific grain and alert configuration
    For Story based trigger the execution_params will have
        - story_date
        - metric_id
        - grain
        - story_groups
    For Metric based trigger the execution_params will have
        (TBD)
    """
    logger = get_run_logger()
    logger.info("Executing alert %s for tenant %s", alert_id, tenant_id)

    # config = await AppConfig.load("default")

    # Initialize tenant context for proper data isolation
    # set_tenant_id(tenant_id)

    # try:
    # Initialize API clients
    # query_client = await get_query_manager_client()
    # insights_backend = await get_insights_backend_client()

    # # Get alert configuration
    # alert = await insights_backend.get_alert(alert_id)
    # if not alert:
    #     raise ValueError(f"Alert {alert_id} not found")

    # slack_connection_config = await insights_backend.get_slack_config()

    # Create alert handler and process alerts
    # slack_alert_service = SlackAlertsService(query_client, slack_connection_config)
    # grain = execution_params.get("grain")
    # story_date = execution_params.get("story_date")
    # metric_id = execution_params.get("metric_id")
    # result = await slack_alert_service.send_metric_stories_notification(
    #     grain=grain, tenant_id=tenant_id, created_date=story_date, metric_id=metric_id, alert_config=alert
    # )

    # # Create execution record
    # await create_execution_record(
    #     alert_id=alert_id,
    #     status=ExecutionStatus.COMPLETED,
    #     trigger_meta={
    #         "metric_id": metric_id,
    #         "grain": grain,
    #         "story_date": story_date,
    #     },
    #     delivery_meta=result,
    # )

    # except Exception as e:
    #     # Create failed execution record
    #     await create_execution_record(
    #         alert_id=alert_id,
    #         status=ExecutionStatus.FAILED,
    #         trigger_meta={
    #             "metric_id": metric_id,
    #             "grain": grain,
    #             "story_date": story_date,
    #         },
    #         error_info={"error_type": type(e).__name__, "message": str(e)},
    #     )
    #     raise
    # finally:
    #     # Clean up tenant context after processing
    #     reset_context()
    logger.info("Alert %s executed for tenant %s", alert_id, tenant_id)
