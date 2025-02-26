from datetime import datetime
from typing import Any, Literal

from prefect import get_run_logger, task, unmapped
from prefect.futures import PrefectFutureList

from commons.clients.insight_backend import InsightBackendClient
from commons.models.enums import ExecutionStatus
from commons.utilities.context import reset_context, set_tenant_id
from tasks_manager.config import AppConfig
from tasks_manager.services.notification_delivery_service import NotificationDeliveryService
from tasks_manager.utils import get_client_auth_from_config


@task(  # type: ignore
    task_run_name="deliver_channel_notification",
    tags=["notifications", "delivery"],
    retries=2,
    retry_delay_seconds=30,
)
async def deliver_channel_notification(
    tenant_id: int,
    channel_config: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    """
    Deliver a notification through a specific channel.

    Args:
        tenant_id: The tenant ID
        channel_config: Configuration for the notification channel
        context: Context data for template rendering

    Returns:
        Dictionary containing delivery status and metadata
    """
    logger = get_run_logger()
    logger.info("Delivering notification through channel %s", channel_config["channel_type"])

    # Set tenant id
    set_tenant_id(tenant_id)

    # Initialize services
    config = await AppConfig.load("default")
    auth = get_client_auth_from_config(config)
    insights_client = InsightBackendClient(config.insights_backend_server_host, auth=auth)
    delivery_service = NotificationDeliveryService(insights_client, config)

    # Deliver notification
    result = await delivery_service.deliver_channel_notification(channel_config=channel_config, context=context)

    logger.info("Delivered notification through channel %s", channel_config["channel_type"])
    # Reset tenant id
    reset_context()

    return result


@task(  # type: ignore
    task_run_name="deliver_notifications_tenant:{tenant_id}",
    tags=["notifications", "delivery"],
)
async def deliver_notifications(
    tenant_id: int,
    notification_channels: list[dict[str, Any]],
    context: dict[str, Any],
) -> dict[str, Any]:
    """
    Deliver notifications through multiple channels in parallel.

    Args:
        tenant_id: The tenant ID
        notification_channels: List of channel configurations
            Each channel config contains:
            - channel_type: The type of channel (slack, email, etc.)
            - template: The template configuration
            - recipients: List of recipients
        context: Context data for template rendering
            For reports: {"data": report_data, "config": report_config}
            For alerts: {"data": alert_data, "config": alert_config}

    Returns:
        Dictionary containing:
            - status: COMPLETED/FAILED/PARTIAL
            - success_count: Number of successful deliveries
            - total_count: Total number of delivery attempts
            - channel_results: List of delivery results per channel
    """
    logger = get_run_logger()
    logger.info("Delivering notifications through %d channels", len(notification_channels))

    # Set tenant id
    set_tenant_id(tenant_id)

    # Deliver notifications through all channels in parallel
    delivery_results_futures: PrefectFutureList = deliver_channel_notification.map(
        tenant_id=tenant_id,
        channel_config=notification_channels,
        context=unmapped(context),
    )

    # Process results
    channel_results = []
    total_deliveries = 0
    successful_deliveries = 0

    for result_future in delivery_results_futures:
        result = result_future.result()
        channel_results.append(result)

        # Aggregate counts
        total_deliveries += result["total_count"]
        successful_deliveries += result["success_count"]

    logger.info("Total deliveries: %d, Successful deliveries: %d", total_deliveries, successful_deliveries)
    # Reset tenant id
    reset_context()

    return {
        "status": (
            ExecutionStatus.COMPLETED
            if successful_deliveries == total_deliveries
            else ExecutionStatus.FAILED if successful_deliveries == 0 else ExecutionStatus.PARTIAL
        ),
        "success_count": successful_deliveries,
        "total_count": total_deliveries,
        "channel_results": channel_results,
    }


@task(  # type: ignore
    task_run_name="record_notification_execution:{tenant_id}:{notification_type}:{notification_id}",
    tags=["notifications"],
)
async def record_notification_execution(
    tenant_id: int,
    notification_type: Literal["ALERT", "REPORT"],
    notification_id: int,
    execution_result: dict,
    metadata: dict,
    run_info: dict,
) -> dict[str, Any]:
    """
    Record notification execution status and details for both alerts and reports

    Args:
        tenant_id: The tenant ID
        notification_type: Type of notification (ALERT or REPORT)
        notification_id: The alert or report ID
        execution_result: Result of notification delivery containing status and channel results
        metadata: Metadata about the execution (alert_meta or report_meta)
        run_info: Prefect run information from the flow

    Returns:
        Dictionary containing the recorded execution details
    """
    logger = get_run_logger()
    logger.info("Recording %s execution for id %s", notification_type.lower(), notification_id)

    # Set tenant context
    set_tenant_id(tenant_id)

    try:
        # Initialize API clients
        config = await AppConfig.load("default")
        auth = get_client_auth_from_config(config)
        insights_client = InsightBackendClient(config.insights_backend_server_host, auth=auth)

        # Prepare error info if execution failed
        error_info = None
        if execution_result.get("error"):
            error_info = {"error_type": "DELIVERY_FAILURE", "message": execution_result["error"]}
        elif execution_result["status"] == ExecutionStatus.PARTIAL:
            failed_channels = [
                result
                for result in execution_result.get("channel_results", [])
                if result.get("status") != ExecutionStatus.COMPLETED
            ]
            error_info = {
                "error_type": "PARTIAL_DELIVERY_FAILURE",
                "message": f"Failed channels: {[d.get('channel') for d in failed_channels]}",
            }

        execution = {
            "executed_at": run_info.get("start_time", datetime.now().strftime("%Y-%m-%dT%H:%M:%S")),
            "status": ExecutionStatus(execution_result["status"]),
            "delivery_meta": execution_result,
            "run_info": run_info,
            "error_info": error_info,
            "notification_type": notification_type,
        }

        # Add type-specific fields
        if notification_type == "REPORT":
            execution["report_id"] = notification_id
            execution["report_meta"] = metadata
        else:  # ALERT
            execution["alert_id"] = notification_id
            execution["trigger_meta"] = metadata

        execution = await insights_client.create_notification_execution(execution)
        logger.info(
            "Recorded %s execution for id %s with execution id: %s",
            notification_type.lower(),
            notification_id,
            execution["id"],
        )
        return execution

    finally:
        # Clear tenant context
        reset_context()
