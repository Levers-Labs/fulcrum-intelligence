import ast
from datetime import date
from typing import Any

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.context import EngineContext, get_run_context
from prefect.events import emit_event

from commons.utilities.context import reset_context, set_tenant_id
from tasks_manager.services.alert_execution import get_alert_execution_service
from tasks_manager.tasks.alerts import fetch_alert_by_id, fetch_relevant_alerts
from tasks_manager.tasks.common import format_delivery_results
from tasks_manager.tasks.notifications import deliver_notifications, record_notification_execution


@flow(name="process-story-alerts", description="Process stories and trigger relevant alerts")  # type: ignore
async def process_story_alerts(metric_id: str, tenant_id: str, story_date: date, stories_str: str):
    """
    Process stories and trigger relevant alerts based on story content
    Args:
        metric_id: The metric ID for which to send alerts
        tenant_id: The tenant ID for which to send alerts
        story_date: The date (YYYY-MM-DD) for which to send alerts
        stories_str: The string representation of the list of stories
    """
    tenant_id = int(tenant_id)  # type: ignore
    logger = get_run_logger()

    # Safely evaluate the string as a list
    stories: list[dict[str, Any]] = ast.literal_eval(stories_str)

    logger.info(
        "Processing alerts for metric %s, tenant %s on %s",
        metric_id,
        tenant_id,
        story_date,
    )

    # Get unique grains and story groups from the stories
    grains = {story.get("grain") for story in stories}
    story_groups = {story.get("story_group") for story in stories}

    # Fetch relevant alerts for the event
    alerts: list[dict[str, Any]] = await fetch_relevant_alerts(  # type: ignore
        tenant_id=tenant_id,
        metric_id=metric_id,
        grains=list(grains),
        story_groups=list(story_groups),
    )

    # If no alerts are found, skip the rest of the flow
    if not alerts:
        logger.info(
            "No relevant alerts found for metric %s, tenant %s on %s",
            metric_id,
            tenant_id,
            story_date,
        )
        return

    # Prepare trigger parameters for alerts
    trigger_params = {
        "story_date": story_date,
        "metric_id": metric_id,
        "story_groups": list(story_groups),
        "trigger_type": "METRIC_STORY",
    }

    # Emit events for each alert that needs to be executed
    for alert in alerts:
        event = emit_event(
            event="alert.execution.requested",
            resource={
                "prefect.resource.id": f"alert.{alert['id']}",
                "tenant_id": str(tenant_id),
                "alert_id": str(alert["id"]),
            },
            payload={"trigger_params": trigger_params, "config": alert},
        )
        logger.info(
            "Triggered execution event for event %s, alert id %s",
            event,
            alert["id"],
        )

    logger.info(
        "Successfully processed alert events for metric %s, tenant %s on %s",
        metric_id,
        tenant_id,
        story_date,
    )


@flow(  # type: ignore
    name="execute-alert",
    description="Execute a single alert",
    retries=2,
    retry_delay_seconds=60,
)
async def execute_alert(tenant_id: str, alert_id: str, trigger_params_str: str):
    """
    Execute a single alert based on the provided configuration and parameters
    Args:
        tenant_id: The tenant ID for which to execute the alert
        alert_id: The ID of the alert to execute
        trigger_params_str: JSON string containing parameters from the trigger including:
            - story_date: The date for which stories were generated
            - metric_id: The metric ID for which stories were generated
            - story_groups: List of story groups that were generated
            - trigger_type: Type of trigger (e.g., METRIC_STORY)
    """
    logger = get_run_logger()
    logger.info("Executing alert %s for tenant %s", alert_id, tenant_id)

    # Convert string parameters to proper types
    tenant_id_int = int(tenant_id)  # type: ignore
    alert_id_int = int(alert_id)  # type: ignore
    trigger_params = ast.literal_eval(trigger_params_str)  # type: ignore

    # Set tenant context for proper data isolation
    set_tenant_id(tenant_id_int)

    try:
        # Get alert configuration
        alert: dict[str, Any] = await fetch_alert_by_id(alert_id_int)  # type: ignore
        if not alert:
            raise ValueError(f"Alert {alert_id} not found")

        # Validate trigger type matches
        trigger_type = alert["trigger"]["type"]
        if trigger_type != trigger_params["trigger_type"]:
            raise ValueError(f"Trigger type mismatch. Expected {trigger_type}, got {trigger_params['trigger_type']}")

        # Get notification channels from alert config
        notification_channels = alert.get("notification_channels", [])
        if not notification_channels:
            logger.warning("No notification channels configured for alert %s", alert_id)
            return

        # Initialize alert execution service
        alert_service = await get_alert_execution_service()

        logger.info("Preparing alert data for alert %s", alert_id)
        # Prepare alert data based on trigger type
        alert_data = await alert_service.prepare_alert_data(
            alert_config=alert,
            trigger_params=trigger_params,
        )

        logger.info("Alert data prepared for alert %s", alert_id)
        # Prepare context for notifications
        notification_context = {
            "data": alert_data,
            "config": alert,
            "trigger": trigger_params,
        }

        # Deliver notifications
        delivery_result = await deliver_notifications(  # type: ignore
            tenant_id=tenant_id_int,
            notification_channels=notification_channels,
            context=notification_context,
        )

        # Prepare run info from Prefect context
        run_context: EngineContext = get_run_context()  # type: ignore
        run_info = {}
        if run_context.flow_run:
            run_info = {
                "flow_run_id": run_context.flow_run.id,
                "flow_run_name": run_context.flow_run.name,
                "deployment_id": run_context.flow_run.deployment_id,
                "start_time": (
                    run_context.flow_run.start_time.strftime("%Y-%m-%dT%H:%M:%S")
                    if run_context.flow_run.start_time
                    else None
                ),
            }

        # Record execution details
        alert_meta = {
            "trigger_type": trigger_type,
            "metric_id": trigger_params.get("metric_id"),
            "story_date": trigger_params.get("story_date"),
            "story_groups": trigger_params.get("story_groups"),
            "alert_name": alert.get("name"),
        }

        execution = await record_notification_execution(  # type: ignore
            tenant_id=tenant_id_int,
            notification_type="ALERT",
            notification_id=alert_id_int,
            execution_result=delivery_result,
            metadata=alert_meta,
            run_info=run_info,
        )

        # Create execution summary artifact
        summary = f"""
        # Alert Execution Summary
        - Alert: {alert.get('name')} (ID: {alert_id})
        - Execution ID: {execution.get('id')}
        - Tenant: {tenant_id}
        - Status: {delivery_result.get('status', 'Unknown')}
        - Trigger Type: {trigger_type}
        - Success Rate: {delivery_result.get('success_count', 0)}/{delivery_result.get('total_count', 0)} deliveries

        ## Alert Details
        - Metric: {alert_data['metric'].get('label')} (ID: {trigger_params.get('metric_id')})
        - Story Date: {trigger_params.get('story_date')}
        - Story Groups: {', '.join(str(g) for g in trigger_params.get('story_groups', []))}
        - Grain: {alert.get('grain')}

        ## Stories Summary
        - Total Stories: {len(alert_data['stories'])}
        - Story Groups Found: {', '.join(set(story['story_group'] for story in alert_data['stories']))}

        ## Delivery Details
        {format_delivery_results(delivery_result.get('channel_results', []))}

        ## Run Info
        - Flow Run ID: {run_info.get('flow_run_id', 'Not available')}
        - Flow Run Name: {run_info.get('flow_run_name', 'Not available')}
        - Deployment ID: {run_info.get('deployment_id', 'Not available')}
        - Start Time: {run_info.get('start_time', 'Not available')}
        """
        await create_markdown_artifact(
            key=f"alert-{alert_id}-execution-{execution.get('id')}-summary",
            markdown=summary,
            description=f"Execution summary for alert {alert_id}",
        )

        logger.info("Successfully executed alert %s for tenant %s", alert_id, tenant_id)
        return delivery_result

    except Exception as e:
        logger.error(
            "Failed to execute alert %s for tenant %s: %s",
            alert_id,
            tenant_id,
            str(e),
        )
        raise
    finally:
        # Clean up tenant context
        reset_context()
