from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect.context import EngineContext, get_run_context

from commons.utilities.context import reset_context, set_tenant_id
from fulcrum_core.enums import Granularity
from tasks_manager.tasks.common import format_delivery_results
from tasks_manager.tasks.notifications import deliver_notifications, record_notification_execution
from tasks_manager.tasks.reports import fetch_report_by_id, prepare_report_metrics_data


@flow(name="metric-reports", description="Generate metric reports")  # type: ignore
async def deliver_metric_reports(tenant_id: int, report_id: int):
    """
    Deliver metric reports
    Args:
        tenant_id: The tenant ID for which to generate reports
        report_id: The report ID for which to generate reports
        Report contains the metric_ids for which the reports need to be generated
        Also details about the recipients of the report
    """
    tenant_id = int(tenant_id)
    logger = get_run_logger()
    # Set tenant context
    set_tenant_id(tenant_id)

    try:
        logger.info("Generating metric reports for report_id %s for tenant %s", report_id, tenant_id)

        # Fetch report configuration
        report = await fetch_report_by_id(report_id)  # type: ignore
        if not report:
            raise ValueError(f"Report {report_id} not found")

        # Extract report configuration
        metric_ids = report["config"]["metric_ids"]
        comparisons = report["config"].get("comparisons")
        notification_channels = report["notification_channels"]
        grain = Granularity(report["grain"])

        # Fetch metrics data
        data = await prepare_report_metrics_data(  # type: ignore
            tenant_id=tenant_id, metric_ids=metric_ids, grain=grain, comparisons=comparisons
        )

        # Deliver notifications
        delivery_result = await deliver_notifications(  # type: ignore
            tenant_id=tenant_id,
            notification_channels=notification_channels,
            context={"data": data, "config": report},
        )

        # Prepare run info
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

        # Record execution
        report_meta = {
            "metric_ids": metric_ids,
            "comparisons": comparisons,
            "report_name": report["name"],
            "grain": grain.value,
            "data_summary": {
                "metrics_with_data": [
                    metric["metric_id"] for metric in data["metrics"] if metric.get("current_value") is not None
                ],
                "metrics_without_data": [
                    metric["metric_id"] for metric in data["metrics"] if metric.get("current_value") is None
                ],
                "fetched_at": data["fetched_at"],
                "has_comparisons": data["has_comparisons"],
            },
        }

        execution = await record_notification_execution(  # type: ignore
            tenant_id=tenant_id,
            notification_type="REPORT",
            notification_id=report_id,
            execution_result=delivery_result,
            metadata=report_meta,
            run_info=run_info,
        )

        # Create execution summary artifact with more details
        summary = f"""
        # Report Execution Summary
        - Report: {report['name']} (ID: {report_id})
        - Execution ID: {execution.get('id')}
        - Tenant: {tenant_id}
        - Status: {delivery_result.get('status', 'Unknown')}
        - Metrics: {', '.join(str(mid) for mid in metric_ids)}
        - Granularity: {grain.value if grain else 'Unknown'}
        - Success Rate: {delivery_result.get('success_count', 0)}/{delivery_result.get('total_count', 0)} deliveries

        ## Data Summary
        - Total Metrics: {len(data.get("metrics", []))}
        - Metrics with Data: {len(report_meta.get('data_summary', {}).get('metrics_with_data', []))}
        - Metrics without Data: {len(report_meta.get('data_summary', {}).get('metrics_without_data', []))}
        - Fetched At: {data.get("fetched_at", "Unknown")}
        - Has Comparisons: {data.get("has_comparisons", False)}

        ## Delivery Details
        {format_delivery_results(delivery_result.get('channel_results', []))}

        ## Run Info
        - Flow Run ID: {run_info.get('flow_run_id', 'Not available')}
        - Flow Run Name: {run_info.get('flow_run_name', 'Not available')}
        - Deployment ID: {run_info.get('deployment_id', 'Not available')}
        - Start Time: {run_info.get('start_time', 'Not available')}
        """
        await create_markdown_artifact(
            key=f"report-{report_id}-execution-{execution.get('id')}-summary",
            markdown=summary,
            description=f"Execution summary for report {report_id}",
        )

        logger.info("Generated metric reports for report_id %s for tenant %s", report_id, tenant_id)
        return delivery_result

    except Exception as e:
        logger.exception("Failed to generate metric reports, error: %s", str(e))
        raise e
    finally:
        reset_context()
