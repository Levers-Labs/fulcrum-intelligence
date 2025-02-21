from prefect import flow, get_run_logger

from commons.utilities.context import set_tenant_id


@flow(name="metric-reports", description="Generate metric reports")  # type: ignore
def deliver_metric_reports(tenant_id: int, report_id: int):
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
    set_tenant_id(tenant_id)
    logger.info("Generating metric reports for report_id %s for tenant %s", report_id, tenant_id)
    logger.info("Generated metric reports for report_id %s for tenant %s", report_id, tenant_id)
