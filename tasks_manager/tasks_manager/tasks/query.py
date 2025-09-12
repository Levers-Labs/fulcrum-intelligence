import os
from datetime import date, datetime

from prefect import get_run_logger, task

from commons.clients.insight_backend import InsightBackendClient
from commons.clients.query_manager import QueryManagerClient
from commons.models.enums import Granularity
from query_manager.core.dependencies import get_cube_client
from query_manager.core.schemas import MetricDetail
from tasks_manager.config import AppConfig
from tasks_manager.utils import get_client_auth_from_config

DATE_FORMAT = "%Y-%m-%d"


@task(  # type: ignore
    name="get_metric",
    task_run_name="get_metric:metric={metric_id}",
    retries=3,
    retry_delay_seconds=30,
    timeout_seconds=120,  # 2 minutes
)
async def get_metric(metric_id: str) -> MetricDetail:
    """Fetch metric details along with dimensions"""
    logger = get_run_logger()
    logger.info("Fetching metric details for %s", metric_id)

    try:
        config = await AppConfig.load("default")
        auth = get_client_auth_from_config(config)
        query_manager = QueryManagerClient(config.query_manager_server_host, auth=auth)

        metric_details = await query_manager.get_metric(metric_id)
        logger.info("Successfully fetched details for metric %s", metric_id)
        return MetricDetail.model_validate(metric_details)
    except Exception as e:
        logger.error("Failed to fetch details for metric %s: %s", metric_id, str(e))
        raise


def _process_raw_metric_data(
    raw_data: dict, metric_id: str, tenant_id: int, grain: Granularity, dimension_id: str | None
) -> dict:
    """
    Process raw metric data into standardized format.

    Args:
        raw_data: Raw data from cube client
        metric_id: ID of the metric
        tenant_id: Tenant identifier
        grain: Granularity level
        dimension_id: Optional dimension identifier

    Returns:
        dict: Processed metric record

    Raises:
        ValueError: If date conversion fails
        TypeError: If value conversion fails
    """
    # Convert string date to date object if needed
    date_value = raw_data["date"]
    if isinstance(date_value, str):
        date_value = datetime.strptime(date_value, DATE_FORMAT).date()

    record = {
        "metric_id": metric_id,
        "tenant_id": tenant_id,
        "date": date_value,
        "grain": grain,
        "value": float(raw_data["value"]),
    }

    if dimension_id:
        record["dimension_name"] = dimension_id
        record["dimension_slice"] = raw_data.get(dimension_id)

    return record


async def fetch_metric_values(
    tenant_id: int,
    metric: MetricDetail,
    start_date: date,
    end_date: date,
    grain: Granularity,
    dimension_id: str | None = None,
) -> tuple[list[dict], dict]:
    """
    Fetch and process metric data from the query client.

    Returns:
        list[dict]: List of processed metric records with standardized format
        dict: Stats of the fetch operation

    Raises:
        ConfigError: If configuration loading fails
        ConnectionError: If unable to connect to cube client
    """
    logger = get_run_logger()
    logger.info(
        "Starting metric data fetch - Metric: %s, Tenant: %s, Date Range: %s to %s",
        metric.metric_id,
        tenant_id,
        start_date,
        end_date,
    )

    try:
        # Initialize clients and configuration
        config = await AppConfig.load("default")
        os.environ["SERVER_HOST"] = config.query_manager_server_host
        auth = get_client_auth_from_config(config)
        insights_client = InsightBackendClient(config.insights_backend_server_host, auth=auth)
        cube_client = await get_cube_client(insights_client)

        # Fetch raw data from cube
        logger.debug("Fetching data from cube with dimensions: %s", dimension_id or "None")
        data = await cube_client.load_metric_values_from_cube(
            metric=metric,  # type: ignore
            grain=grain,
            start_date=start_date,
            end_date=end_date,
            dimensions=[dimension_id] if dimension_id else None,
        )
        logger.info(
            "Retrieved %d raw values for metric %s (grain: %s, dimension: %s)",
            len(data),
            metric.metric_id,
            grain,
            dimension_id or "None",
        )

        # Process and transform the data
        records = []
        skipped_records = 0

        for raw_data in data:
            if raw_data.get("value") is None:
                skipped_records += 1
                continue
            # skip if dimension value is None
            if dimension_id and raw_data.get(dimension_id) is None:
                skipped_records += 1
                continue

            try:
                record = _process_raw_metric_data(
                    raw_data=raw_data,
                    metric_id=metric.metric_id,
                    tenant_id=tenant_id,
                    grain=grain,
                    dimension_id=dimension_id,
                )
                records.append(record)
            except (ValueError, TypeError) as e:
                logger.warning("Failed to process record: %s. Error: %s", raw_data, str(e))
                skipped_records += 1

        logger.info("Processing complete - Valid records: %d, Skipped records: %d", len(records), skipped_records)
        return records, {"total": len(data), "skipped": skipped_records, "processed": len(records)}

    except Exception as e:
        logger.error("Failed to fetch metric values - Metric: %s, Error: %s", metric.metric_id, str(e))
        raise
