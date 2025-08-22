import logging
from datetime import date, datetime

from asset_manager.resources.config import AppConfigResource
from asset_manager.services.auth import get_client_auth
from commons.clients.insight_backend import InsightBackendClient
from commons.models.enums import Granularity
from commons.utilities.context import get_tenant_id
from query_manager.core.dependencies import get_cube_client
from query_manager.core.schemas import MetricDetail

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%d"


def _process_raw_metric_data(
    raw_data: dict, metric: MetricDetail, grain: Granularity, dimension_id: str | None
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
    tenant_id = get_tenant_id()
    record = {
        "metric_id": metric.metric_id,
        "tenant_id": tenant_id,
        "date": date_value,
        "grain": grain,
        "value": float(raw_data["value"]),
    }

    if dimension_id:
        record["dimension_name"] = dimension_id
        record["dimension_slice"] = raw_data.get(dimension_id)

    return record


def sanitize_metric_data(
    data: list[dict], metric: MetricDetail, grain: Granularity, dimension_id: str | None
) -> tuple[list[dict], dict]:
    """
    Sanitize and process raw metric data, filtering out invalid records.

    Args:
        data: Raw data from cube client
        metric: Metric details
        grain: Granularity level
        dimension_id: Optional dimension identifier

    Returns:
        tuple: (processed_records, stats) where stats contains processing metadata
    """
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
                metric=metric,
                grain=grain,
                dimension_id=dimension_id,
            )
            records.append(record)
        except (ValueError, TypeError) as e:
            logger.warning("Failed to process record: %s. Error: %s", raw_data, str(e))
            skipped_records += 1

    logger.info("Processing complete - Valid records: %d, Skipped records: %d", len(records), skipped_records)
    return records, {"total": len(data), "skipped": skipped_records, "processed": len(records)}


async def fetch_metric_values(
    metric: MetricDetail,
    start_date: date,
    end_date: date,
    grain: Granularity,
    config: AppConfigResource,
    dimension_id: str | None = None,
) -> tuple[list[dict], dict]:
    """Fetch metric time series from Query Manager API with metadata/stats."""
    auth = get_client_auth(config)
    s = config.settings

    insights_client = InsightBackendClient(s.insights_backend_server_host, auth=auth)
    cube_client = await get_cube_client(insights_client)

    data = await cube_client.load_metric_values_from_cube(
        metric=metric,  # type: ignore
        grain=grain,
        start_date=start_date,
        end_date=end_date,
        dimensions=[dimension_id] if dimension_id else None,
    )

    # Process and transform the data
    return sanitize_metric_data(data, metric, grain, dimension_id)
