"""Dagster assets for semantic extraction and Snowflake cache loading.

Assets:
- metric_semantic_values: Extracts metric values for a tenant/metric/grain partition
- snowflake_metric_cache: Loads extracted values into Snowflake cache tables

Both assets include structured logging and output metadata for observability.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import pandas as pd
from dagster import (
    Backoff,
    Jitter,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)
from pytz import utc

from asset_manager.partitions import cache_tenant_grain_metric_partition, parse_tenant_grain_metric_key
from asset_manager.resources import AppConfigResource, DbResource, SnowflakeResource
from asset_manager.services.semantic_loader import fetch_metric_values
from asset_manager.services.snowflake_sync_service import SyncType, compute_date_window, record_sync_status
from asset_manager.services.utils import get_metric, get_tenant_id_by_identifier
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from query_manager.semantic_manager.cache_manager import SnowflakeSemanticCacheManager
from query_manager.semantic_manager.models import SyncOperation, SyncStatus

logger = logging.getLogger(__name__)


@asset(
    name="metric_semantic_values",
    description="Raw metric values from cube for a tenant/grain/metric partition",
    partitions_def=cache_tenant_grain_metric_partition,
    group_name="semantic_extraction",
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=2.0,  # 2 seconds base delay
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
async def metric_semantic_values(context, app_config: AppConfigResource, db: DbResource) -> pd.DataFrame:
    """Extract metric time series values for the current partition.

    Emits metadata including date window, sync type, and preview rows.
    """

    key = context.partition_key
    tenant_identifier, grain_value, metric_id = parse_tenant_grain_metric_key(key)
    # Get tenant id to set tenant id context
    tenant_id = await get_tenant_id_by_identifier(config=app_config, identifier=tenant_identifier)
    context.log.info("Tenant ID: %s", tenant_id)
    grain = Granularity(grain_value)

    context.log.info("Extracting values: tenant=%s metric=%s grain=%s", tenant_identifier, metric_id, grain.value)
    # set tenant id context
    set_tenant_id(tenant_id)

    sync_type = None
    start_date = None
    end_date = None

    try:
        # compute date window using DB-backed helpers
        async with db.session() as session:
            metric = await get_metric(metric_id, session)
            sync_type, start_date, end_date, _extra = await compute_date_window(session, metric_id, grain)
        context.log.info("Sync type: %s, Sync Period: %s - %s", sync_type, start_date, end_date)
        # fetch metric values
        async with db.session() as session:
            values, stats = await fetch_metric_values(metric, start_date, end_date, grain, app_config)
        context.log.info("Data Extracted, stats: %s", stats)
        # convert to dataframe and attach window metadata for downstream asset
        df = pd.DataFrame(values or [])
        df.attrs["sync_type"] = sync_type.value
        df.attrs["start_date"] = start_date.isoformat()
        df.attrs["end_date"] = end_date.isoformat()

        context.add_output_metadata(
            {
                "num_rows": MetadataValue.int(len(df)),
                "tenant": MetadataValue.text(tenant_identifier),
                "metric": MetadataValue.text(metric_id),
                "grain": MetadataValue.text(grain.value),
                "sync_type": MetadataValue.text(sync_type.value),
                "start_date": MetadataValue.text(start_date.isoformat()),
                "end_date": MetadataValue.text(end_date.isoformat()),
                "date_window": MetadataValue.text(f"{start_date.isoformat()} to {end_date.isoformat()}"),
                "stats": MetadataValue.json(stats),
                "columns": MetadataValue.text(str(list(df.columns))),
                "preview": (
                    MetadataValue.md(df.head().to_markdown(index=False)) if not df.empty else MetadataValue.text("")
                ),
            }
        )

        return df
    except Exception as e:
        error_message = f"Failed to extract semantic values: {str(e)}"
        context.log.error(error_message, exc_info=True)

        # Record sync failure if we have the necessary info
        if sync_type and start_date and end_date:
            await record_sync_status(
                db=db,
                tenant_id=tenant_id,
                metric_id=metric_id,
                grain=grain,
                sync_operation=SyncOperation.SNOWFLAKE_CACHE,
                sync_status=SyncStatus.FAILED,
                sync_type=sync_type,
                start_date=start_date,
                end_date=end_date,
                error_message=error_message,
            )

        # Re-raise the exception to fail the asset
        raise
    finally:
        reset_context()


@asset(
    name="snowflake_metric_cache",
    description="Load semantic metric values into Snowflake cache tables per partition",
    partitions_def=cache_tenant_grain_metric_partition,
    group_name="semantic_loader",
    deps=[metric_semantic_values],
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=5.0,  # 5 seconds base delay
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
)
async def snowflake_metric_cache(
    context,
    app_config: AppConfigResource,
    snowflake: SnowflakeResource,
    db: DbResource,
    metric_semantic_values: pd.DataFrame,
) -> MaterializeResult:
    """Load metric values into Snowflake cache for the current partition.

    Emits metadata including rows loaded and target table.
    """

    key = context.partition_key
    tenant_identifier, grain_value, metric_id = parse_tenant_grain_metric_key(key)
    # Get tenant id to set tenant id context
    tenant_id = await get_tenant_id_by_identifier(app_config, tenant_identifier)
    grain = Granularity(grain_value)
    context.log.info("Loading values: tenant=%s metric=%s grain=%s", tenant_identifier, metric_id, grain.value)
    rows = metric_semantic_values.to_dict(orient="records")
    if not rows:
        context.log.warning("No rows to load: tenant=%s metric=%s grain=%s", tenant_identifier, metric_id, grain.value)
        return MaterializeResult(
            metadata={
                "rows_loaded": MetadataValue.int(0),
                "tenant": MetadataValue.text(tenant_identifier),
                "metric": MetadataValue.text(metric_id),
                "grain": MetadataValue.text(grain.value),
            }
        )
    # Set tenant id context
    set_tenant_id(tenant_id)
    try:
        # Prefer window from upstream dataframe attrs; fallback to recompute
        attrs = metric_semantic_values.attrs
        sync_type_str = attrs.get("sync_type")
        start_date_str = attrs.get("start_date")
        end_date_str = attrs.get("end_date")

        if sync_type_str and start_date_str and end_date_str:
            sync_type = SyncType(sync_type_str)
            start_date = date.fromisoformat(start_date_str)
            end_date = date.fromisoformat(end_date_str)
            context.log.info("Using date window from upstream attrs: %s, %s - %s", sync_type, start_date, end_date)
        else:
            async with db.session() as session:
                sync_type, start_date, end_date, _extra = await compute_date_window(session, metric_id, grain)
            context.log.info("Upstream attrs missing; recomputed window: %s, %s - %s", sync_type, start_date, end_date)

        async with db.session() as session:
            sf_client = await snowflake.get_client()
            cache_mgr = SnowflakeSemanticCacheManager(session, sf_client, tenant_identifier)
            result = await cache_mgr.cache_metric_time_series(
                metric_id=metric_id,
                grain=grain,
                sync_type=sync_type,
                start_date=start_date,
                end_date=end_date,
                values=rows,  # type: ignore[arg-type]
            )
    finally:
        reset_context()

    table_fqn = result.get("table_name")
    cache_size_mb = result.get("cache_size_mb")
    status = result.get("status")
    stats = result.get("time_series_stats") or {}
    rows_loaded = int(stats["processed"]) if "processed" in stats else len(rows)

    return MaterializeResult(
        metadata={
            "load_timestamp": MetadataValue.timestamp(datetime.now(tz=utc)),
            "date_window": MetadataValue.text(f"{start_date.isoformat()} to {end_date.isoformat()}"),
            "rows_loaded": MetadataValue.int(rows_loaded),
            "cache_size_mb": MetadataValue.float(cache_size_mb or 0.0),
            "status": MetadataValue.text(status or "unknown"),
            "table_fqn": MetadataValue.text(table_fqn or "unknown"),
            "tenant": MetadataValue.text(tenant_identifier),
            "metric": MetadataValue.text(metric_id),
            "grain": MetadataValue.text(grain.value),
            "start_date": MetadataValue.text(start_date.isoformat()),
            "end_date": MetadataValue.text(end_date.isoformat()),
            "sync_type": MetadataValue.text(sync_type.value),
        }
    )
