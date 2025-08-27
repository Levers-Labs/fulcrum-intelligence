"""Service functions for Snowflake cache sync, independent of Prefect.

These mirror the logic from the legacy orchestration but are implemented here
without importing from tasks_manager. They rely on commons and query_manager
libraries available in this monorepo.
"""

import logging
from datetime import date, timedelta
from typing import Any

from sqlmodel.ext.asyncio.session import AsyncSession

from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.auth import get_client_auth
from commons.clients.insight_backend import InsightBackendClient
from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from query_manager.core.crud import CRUDMetricCacheConfig, CRUDMetricCacheGrainConfig
from query_manager.core.models import MetricCacheConfig, MetricCacheGrainConfig
from query_manager.semantic_manager.cache_manager import SnowflakeSemanticCacheManager
from query_manager.semantic_manager.models import SyncOperation, SyncType

logger = logging.getLogger(__name__)

DATE_FORMAT = "%Y-%m-%d"


async def get_enabled_grains_for_tenant(session: AsyncSession) -> list[MetricCacheGrainConfig]:
    """Return sync enabled cache grains for current tenant."""
    crud = CRUDMetricCacheGrainConfig(MetricCacheGrainConfig, session)
    return await crud.get_enabled_grains()


async def get_enabled_grain_config(session: AsyncSession, grain: Granularity) -> MetricCacheGrainConfig:
    """Return sync enabled cache grain config for current tenant."""
    crud = CRUDMetricCacheGrainConfig(MetricCacheGrainConfig, session)
    return await crud.get_by_grain(grain)


async def get_enabled_metrics_for_tenant(session: AsyncSession) -> list[MetricCacheConfig]:
    """Return sync enabled cache metrics for current tenant."""
    crud = CRUDMetricCacheConfig(MetricCacheConfig, session)
    return await crud.get_enabled_metrics()


def calculate_snowflake_grain_lookback(sync_type: SyncType, grain_config: MetricCacheGrainConfig) -> dict[str, int]:
    """Calculate lookback window based on sync type and grain configuration."""
    return {"days": grain_config.initial_sync_period if sync_type == SyncType.FULL else grain_config.delta_sync_period}


async def determine_sync_type(session: AsyncSession, metric_id: str, grain: Granularity) -> SyncType:
    """Determine sync type using existing sync status records."""
    mgr = SnowflakeSemanticCacheManager(session, None, None)
    return await mgr.metric_sync_status.determine_sync_type(
        metric_id=metric_id,
        grain=grain,
        sync_operation=SyncOperation.SNOWFLAKE_CACHE,
    )


async def compute_date_window(
    session: AsyncSession, metric_id: str, grain: Granularity
) -> tuple[SyncType, date, date, dict[str, Any]]:
    sync_type = await determine_sync_type(session, metric_id, grain)
    grain_cfg = await get_enabled_grain_config(session, grain)
    lookback = calculate_snowflake_grain_lookback(sync_type, grain_cfg)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(**lookback)
    return sync_type, start_date, end_date, {"grain_config": grain_cfg, "lookback": lookback}


async def get_tenant_partition_sets(
    config: AppConfigResource, db: DbResource
) -> tuple[list[str], dict[str, list[str]], dict[str, list[str]]]:
    """Compute tenant-specific metric and grain sets.

    Returns:
        tenant_keys: list[str]
        tenant_metrics_map: dict[tenant_key(str), list[metric_id]]
        tenant_grains_map: dict[tenant_key(str), list[grain(str)]]
    """
    auth = get_client_auth(config)
    s = config.settings
    insights = InsightBackendClient(base_url=s.insights_backend_server_host, auth=auth)

    tenants_resp = await insights.get_tenants(enable_metric_cache=True)  # type: ignore[arg-type]
    tenants = tenants_resp.get("results", tenants_resp)  # support both formats
    tenant_map = {str(t["identifier"]): t["id"] for t in tenants}
    tenant_keys = list(tenant_map.keys())

    tenant_metrics_map: dict[str, list[str]] = {}
    tenant_grains_map: dict[str, list[str]] = {}

    for tenant_key in tenant_keys:
        tenant_id = tenant_map[tenant_key]
        set_tenant_id(tenant_id)
        try:
            async with db.session() as session:
                metrics_cfg = await get_enabled_metrics_for_tenant(session)
                grains_cfg = await get_enabled_grains_for_tenant(session)
        finally:
            reset_context()

        tenant_metrics_map[tenant_key] = [cfg.metric_id for cfg in metrics_cfg]
        tenant_grains_map[tenant_key] = [cfg.grain.value for cfg in grains_cfg]

    return tenant_keys, tenant_metrics_map, tenant_grains_map
