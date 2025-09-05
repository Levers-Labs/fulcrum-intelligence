"""Utility functions for asset manager."""

from sqlmodel.ext.asyncio.session import AsyncSession

from asset_manager.partitions import MetricContext
from asset_manager.resources.config import AppConfigResource
from asset_manager.resources.db import DbResource
from asset_manager.services.auth import get_client_auth
from commons.clients.insight_backend import InsightBackendClient
from commons.utilities.context import reset_context, set_tenant_id
from query_manager.core.crud import CRUDMetric
from query_manager.core.models import Metric
from query_manager.core.schemas import MetricDetail


async def get_tenant_identifier(config: AppConfigResource) -> str | None:
    """Resolve tenant identifier for naming Snowflake tables."""
    auth = get_client_auth(config)
    s = config.settings
    insights = InsightBackendClient(base_url=s.insights_backend_server_host, auth=auth)
    details = await insights.get_tenant_details()
    return details.get("identifier")


async def get_tenant_id_by_identifier(config: AppConfigResource, identifier: str) -> int:
    """Return tenant id for current tenant by identifier."""
    auth = get_client_auth(config)
    s = config.settings
    insights = InsightBackendClient(base_url=s.insights_backend_server_host, auth=auth)
    tenants = await insights.get_tenants(identifier=identifier)
    result = tenants.get("results")
    if not result:
        raise ValueError(f"Tenant with identifier {identifier} not found")
    return int(result[0]["id"])


async def get_metric(metric_id: str, session: AsyncSession) -> MetricDetail:
    """Fetch metric metadata from Query Manager DB."""
    crud = CRUDMetric(Metric, session)
    instance = await crud.get_by_metric_id(metric_id)
    return MetricDetail.model_validate(instance)


async def discover_metric_contexts(config: AppConfigResource, db: DbResource) -> list[MetricContext]:
    """
    Discover active metric contexts for dynamic partitioning.
    Returns a list of MetricContexts for active tenants and metrics
    """
    auth = get_client_auth(config)
    s = config.settings
    insights = InsightBackendClient(base_url=s.insights_backend_server_host, auth=auth)

    # Get all active tenants
    tenants_resp = await insights.get_tenants()  # Get all tenants for semantic sync
    tenants = tenants_resp.get("results", tenants_resp)  # Support both formats

    metric_contexts: list[MetricContext] = []

    for tenant_data in tenants:
        tenant_id = tenant_data["id"]
        tenant_identifier = str(tenant_data["identifier"])

        # Set tenant context for database operations
        set_tenant_id(tenant_id)
        try:
            async with db.session() as session:
                # Get all active metrics for this tenant
                crud = CRUDMetric(Metric, session)
                metric_ids = await crud.get_all_metric_ids()
                metric_contexts.extend(
                    [MetricContext(tenant=tenant_identifier, metric=metric_id) for metric_id in metric_ids]
                )
        finally:
            # Always reset context after processing each tenant
            reset_context()

    return metric_contexts
