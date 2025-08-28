"""Utility functions for asset manager."""

from sqlmodel.ext.asyncio.session import AsyncSession

from asset_manager.resources.config import AppConfigResource
from asset_manager.services.auth import get_client_auth
from commons.clients.insight_backend import InsightBackendClient
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
