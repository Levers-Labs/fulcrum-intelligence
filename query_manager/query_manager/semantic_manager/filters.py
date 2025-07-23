"""Filter classes for semantic manager models."""

from datetime import date

from commons.db.filters import BaseFilter, FilterField
from commons.models.enums import Granularity
from query_manager.semantic_manager.models import MetricTarget, TenantSyncStatus


class TargetFilter(BaseFilter[MetricTarget]):
    """Filter for metric targets."""

    metric_ids: list[str] | None = FilterField(field=MetricTarget.metric_id, operator="in")  # type: ignore
    grain: Granularity | None = FilterField(field=MetricTarget.grain)  # type: ignore
    target_date: date | None = FilterField(field=MetricTarget.target_date)  # type: ignore
    target_date_ge: date | None = FilterField(field=MetricTarget.target_date, operator="ge")  # type: ignore
    target_date_le: date | None = FilterField(field=MetricTarget.target_date, operator="le")  # type: ignore


class TenantSyncStatusFilter(BaseFilter[TenantSyncStatus]):
    """Filter for tenant sync status."""

    sync_operation: str | None = FilterField(TenantSyncStatus.sync_operation, operator="eq", default=None)  # type: ignore
    grain: str | None = FilterField(TenantSyncStatus.grain, operator="eq", default=None)  # type: ignore
    sync_status: str | None = FilterField(TenantSyncStatus.sync_status, operator="eq", default=None)  # type: ignore
