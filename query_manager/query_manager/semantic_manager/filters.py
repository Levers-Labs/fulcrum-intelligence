from datetime import date

from commons.db.filters import BaseFilter, FilterField
from commons.models.enums import Granularity
from query_manager.core.models import Metric
from query_manager.semantic_manager.models import MetricTarget


class TargetFilter(BaseFilter):
    """Filter for targets."""

    metric_ids: list[str] | None = FilterField(field=MetricTarget.metric_id, operator="in")  # type: ignore
    metric_label: str | None = FilterField(  # type: ignore
        field=Metric.label,  # type: ignore
        operator="ilike",
        join_model=Metric,
        join_condition=MetricTarget.metric_id == Metric.metric_id,
    )
    grain: Granularity | None = FilterField(field=MetricTarget.grain)  # type: ignore
    target_date: date | None = FilterField(field=MetricTarget.target_date)  # type: ignore
    target_date_ge: date | None = FilterField(field=MetricTarget.target_date, operator="ge")  # type: ignore
    target_date_le: date | None = FilterField(field=MetricTarget.target_date, operator="le")  # type: ignore
