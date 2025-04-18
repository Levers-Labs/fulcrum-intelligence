from commons.db.filters import BaseFilter, FilterField
from query_manager.core.models import Dimension, Metric


class DimensionFilter(BaseFilter[Dimension]):
    dimension_ids: list[str] | None = FilterField(Dimension.dimension_id, operator="in", default=None)  # type: ignore
    dimension_label: str | None = FilterField(Dimension.label, operator="ilike", default=None)  # type: ignore


class MetricFilter(BaseFilter[Metric]):
    metric_ids: list[str] | None = FilterField(Metric.metric_id, operator="in", default=None)  # type: ignore
    metric_label: str | None = FilterField(Metric.label, operator="ilike", default=None)  # type: ignore
