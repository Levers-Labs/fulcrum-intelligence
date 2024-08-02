from commons.db.filters import BaseFilter, FilterField
from query_manager.core.models import Dimension, Metric


class DimensionFilter(BaseFilter[Dimension]):
    dimension_ids: list[str] | None = FilterField(Dimension, operator="in", default=None)  # type: ignore


class MetricFilter(BaseFilter[Metric]):
    metric_ids: list[str] | None = FilterField(Metric, operator="in", default=None)  # type: ignore
