from commons.db.filters import BaseFilter, FilterField
from query_manager.core.models import Dimension


class DimensionFilter(BaseFilter[Dimension]):
    dimension_ids: list[str] | None = FilterField(Dimension, operator="in", default=None)  # type: ignore
