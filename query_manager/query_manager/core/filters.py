from commons.db.filters import BaseFilter, FilterField
from query_manager.core.models import Dimensions


class DimensionFilter(BaseFilter[Dimensions]):
    dimension_ids: list[str] | None = FilterField(Dimensions, operator="in", default=None)  # type: ignore
