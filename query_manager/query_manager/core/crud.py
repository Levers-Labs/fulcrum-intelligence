from commons.db.crud import CRUDBase
from query_manager.core.filters import DimensionFilter
from query_manager.core.models import Dimensions


class CRUDDimensions(CRUDBase[Dimensions, Dimensions, Dimensions, DimensionFilter]):  # noqa
    """
    CRUD for Dimension Model.
    """
