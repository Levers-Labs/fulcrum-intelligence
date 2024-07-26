from commons.db.crud import CRUDBase
from query_manager.core.filters import DimensionFilter
from query_manager.core.models import Dimension


class CRUDDimensions(CRUDBase[Dimension, Dimension, Dimension, DimensionFilter]):  # noqa
    """
    CRUD for Dimension Model.
    """
