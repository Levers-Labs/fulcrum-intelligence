from analysis_manager.core.models import User
from commons.db.crud import CRUDBase


class CRUDUser(CRUDBase[User, User, User, None]):  # type: ignore
    """
    CRUD for User Model.
    """
