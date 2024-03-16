from analysis_manager.core.models import User
from analysis_manager.utilities.crud import CRUDBase


class CRUDUser(CRUDBase[User, User, User]):
    """
    CRUD for User Model.
    """
