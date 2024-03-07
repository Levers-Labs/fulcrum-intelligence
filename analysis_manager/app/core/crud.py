from app.core.models import User
from app.utilities.crud import CRUDBase


class CRUDUser(CRUDBase[User, User, User]):
    """
    CRUD for User Model.
    """
