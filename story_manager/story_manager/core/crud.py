from commons.db.crud import CRUDBase
from story_manager.core.models import Story


class CRUDStory(CRUDBase[Story, Story, Story]):
    """
    CRUD for Story Model.
    """
