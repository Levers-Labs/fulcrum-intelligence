from commons.db.crud import CRUDBase
from story_manager.core.filters import StoryFilter
from story_manager.core.models import Story


class CRUDStory(CRUDBase[Story, Story, Story, StoryFilter]):
    """
    CRUD for Story Model.
    """

    filter_class = StoryFilter
