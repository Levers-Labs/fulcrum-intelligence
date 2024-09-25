from commons.db.crud import CRUDBase
from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.core.filters import StoryConfigFilter, StoryFilter
from story_manager.core.models import Story, StoryConfig


class CRUDStory(CRUDBase[Story, Story, Story, StoryFilter]):
    """
    CRUD for Story Model.
    """

    filter_class = StoryFilter


class CRUDStoryConfig(CRUDBase[StoryConfig, StoryConfig, StoryConfig, StoryConfigFilter]):
    """
    CRUD for StoryConfig Model.
    """

    filter_class = StoryConfigFilter

    async def get_heuristic_expression(self, story_type: StoryType, grain: Granularity) -> str | None:
        statement = self.get_select_query().filter_by(story_type=story_type, grain=grain)
        results = await self.session.execute(statement=statement)
        instance: StoryConfig | None = results.unique().scalar_one_or_none()

        if instance is None:
            return None
        return instance.heuristic_expression
