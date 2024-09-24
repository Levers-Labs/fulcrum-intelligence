from commons.db.crud import CRUDBase
from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.core.filters import HeuristicExpressionFilter, StoryFilter
from story_manager.core.models import HeuristicExpression, Story


class CRUDStory(CRUDBase[Story, Story, Story, StoryFilter]):
    """
    CRUD for Story Model.
    """

    filter_class = StoryFilter


class CRUDHeuristic(CRUDBase[HeuristicExpression, HeuristicExpression, HeuristicExpression, HeuristicExpressionFilter]):
    """
    CRUD for HeuristicExpression Model.
    """

    filter_class = HeuristicExpressionFilter

    async def get_heuristic_expression(self, story_type: StoryType, grain: Granularity) -> str | None:
        statement = self.get_select_query().filter_by(story_type=story_type, grain=grain)
        results = await self.session.execute(statement=statement)
        instance: HeuristicExpression | None = results.unique().scalar_one_or_none()

        if instance is None:
            return None
        return instance.expression
