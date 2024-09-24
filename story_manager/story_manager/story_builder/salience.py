import logging
from typing import Any

from jinja2 import Template

from commons.models.enums import Granularity
from story_manager.core.dependencies import CRUDHeuristicDep
from story_manager.core.enums import StoryType
from story_manager.core.models import HeuristicExpression
from story_manager.db.config import get_async_session

logger = logging.getLogger(__name__)


class SalienceEvaluator:

    def __init__(self, story_type: StoryType, grain: Granularity, variables: dict[str, Any]):

        self.story_type = story_type
        self.grain = grain
        self.variables = variables

    def render_expression(self, expression: str) -> str:
        template = Template(expression)
        return template.render(self.variables)

    @staticmethod
    def evaluate_expression(expression: str) -> bool:
        try:
            return eval(expression)  # noqa
        except Exception as ex:
            logger.error(f"Error evaluating expression: {ex}")
            return False

    async def evaluate_salience(self) -> bool:
        async for session in get_async_session():
            heuristic_crud = CRUDHeuristicDep(HeuristicExpression, session)
            expression_template = await heuristic_crud.get_heuristic_expression(
                story_type=self.story_type, grain=self.grain
            )
            if not expression_template:
                return True
            rendered_expression = self.render_expression(expression_template)
            return self.evaluate_expression(rendered_expression)
        return True
