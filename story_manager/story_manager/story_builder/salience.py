import logging
from typing import Any

from jinja2 import Template

from commons.models.enums import Granularity
from story_manager.core.dependencies import CRUDStoryConfigDep
from story_manager.core.enums import StoryType
from story_manager.core.models import StoryConfig
from story_manager.db.config import get_async_session

logger = logging.getLogger(__name__)


class SalienceEvaluator:
    """
    A class to evaluate the salience of a story based on its type, granularity, and provided variables.
    """

    def __init__(self, story_type: StoryType, grain: Granularity, variables: dict[str, Any]):
        """
        Initialize the SalienceEvaluator with the given story type, granularity, and variables.

        :param story_type: The type of the story.
        :param grain: The granularity of the story.
        :param variables: A dictionary of variables to be used in the heuristic heuristic_expression.
        """
        self.story_type = story_type
        self.grain = grain
        self.variables = variables

    def render_expression(self, expression: str) -> str:
        """
        Render the heuristic heuristic_expression using the provided variables.

        :param expression: The heuristic heuristic_expression template as a string.
        :return: The rendered heuristic_expression as a string.
        """
        template = Template(expression)
        return template.render(self.variables)

    @staticmethod
    def evaluate_expression(expression: str) -> bool:
        """
        Evaluate the rendered heuristic heuristic_expression.

        :param expression: The rendered heuristic heuristic_expression as a string.
        :return: The result of the evaluation as a boolean.
        """
        try:
            return eval(expression)  # noqa
        except Exception as ex:
            logger.error(f"Error evaluating heuristic_expression: {ex}")
            return False

    async def evaluate_salience(self):
        """
        Evaluate the salience of the story by fetching the heuristic expression from the database,
        rendering it with the provided variables, and evaluating the rendered expression.

        :return: The result of the salience evaluation as a boolean.
        """
        async for session in get_async_session():
            try:
                # Get the CRUDStoryConfig dependency
                story_config_crud = CRUDStoryConfigDep(StoryConfig, session)
                # Fetch the heuristic expression template from the database
                expression_template = await story_config_crud.get_heuristic_expression(
                    story_type=self.story_type, grain=self.grain
                )
                # If no expression template is found, return True
                if not expression_template:
                    return True
                # Render the expression template with the provided variables
                rendered_expression = self.render_expression(expression_template)
                # Evaluate the rendered expression
                return self.evaluate_expression(rendered_expression)
            finally:
                await session.close()
