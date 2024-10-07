import logging
from datetime import datetime
from typing import Any

from jinja2 import Template
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.models.enums import Granularity
from story_manager.core.dependencies import CRUDStoryConfigDep, CRUDStoryDep
from story_manager.core.enums import StoryType
from story_manager.core.models import Story, StoryConfig

logger = logging.getLogger(__name__)


class StoryHeuristicEvaluator:
    """
    A class to evaluate stories using heuristics, including salience and cool-off period,
    based on the story's type, granularity, and provided variables.

    Attributes:
        story_type (StoryType): The type of the story.
        grain (Granularity): The granularity of the story.
        session (AsyncSession): The database session used for fetching configurations and stories.
        story_config_crud (CRUDStoryConfigDep): CRUD operations for story configurations.
        story_crud (CRUDStoryDep): CRUD operations for stories.
    """

    def __init__(self, story_type: StoryType, grain: Granularity, session: AsyncSession):
        self.story_type = story_type
        self.grain = grain
        self.session = session
        self.story_config_crud = CRUDStoryConfigDep(StoryConfig, self.session)
        self.story_crud = CRUDStoryDep(Story, self.session)

    async def evaluate(self, variables: dict[str, Any]) -> tuple[bool, bool, bool]:
        """
        Evaluate the story based on defined heuristics.

        This method evaluates the story's salience and cool-off period based on the provided variables.
        It first checks if the story is salient using the heuristic expression, and then determines if the
        story is within the cool-off period.

        :param variables: A dictionary of variables to be used in the heuristic evaluation.
        :return: A tuple of (is_salient: bool, in_cool_off: bool, render_story: bool).
        """
        # Evaluate if the story is salient based on the heuristic expression
        is_salient = await self._evaluate_salience(variables)

        # Evaluate if the story is within the cool-off period and if it should be rendered
        in_cool_off, render_story = await self._evaluate_cool_off(is_salient)

        return is_salient, in_cool_off, render_story

    async def _evaluate_salience(self, variables: dict[str, Any]) -> bool:
        """
        Evaluate the salience of the story based on the heuristic expression.

        This method retrieves the heuristic expression from the story configuration and renders it
        with the provided variables. It then evaluates the rendered expression to determine if the
        story is salient.

        :param variables: A dictionary of variables to be used in the heuristic evaluation.
        :return: A boolean indicating if the story is salient.
        """
        # Retrieve the heuristic expression template from the story configuration
        expression_template, _ = await self.story_config_crud.get_story_config(
            story_type=self.story_type, grain=self.grain
        )

        # If no heuristic expression is defined, consider the story as salient
        if not expression_template:
            return True

        # Render the heuristic expression with the provided variables
        rendered_expression = self._render_expression(expression_template, variables)

        # Evaluate the rendered expression to determine if the story is salient
        return self._evaluate_expression(rendered_expression)

    @staticmethod
    def _render_expression(expression: str, variables: dict[str, Any]) -> str:
        """
        Render a Jinja2 template expression with the provided variables.

        :param expression: The Jinja2 template expression to be rendered.
        :param variables: A dictionary of variables to be used in the rendering process.
        :return: The rendered expression as a string.
        """
        # Create a Jinja2 template from the expression
        template = Template(expression)
        # Render the template with the provided variables
        return template.render(variables)

    @staticmethod
    def _evaluate_expression(expression: str) -> bool:
        """
        Evaluate a rendered expression and return the result as a boolean.

        :param expression: The rendered expression to be evaluated.
        :return: The result of the evaluation as a boolean.
        """
        try:
            # Use eval to evaluate the expression and return the result
            return eval(expression)  # noqa
        except Exception as ex:
            # Log an error message if the evaluation fails
            logger.error(f"Error evaluating heuristic_expression: {ex}")
            # Return False if an exception occurs during evaluation
            return False

    async def _evaluate_cool_off(self, is_salient: bool) -> tuple[bool, bool]:
        """
        Evaluate the cool-off period for the story.

        This method checks if the story is within the cool-off period based on the last rendered story's date
        and the cool-off duration defined in the story configuration. It returns a tuple indicating whether the
        story is in the cool-off period and whether the story should be rendered.

        :param is_salient: A boolean indicating if the story is salient.
        :return: A tuple (in_cool_off: bool, render_story: bool).
        """
        # Retrieve the cool-off duration from the story configuration
        _, cool_off_duration = await self.story_config_crud.get_story_config(
            story_type=self.story_type, grain=self.grain
        )

        # If no cool-off duration is defined, return False for in_cool_off and the value of is_salient for render_story
        if cool_off_duration is None:
            return False, is_salient

        current_date = datetime.now()

        # Fetch the last rendered story before the current date
        last_rendered_story = await self.story_crud.get_last_rendered_story(self.story_type, self.grain, current_date)

        # If no previous story is found, return False for in_cool_off and the value of is_salient for render_story
        if not last_rendered_story:
            return False, is_salient

        # Calculate the time difference between the current date and the last rendered story's date
        time_since_last_render = self._calculate_time_difference(current_date, last_rendered_story.story_date)

        # Determine if the story is in the cool-off period
        in_cool_off = time_since_last_render < cool_off_duration

        # Determine if the story should be rendered
        render_story = is_salient and not in_cool_off

        return in_cool_off, render_story

    def _calculate_time_difference(self, current_date: datetime, last_render_date: datetime) -> int:
        """
        Calculate the time difference between the current date and the last render date based on the grain.

        :param current_date: The current date.
        :param last_render_date: The date of the last rendered story.
        :return: The time difference in the appropriate units (days, weeks, or months).
        """
        if self.grain == Granularity.DAY:
            # Calculate the difference in days
            return (current_date.date() - last_render_date.date()).days
        elif self.grain == Granularity.WEEK:
            # Calculate the difference in weeks
            return (current_date.date() - last_render_date.date()).days // 7
        elif self.grain == Granularity.MONTH:
            # Calculate the difference in months
            return (current_date.year - last_render_date.year) * 12 + current_date.month - last_render_date.month

        return 0
