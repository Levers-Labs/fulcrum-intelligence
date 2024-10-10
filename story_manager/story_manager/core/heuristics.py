import logging
from datetime import datetime
from typing import Any

from jinja2 import Template
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.models.enums import Granularity
from story_manager.core.dependencies import CRUDStoryConfigDep, CRUDStoryDep
from story_manager.core.enums import StoryType
from story_manager.core.models import Story, StoryConfig
from story_manager.story_builder.utils import calculate_periods_count

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
        self._story_config = None
        self.story_crud = CRUDStoryDep(Story, self.session)

    @property
    async def story_config(self) -> StoryConfig | None:
        """
        Asynchronously retrieve the story configuration for the given story type and granularity.

        This property fetches the story configuration from the database if it has not been
        previously retrieved and cached. The configuration is then cached for future use.

        :return: The story configuration or None if not found.
        """
        # Check if the story configuration has already been retrieved and cached
        if self._story_config is None:
            # Fetch the story configuration from the database
            self._story_config = await self.story_config_crud.get_story_config(  # type: ignore
                story_type=self.story_type, grain=self.grain
            )
        # Return the cached story configuration
        return self._story_config

    async def evaluate(self, variables: dict[str, Any]) -> tuple[bool, bool, bool]:
        """
        Evaluate the story based on defined heuristics.

        This method evaluates the story's salience and cool-off period using the provided variables.
        It first checks if the story is salient using the heuristic expression, and then determines if the
        story is within the cool-off period.

        :param variables: A dictionary of variables to be used in the heuristic evaluation.
        :return: A tuple containing:
            - is_salient (bool): Indicates if the story is salient.
            - in_cool_off (bool): Indicates if the story is within the cool-off period.
            - is_heuristic (bool): Indicates if the story should be rendered based on heuristics.
        """

        # Check if the story is salient based on the heuristic expression
        is_salient = await self._evaluate_salience(variables)

        # Check if the story is within the cool-off period and if it should be rendered
        in_cool_off, is_heuristic = await self._evaluate_cool_off(is_salient)

        return is_salient, in_cool_off, is_heuristic

    async def _evaluate_salience(self, variables: dict[str, Any]) -> bool:
        """
        Determine if the story is salient based on the heuristic expression.

        This method fetches the heuristic expression from the story configuration and substitutes
        the provided variables into it. It then evaluates the resulting expression to decide if the
        story is salient.

        :param variables: A dictionary of variables to be used in the heuristic evaluation.
        :return: A boolean indicating if the story is salient.
        """
        # Retrieve the story configuration asynchronously
        story_config = await self.story_config

        # If no story configuration is found, return True indicating the story is salient
        if not story_config:
            return True

        # Fetch the heuristic expression from the story configuration
        expression_template = story_config.heuristic_expression  # type: ignore

        # If no heuristic expression is defined, return True indicating the story is salient
        if not expression_template:
            return True

        # Substitute the provided variables into the heuristic expression
        rendered_expression = self._render_expression(expression_template, variables)

        # Evaluate the resulting expression to determine if the story is salient
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

        This method checks if the story is within the cool-off period based on the latest heuristic story's date
        and the cool-off duration defined in the story configuration. It returns a tuple indicating whether the
        story is in the cool-off period and whether the story should be considered heuristic.

        :param is_salient: A boolean indicating if the story is salient.
        :return: A tuple (in_cool_off: bool, is_heuristic: bool).
        """
        # Retrieve the story configuration asynchronously
        story_config = await self.story_config

        # If the story is not salient or the story configuration is not available,
        # return False and the value of is_salient
        if not is_salient or not story_config:
            return False, is_salient

        # Get the cool-off duration from the story configuration
        cool_off_duration = story_config.cool_off_duration  # type: ignore

        # If no cool-off duration is defined, return False for in_cool_off and the value of is_salient for is_heuristic
        if cool_off_duration is None:
            return False, is_salient

        current_date = datetime.now()

        # Fetch the latest heuristic story before the current date
        latest_heuristic_story = await self.story_crud.get_latest_story(
            story_type=self.story_type, grain=self.grain, story_date=current_date, is_heuristic=True
        )

        # If no previous story is found, return False for in_cool_off and the value of is_salient for is_heuristic
        if not latest_heuristic_story:
            return False, is_salient

        # Calculate the number of periods between the current date and the latest heuristic story's date
        period_count = calculate_periods_count(current_date, latest_heuristic_story.story_date, self.grain)

        # Determine if the story is in the cool-off period
        in_cool_off = period_count < cool_off_duration

        # Determine if the story should be considered heuristic
        is_heuristic = is_salient and not in_cool_off

        return in_cool_off, is_heuristic
