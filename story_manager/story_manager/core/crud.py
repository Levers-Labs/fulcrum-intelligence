from datetime import datetime
from typing import Any

from sqlalchemy import desc, func

from commons.db.crud import CRUDBase
from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.core.filters import StoryConfigFilter, StoryFilter
from story_manager.core.models import Story, StoryConfig


class CRUDStory(CRUDBase[Story, Story, Story, StoryFilter]):
    """
    CRUD for Story Model.
    Provides methods to interact with the Story table in the database.
    """

    filter_class = StoryFilter

    async def get_last_rendered_story(
        self,
        story_type: StoryType,
        grain: Granularity,
        current_date: datetime,
    ) -> Any:
        """
        Retrieve the last rendered story of a specific type and granularity before the current date.

        :param story_type: The type of the story.
        :param grain: The granularity of the story.
        :param current_date: The current date to compare against.
        :return: The last rendered story instance or None if no such story exists.
        """
        # Create a query to select the last rendered story before the current date
        statement = (
            self.get_select_query()
            .filter(func.date(Story.story_date) < func.date(current_date))  # type: ignore
            .filter_by(
                story_type=story_type,
                grain=grain,
                render_story=True,
                is_salient=True,  # Filter by story type, grain, and other conditions
            )
            .order_by(desc("story_date"))  # Order by story date in descending order
            .limit(1)
        )  # Limit the results to 1

        # Execute the query
        results = await self.session.execute(statement=statement)

        # Get the unique result or None if no result is found
        instance: Story | None = results.unique().scalar_one_or_none()  # noqa

        return instance


class CRUDStoryConfig(CRUDBase[StoryConfig, StoryConfig, StoryConfig, StoryConfigFilter]):
    """
    CRUD for StoryConfig Model.
    Provides methods to interact with the StoryConfig table in the database.
    """

    filter_class = StoryConfigFilter

    async def get_story_config(
        self, story_type: StoryType, grain: Granularity
    ) -> tuple[str | None, int | None]:  # noqa
        """
        Retrieve the heuristic expression and cool-off duration for a specific story type and granularity.

        :param story_type: The type of the story.
        :param grain: The granularity of the story.
        :return: A tuple containing the heuristic expression and cool-off duration, or (None, None)
        if no such config exists.
        """
        # Create a query to select the story config for the given story type and granularity
        statement = self.get_select_query().filter_by(story_type=story_type, grain=grain)

        # Execute the query
        results = await self.session.execute(statement=statement)

        # Get the unique result or None if no result is found
        instance: StoryConfig | None = results.unique().scalar_one_or_none()  # noqa

        # If no instance is found, return (None, None)
        if instance is None:
            return None, None

        # Return the heuristic expression and cool-off duration
        return instance.heuristic_expression, instance.cool_off_duration
