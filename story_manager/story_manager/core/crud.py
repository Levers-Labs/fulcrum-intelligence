from datetime import date, timedelta
from typing import Any

from sqlalchemy import desc, func

from commons.db.crud import CRUDBase
from commons.models.enums import Granularity
from story_manager.core.enums import StoryGroup, StoryType
from story_manager.core.filters import StoryConfigFilter, StoryFilter
from story_manager.core.models import Story, StoryConfig


class CRUDStory(CRUDBase[Story, Story, Story, StoryFilter]):
    """
    CRUD for Story Model.
    Provides methods to interact with the Story table in the database.
    """

    filter_class = StoryFilter

    async def get_latest_story(
        self,
        metric_id: str,
        story_type: StoryType,
        grain: Granularity,
        story_date: date,
        tenant_id: int,
        is_salient: bool | None = None,
        is_cool_off: bool | None = None,
        is_heuristic: bool | None = None,
    ) -> Any:
        """
        Retrieve the latest story of a specific type and granularity before the current date,
        with optional filters for salience, cool-down status, and heuristic flag.

        :param tenant_id:
        :param metric_id:
        :param story_type: The type of the story.
        :param grain: The granularity of the story.
        :param story_date: The current date to compare against.
        :param is_salient: Optional filter for salient stories.
        :param is_cool_off: Optional filter for cooled-down stories.
        :param is_heuristic: Optional filter for heuristic stories.
        :return: The latest story instance or None if no such story exists.
        """

        # Create a query to select the latest story before the current date
        statement = (
            self.get_select_query()
            .filter(func.date(Story.story_date) < func.date(story_date))  # type: ignore
            .filter_by(story_type=story_type, grain=grain, metric_id=metric_id, tenant_id=tenant_id)
        )

        # Apply optional filters
        if is_salient is not None:
            statement = statement.filter_by(is_salient=is_salient)
        if is_cool_off is not None:
            statement = statement.filter_by(in_cool_off=is_cool_off)
        if is_heuristic is not None:
            statement = statement.filter_by(is_heuristic=is_heuristic)

        # Order by story date in descending order and limit to 1 result
        statement = statement.order_by(desc("story_date")).limit(1)

        # Execute the query
        result = await self.session.execute(statement=statement)

        # Get the unique result or None if no result is found
        instance: Story | None = result.unique().scalar_one_or_none()  # noqa

        return instance

    async def get_stories(
        self,
        metric_id: str,
        story_group: StoryGroup,
        grain: Granularity,
        created_date: date,
        tenant_id: int,
    ) -> Any:

        statement = (
            self.get_select_query()
            .filter(func.date(Story.created_at) >= func.date(created_date))
            .filter(func.date(Story.created_at) < func.date(created_date + timedelta(days=1)))
            .filter_by(
                story_group=story_group, grain=grain, metric_id=metric_id, tenant_id=tenant_id, is_heuristic=True
            )
        )

        # Order by story date in descending order and limit to 1 result
        statement = statement.order_by(desc("story_date"))

        # Execute the query
        result = await self.session.execute(statement=statement)

        # Get the unique result or None if no result is found
        instances: list(Story) | None = result.scalars().all()  # type: ignore

        return instances


class CRUDStoryConfig(CRUDBase[StoryConfig, StoryConfig, StoryConfig, StoryConfigFilter]):
    """
    CRUD for StoryConfig Model.
    Provides methods to interact with the StoryConfig table in the database.
    """

    filter_class = StoryConfigFilter

    async def get_story_config(
        self, story_type: StoryType, grain: Granularity, tenant_id: int
    ) -> StoryConfig | None:  # noqa
        """
        Retrieve the StoryConfig for a specific story type and granularity.

        This method queries the database to find the configuration settings for a given story type and granularity.
        The configuration includes the heuristic expression and cool-off duration, which are used to evaluate the
        salience and cool-off period of stories.

        :param story_type: The type of the story.
        :param grain: The granularity of the story.
        :param tenant_id: The tenant ID
        :return: A StoryConfig object if found, otherwise None.
        """
        # Create a query to select the story config for the given story type and granularity
        statement = self.get_select_query().filter_by(story_type=story_type, grain=grain, tenant_id=tenant_id)

        # Execute the query and fetch the results
        results = await self.session.execute(statement=statement)

        # Get the unique result or None if no result is found
        instance: StoryConfig | None = results.unique().scalar_one_or_none()  # noqa

        # Return the StoryConfig instance or None
        return instance
