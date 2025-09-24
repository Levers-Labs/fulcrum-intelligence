from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from typing import Any

from sqlalchemy import (
    Date as SQLDate,
    cast,
    desc,
    func,
    select,
)

from commons.db.crud import CRUDBase
from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.core.filters import StoryConfigFilter, StoryFilter
from story_manager.core.models import Story, StoryConfig
from story_manager.core.schemas import StoryStatsResponse


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
        version: int,
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
        :param version: The version of the story.
        :param is_salient: Optional filter for salient stories.
        :param is_cool_off: Optional filter for cooled-down stories.
        :param is_heuristic: Optional filter for heuristic stories.
        :return: The latest story instance or None if no such story exists.
        """

        # Create a query to select the latest story before the current date
        statement = (
            self.get_select_query()
            .filter(Story.story_date < datetime.combine(story_date, time.min))  # type: ignore
            .filter_by(story_type=story_type, grain=grain, metric_id=metric_id, tenant_id=tenant_id, version=version)
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
        instance: Story | None = (await result.unique()).scalar_one_or_none()  # noqa

        return instance

    async def get_stories(
        self,
        metric_id: str | None = None,
        grain: Granularity | None = None,
        story_date: date | None = None,
        is_salient: bool | None = None,
        is_cool_off: bool | None = None,
        is_heuristic: bool | None = True,
    ) -> Any:
        """
        Retrieve all stories for a specific metric, granularity and creation date, with optional filters.

        This method queries the database to find stories that match the given criteria, including
        optional filters for salience, cool-off status, and heuristic flag. Stories are filtered
        to only include those created on the specified date.

        Args:
            metric_id: The ID of the metric to retrieve stories for
            grain: The time granularity of the stories (e.g. daily, weekly)
            created_date: The date when the stories were created
            is_salient: Optional filter for salient stories
            is_cool_off: Optional filter for stories in cool-off period
            is_heuristic: Optional filter for heuristic stories, defaults to True

        Returns:
            list[Story] | None: A list of Story objects matching the criteria, or None if no stories found
        """
        # Build base query filtering by date range, grain, metric and tenant
        statement = self.get_select_query()
        if story_date is not None:
            # Filter stories created on the specified date (inclusive of start, exclusive of end)
            start_date = datetime.combine(story_date, time.min)
            end_date = start_date + timedelta(days=1)
            statement = statement.filter(Story.story_date >= start_date, Story.story_date < end_date)  # type: ignore
        if grain is not None:
            statement = statement.filter_by(grain=grain)
        if metric_id is not None:
            statement = statement.filter_by(metric_id=metric_id)

        # Apply optional boolean filters if specified
        if is_salient is not None:
            statement = statement.filter_by(is_salient=is_salient)
        if is_cool_off is not None:
            statement = statement.filter_by(in_cool_off=is_cool_off)
        if is_heuristic is not None:
            statement = statement.filter_by(is_heuristic=is_heuristic)

        # Order results by story date in descending order (newest first)
        statement = statement.order_by(desc("story_date"))

        # Execute the query asynchronously
        result = await self.session.execute(statement=statement)

        # Return all matching stories as a list
        instances: list(Story) | None = result.scalars().all()  # type: ignore

        return instances

    async def update_story_date(self, story_id: int, new_date: date):
        """
        Update the date of a story in the database.
        """
        statement = self.get_select_query().where(Story.id == story_id)  # type: ignore
        result = await self.session.execute(statement)
        story = result.scalar_one_or_none()
        if story is None:
            return False
        story.story_date = new_date
        self.session.add(story)
        return True

    async def get_story_stats(self, filter_params: dict[str, Any]) -> list[StoryStatsResponse]:
        """
        Get story count statistics grouped by story date.

        This method queries the database to count stories grouped by story_date,
        applying the provided filters.

        Args:
            filter_params: Dictionary of filter parameters

        Returns:
            A list of StoryStatsResponse containing story_date and count
        """
        # Build the select statement with grouping and counting
        stmt = select(cast(Story.story_date, SQLDate).label("story_date"), func.count().label("count")).group_by(
            cast(Story.story_date, SQLDate)
        )

        # Apply filters
        stmt = self.filter_class.apply_filters(stmt, filter_params)

        # Order by story_date
        stmt = stmt.order_by(cast(Story.story_date, SQLDate).desc())

        # Execute the query
        result = await self.session.execute(stmt)
        return list(result.mappings())  # type: ignore


class CRUDStoryConfig(CRUDBase[StoryConfig, StoryConfig, StoryConfig, StoryConfigFilter]):
    """
    CRUD for StoryConfig Model.
    Provides methods to interact with the StoryConfig table in the database.
    """

    filter_class = StoryConfigFilter

    async def get_story_config(
        self, story_type: StoryType, grain: Granularity, tenant_id: int, version: int
    ) -> StoryConfig | None:  # noqa
        """
        Retrieve the StoryConfig for a specific story type and granularity.

        This method queries the database to find the configuration settings for a given story type and granularity.
        The configuration includes the heuristic expression and cool-off duration, which are used to evaluate the
        salience and cool-off period of stories.

        :param story_type: The type of the story.
        :param grain: The granularity of the story.
        :param tenant_id: The tenant ID
        :param version: The version of the story config
        :return: A StoryConfig object if found, otherwise None.
        """
        # Create a query to select the story config for the given story type and granularity
        statement = self.get_select_query().filter_by(
            story_type=story_type, grain=grain, tenant_id=tenant_id, version=version
        )

        # Execute the query and fetch the results
        results = await self.session.execute(statement=statement)

        # Get the unique result or None if no result is found
        instance: StoryConfig | None = (await results.unique()).scalar_one_or_none()  # noqa

        # Return the StoryConfig instance or None
        return instance