import logging
from datetime import date
from typing import Any

from sqlmodel.ext.asyncio.session import AsyncSession

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGroup
from story_manager.core.models import Story
from story_manager.mocks.generators import (
    ComponentDriftMockGenerator,
    GoalVsActualMockGenerator,
    GrowthRatesMockGenerator,
    InfluenceDriftMockGenerator,
    LikelyStatusMockGenerator,
    LongRangeMockGenerator,
    RecordValuesMockGenerator,
    RequiredPerformanceMockGenerator,
    SegmentDriftMockGenerator,
    SignificantSegmentMockGenerator,
    StatusChangeMockGenerator,
    TrendChangesMockGenerator,
    TrendExceptionsMockGenerator,
)
from story_manager.mocks.services.data_service import MockDataService

logger = logging.getLogger(__name__)


class MockStoryLoader:
    """Service to load mock stories into the database"""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.mock_data = MockDataService()
        # Initialize generators directly
        self.generators = {
            StoryGroup.LONG_RANGE: LongRangeMockGenerator(self.mock_data),
            StoryGroup.GOAL_VS_ACTUAL: GoalVsActualMockGenerator(self.mock_data),
            StoryGroup.GROWTH_RATES: GrowthRatesMockGenerator(self.mock_data),
            StoryGroup.RECORD_VALUES: RecordValuesMockGenerator(self.mock_data),
            StoryGroup.TREND_EXCEPTIONS: TrendExceptionsMockGenerator(self.mock_data),
            StoryGroup.TREND_CHANGES: TrendChangesMockGenerator(self.mock_data),
            StoryGroup.STATUS_CHANGE: StatusChangeMockGenerator(self.mock_data),
            StoryGroup.REQUIRED_PERFORMANCE: RequiredPerformanceMockGenerator(self.mock_data),
            StoryGroup.LIKELY_STATUS: LikelyStatusMockGenerator(self.mock_data),
            StoryGroup.COMPONENT_DRIFT: ComponentDriftMockGenerator(self.mock_data),
            StoryGroup.INFLUENCE_DRIFT: InfluenceDriftMockGenerator(self.mock_data),
            StoryGroup.SEGMENT_DRIFT: SegmentDriftMockGenerator(self.mock_data),
            StoryGroup.SIGNIFICANT_SEGMENTS: SignificantSegmentMockGenerator(self.mock_data),
        }

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_group: StoryGroup, story_date: date = None
    ) -> list[dict[str, Any]]:
        """
        Generate mock stories without persisting them

        :param metric: The metric to generate stories for
        :param grain: The granularity level
        :param story_group: The story group type
        :param story_date: Optional specific date for the stories
        :return: List of story dictionaries
        """
        logger.info(f"Generating mock stories for metric {metric['metric_id']}")

        # Check if we have a generator for this story group
        if story_group not in self.generators:
            raise ValueError(f"No generator available for story group: {story_group}")

        # Get the appropriate generator
        generator = self.generators[story_group]

        # Generate stories
        return generator.generate_stories(metric=metric, grain=grain, story_date=story_date)

    async def persist_stories(self, stories: list[dict[str, Any]]) -> list[Story]:
        """
        Load stories into the database

        :param stories: Stories to be loaded into database
        :return: List of persisted Story objects
        """

        # Convert to Story objects
        story_objects = [Story(**story_dict) for story_dict in stories]

        # Add to database
        self.db_session.add_all(story_objects)
        await self.db_session.commit()

        # Refresh to get latest data
        for story in story_objects:
            await self.db_session.refresh(story)

        logger.info(f"Successfully loaded {len(story_objects)} mock stories")
        return story_objects
