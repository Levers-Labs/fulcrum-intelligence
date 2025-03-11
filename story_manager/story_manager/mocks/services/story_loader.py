import logging
from datetime import date
from typing import List

from sqlmodel.ext.asyncio.session import AsyncSession

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGroup
from story_manager.core.models import Story
from story_manager.mocks.generators.component_drift import ComponentDriftMockGenerator
from story_manager.mocks.generators.goal_vs_actual import GoalVsActualMockGenerator
from story_manager.mocks.generators.growth_rates import GrowthRatesMockGenerator
from story_manager.mocks.generators.influence_drift import InfluenceDriftMockGenerator
from story_manager.mocks.generators.likely_status import LikelyStatusMockGenerator
from story_manager.mocks.generators.long_range import LongRangeMockGenerator
from story_manager.mocks.generators.record_values import RecordValuesMockGenerator
from story_manager.mocks.generators.required_performance import RequiredPerformanceMockGenerator
from story_manager.mocks.generators.segment_drift import SegmentDriftMockGenerator
from story_manager.mocks.generators.significant_segments import SignificantSegmentMockGenerator
from story_manager.mocks.generators.status_change import StatusChangeMockGenerator
from story_manager.mocks.generators.trend_changes import TrendChangesMockGenerator
from story_manager.mocks.generators.trend_exceptions import TrendExceptionsMockGenerator
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
            # Add other generators as needed
        }

    async def run(
        self, metric_id: str, grain: Granularity, story_group: StoryGroup, story_date: date = None
    ) -> list[Story]:
        """
        Generate mock stories and load them into the database

        :param metric_id: The metric ID to generate stories for
        :param grain: The granularity level
        :param story_group: The story group type
        :param story_date: Optional specific date for the stories
        :return: List of persisted Story objects
        """
        logger.info(f"Generating and loading mock stories for metric '{metric_id}'")

        # Check if we have a generator for this story group
        if story_group not in self.generators:
            raise ValueError(f"No generator available for story group: {story_group}")

        # Get the appropriate generator
        generator = self.generators[story_group]

        # Create a mock metric object
        # In a real implementation, you might want to fetch actual metric details
        # TODO: think how to use this
        # mock_metric = {
        #     "id": "inqToMqlRate",
        #     "label": "Inquiry to MQL Rate",
        #     "components": ["newMqls", "newInqs"],
        #     "influencers": ["newMqls", "newInqs"],
        #     "output_metric": ["newMqls"]
        # }
        mock_metric = {
            "id": "newInqs",
            "label": "New Inquiries",
            "dimensions": [
                {
                    "dimension_id": "owner_id",
                    "label": "Owner Id",
                },
                {
                    "dimension_id": "region",
                    "label": "Region",
                },
                {
                    "dimension_id": "segment",
                    "label": "Segment",
                },
                {
                    "dimension_id": "enterprise_lifecycle_stage",
                    "label": "Enterprise Lifecycle Stage",
                },
                {
                    "dimension_id": "enterprise_lifecycle_status",
                    "label": "Enterprise Lifecycle Status",
                },
            ],
        }

        # Generate stories
        mock_stories = generator.generate_stories(metric=mock_metric, grain=grain, story_date=story_date)

        # Convert to Story objects
        story_objects = [Story(**story_dict) for story_dict in mock_stories]

        # Add to database
        self.db_session.add_all(story_objects)
        await self.db_session.commit()

        # No need to set heuristics as they're already in the mock data

        # Refresh to get latest data
        for story in story_objects:
            await self.db_session.refresh(story)

        logger.info(f"Successfully loaded {len(story_objects)} mock stories")
        return story_objects
