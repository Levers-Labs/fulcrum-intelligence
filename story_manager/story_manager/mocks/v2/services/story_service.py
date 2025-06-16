"""
V2 Mock Story Service

High-level service for generating v2 mock stories using pattern-based approach.
"""

import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity

from ..generators.stories import (
    DimensionAnalysisStoryGenerator,
    HistoricalPerformanceStoryGenerator,
    PerformanceStatusStoryGenerator,
)


class V2MockStoryService:
    """High-level service for generating v2 mock stories"""

    def __init__(self):
        self.dimension_generator = DimensionAnalysisStoryGenerator()
        self.historical_generator = HistoricalPerformanceStoryGenerator()
        self.performance_generator = PerformanceStatusStoryGenerator()

    async def generate_all_pattern_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None, **kwargs
    ) -> list[dict[str, Any]]:
        """Generate stories from all patterns"""

        if story_date is None:
            story_date = date.today()

        all_stories = []

        # Generate dimension analysis stories
        dim_stories = await self.dimension_generator.generate_stories(metric, grain, story_date, **kwargs)
        all_stories.extend(dim_stories)

        # Generate historical performance stories
        hist_stories = await self.historical_generator.generate_stories(metric, grain, story_date, **kwargs)
        all_stories.extend(hist_stories)

        # Generate performance status stories
        perf_stories = await self.performance_generator.generate_stories(metric, grain, story_date, **kwargs)
        all_stories.extend(perf_stories)

        return all_stories

    async def generate_dimension_analysis_stories(
        self,
        metric: dict[str, Any],
        grain: Granularity,
        story_date: date | None = None,
        dimension_name: str | None = None,
        **kwargs
    ) -> list[dict[str, Any]]:
        """Generate dimension analysis stories only"""

        return await self.dimension_generator.generate_stories(
            metric, grain, story_date, dimension_name=dimension_name, **kwargs
        )

    async def generate_historical_performance_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None, **kwargs
    ) -> list[dict[str, Any]]:
        """Generate historical performance stories only"""

        return await self.historical_generator.generate_stories(metric, grain, story_date, **kwargs)

    async def generate_performance_status_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None, **kwargs
    ) -> list[dict[str, Any]]:
        """Generate performance status stories only"""

        return await self.performance_generator.generate_stories(metric, grain, story_date, **kwargs)

    async def generate_mixed_stories(
        self,
        metric: dict[str, Any],
        grain: Granularity,
        story_date: date | None = None,
        num_stories: int = 10,
        **kwargs
    ) -> list[dict[str, Any]]:
        """Generate a mix of stories from different patterns"""

        if story_date is None:
            story_date = date.today()

        all_stories = []

        # Get stories from each pattern
        pattern_results = await self.generate_all_pattern_stories(metric, grain, story_date, **kwargs)

        # If we have enough stories, randomly sample
        if len(pattern_results) >= num_stories:
            return random.sample(pattern_results, num_stories)
        else:
            return pattern_results

    def get_sample_metric(self) -> dict[str, Any]:
        """Get a sample metric for testing"""
        return {
            "metric_id": "test_metric_001",
            "metric_name": "Revenue",
            "metric_type": "currency",
            "unit": "USD",
        }
