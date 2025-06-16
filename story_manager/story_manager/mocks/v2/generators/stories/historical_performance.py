"""
Historical Performance Story Generator
"""

import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.story_evaluator.evaluators import HistoricalPerformanceEvaluator

from ..patterns import HistoricalPerformancePatternGenerator
from .base import StoryGeneratorBase


class HistoricalPerformanceStoryGenerator(StoryGeneratorBase):
    """Generator for historical performance stories"""

    def __init__(self):
        super().__init__()
        self.evaluator = HistoricalPerformanceEvaluator()

    async def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None, **kwargs
    ) -> list[dict[str, Any]]:
        """Generate historical performance stories"""

        if story_date is None:
            story_date = date.today()

        # Create analysis window
        analysis_window = self.create_analysis_window(grain, story_date)

        # Generate multiple pattern results to ensure story diversity
        all_stories = []

        # Generate 3-4 different pattern results to get variety in stories
        for i in range(random.randint(3, 4)):
            # Generate pattern result
            pattern_generator = HistoricalPerformancePatternGenerator(
                metric_id=metric["metric_id"],
                analysis_window=analysis_window,
            )
            pattern_result = pattern_generator.generate()

            # Generate stories using evaluator
            stories = await self.evaluator.evaluate(pattern_result, metric)
            all_stories.extend(stories)

        # Ensure we have at least 2 stories per story type
        enhanced_stories = self.ensure_minimum_stories(all_stories, metric, min_stories=2)

        return enhanced_stories
