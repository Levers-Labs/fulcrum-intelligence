"""
Performance Status Story Generator
"""

import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.story_evaluator.evaluators import PerformanceStatusEvaluator

from ..patterns import PerformanceStatusPatternGenerator
from .base import StoryGeneratorBase


class PerformanceStatusStoryGenerator(StoryGeneratorBase):
    """Generator for performance status stories"""

    def __init__(self):
        super().__init__()
        self.evaluator = PerformanceStatusEvaluator()

    async def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None, **kwargs
    ) -> list[dict[str, Any]]:
        """Generate performance status stories"""

        if story_date is None:
            story_date = date.today()

        # Create analysis window
        analysis_window = self.create_analysis_window(grain, story_date)

        # Generate multiple pattern results to ensure story diversity
        all_stories = []

        # Generate 2-3 different pattern results to get variety in stories
        for i in range(random.randint(2, 3)):
            # Generate pattern result
            pattern_generator = PerformanceStatusPatternGenerator(
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
