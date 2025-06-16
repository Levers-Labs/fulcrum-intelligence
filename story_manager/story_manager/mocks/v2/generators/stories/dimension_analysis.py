"""
Dimension Analysis Story Generator
"""

import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.story_evaluator.evaluators import DimensionAnalysisEvaluator

from ..patterns import DimensionAnalysisPatternGenerator
from .base import StoryGeneratorBase


class DimensionAnalysisStoryGenerator(StoryGeneratorBase):
    """Generator for dimension analysis stories"""

    def __init__(self):
        super().__init__()
        self.evaluator = DimensionAnalysisEvaluator()

    async def generate_stories(
        self,
        metric: dict[str, Any],
        grain: Granularity,
        story_date: date | None = None,
        dimension_name: str | None = None,
        **kwargs
    ) -> list[dict[str, Any]]:
        """Generate dimension analysis stories"""

        if story_date is None:
            story_date = date.today()

        if dimension_name is None:
            dimension_name = random.choice(["Region", "Product", "Channel", "Segment"])

        # Create analysis window
        analysis_window = self.create_analysis_window(grain, story_date)

        # Generate multiple pattern results to ensure story diversity
        all_stories = []

        # Generate 2-3 different pattern results to get variety in stories
        for i in range(random.randint(2, 3)):
            # Generate pattern result
            pattern_generator = DimensionAnalysisPatternGenerator(
                metric_id=metric["metric_id"],
                analysis_window=analysis_window,
                dimension_name=dimension_name,
            )
            pattern_result = pattern_generator.generate()

            # Generate stories using evaluator
            stories = await self.evaluator.evaluate(pattern_result, metric)
            all_stories.extend(stories)

        # Ensure we have at least 2 stories per story type
        enhanced_stories = self.ensure_minimum_stories(all_stories, metric, min_stories=2)

        return enhanced_stories
