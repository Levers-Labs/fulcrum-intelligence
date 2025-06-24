"""
Mock Story Service V2

This service generates v2 stories by creating mock pattern results and evaluating them
through the story evaluator system.
"""

import logging
from datetime import date
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from commons.models.enums import Granularity
from story_manager.core.models import Story
from story_manager.mocks.v2.pattern_generators import (
    MockDimensionAnalysisGenerator,
    MockHistoricalPerformanceGenerator,
    MockPerformanceStatusGenerator,
)
from story_manager.story_evaluator.evaluators import (
    DimensionAnalysisEvaluator,
    HistoricalPerformanceEvaluator,
    PerformanceStatusEvaluator,
)

logger = logging.getLogger(__name__)


class MockStoryServiceV2:
    """
    Service to generate v2 stories using pattern results and story evaluators.

    This follows the v2 architecture:
    1. Generate mock pattern results (simulating what the real patterns would produce)
    2. Generate mock series data for the evaluators
    3. Pass pattern results and series data to story evaluators
    4. Story evaluators generate the actual stories
    5. Persist stories to database
    """

    def __init__(self, db_session: AsyncSession | None = None):
        self.db_session = db_session
        # Initialize pattern generators
        self.pattern_generators = {
            "performance_status": MockPerformanceStatusGenerator(),
            "historical_performance": MockHistoricalPerformanceGenerator(),
            "dimension_analysis": MockDimensionAnalysisGenerator(),
        }

    def _get_evaluator_for_pattern(self, pattern_name: str, series_df=None):
        """Get the appropriate evaluator for a pattern with series data."""
        if pattern_name == "performance_status":
            return PerformanceStatusEvaluator(series_df=series_df)
        elif pattern_name == "historical_performance":
            return HistoricalPerformanceEvaluator(series_df=series_df)
        elif pattern_name == "dimension_analysis":
            return DimensionAnalysisEvaluator(series_df=series_df)
        else:
            raise ValueError(f"Unknown pattern: {pattern_name}")

    async def generate_pattern_stories(
        self, pattern_name: str, metric: dict[str, Any], grain: Granularity, story_date: date
    ) -> list[dict[str, Any]]:
        """
        Generate stories for a specific pattern.

        Args:
            pattern_name: Name of the pattern to generate stories for
            metric: Metric dictionary containing metric details
            grain: Time granularity for the stories
            story_date: Date for story generation

        Returns:
            List of generated story dictionaries
        """
        if pattern_name not in self.pattern_generators:
            raise ValueError(f"Unknown pattern: {pattern_name}")

        generator = self.pattern_generators[pattern_name]
        stories = []

        try:

            # Generate mock series data if the generator supports it
            series_df = None
            if hasattr(generator, "generate_mock_series_data"):
                series_df = generator.generate_mock_series_data(metric=metric, grain=grain, story_date=story_date)

            # Generate mock pattern results
            if pattern_name in ["performance_status", "historical_performance"] and series_df is not None:
                # pass series_df for consistency
                pattern_results = generator.generate_pattern_results(  # type: ignore
                    metric=metric, grain=grain, story_date=story_date, series_df=series_df
                )
            else:
                pattern_results = generator.generate_pattern_results(metric=metric, grain=grain, story_date=story_date)

            # For historical performance, add period_metrics and trend_analysis to pattern results
            if (
                pattern_name == "historical_performance"
                and hasattr(generator, "generate_mock_period_metrics")
                and series_df is not None
            ):
                for pattern_result in pattern_results:
                    # Generate period metrics that align with series data
                    period_metrics = generator.generate_mock_period_metrics(series_df)
                    # Update the pattern result with aligned period metrics
                    pattern_result.period_metrics = period_metrics

                    # Generate trend analysis from series data if available
                    if hasattr(generator, "generate_mock_trend_analysis"):
                        trend_analysis = generator.generate_mock_trend_analysis(series_df, grain)
                        pattern_result.trend_analysis = trend_analysis

            # Get evaluator with series data
            evaluator = self._get_evaluator_for_pattern(pattern_name, series_df)

            # Evaluate each pattern result to generate stories
            for pattern_result in pattern_results:
                pattern_stories = await evaluator.evaluate(pattern_result=pattern_result, metric=metric)
                stories.extend(pattern_stories)

            logger.info(f"Generated {len(stories)} stories for {pattern_name}")

        except Exception as e:
            logger.error(f"Error generating {pattern_name} stories: {str(e)}")

        return stories

    async def persist_stories(self, stories: list[dict[str, Any]]) -> list[Story]:
        """
        Persist stories to the database.

        Args:
            stories: List of story dictionaries to persist

        Returns:
            List of persisted Story objects

        Raises:
            ValueError: If no database session is provided
        """
        if not self.db_session:
            raise ValueError("Database session is required for story persistence")

        if not stories:
            logger.info("No stories to persist")
            return []

        try:
            # Convert to Story objects
            story_objects = [Story(**story_dict) for story_dict in stories]

            # Add to database
            self.db_session.add_all(story_objects)
            await self.db_session.commit()

            # Refresh to get latest data
            for story in story_objects:
                await self.db_session.refresh(story)

            logger.info(f"Successfully persisted {len(story_objects)} v2 mock stories")
            return story_objects

        except Exception as e:
            logger.error(f"Error persisting stories: {str(e)}")
            await self.db_session.rollback()
            raise
