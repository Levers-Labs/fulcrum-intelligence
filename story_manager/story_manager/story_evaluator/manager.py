"""
Story evaluator manager for generating stories from pattern results.
"""

import logging
from typing import Any, TypeVar

import pandas as pd
from pydantic import BaseModel
from sqlmodel.ext.asyncio.session import AsyncSession

from story_manager.core.crud import CRUDStory
from story_manager.core.models import Story
from story_manager.story_evaluator.factory import StoryEvaluatorFactory

T = TypeVar("T", bound=BaseModel)
logger = logging.getLogger(__name__)


class StoryEvaluatorManager:
    """
    Manager for evaluating pattern results and generating stories.
    """

    async def persist_stories(self, stories: list[dict[str, Any]], db_session: AsyncSession) -> list[Story]:
        """
        Persist stories to the database using context-based upsert semantics.

        Args:
            stories: List of story dictionaries to persist
            db_session: Database session for persisting stories

        Returns:
            List of persisted Story objects
        """
        if not stories:
            logger.info("No stories to persist")
            return []

        logger.info(f"Persisting {len(stories)} stories")

        # Delegate to CRUD layer for data persistence (handles tenant context internally)
        story_crud = CRUDStory(Story, db_session)
        return await story_crud.upsert_stories_by_context(stories)

    async def evaluate_pattern_result(
        self, pattern_result: BaseModel, metric: dict[str, Any], series_df: pd.DataFrame | None = None
    ) -> list[dict[str, Any]]:
        """
        Evaluate a pattern result and generate stories.

        Args:
            pattern_result: Pattern result to evaluate
            metric: Metric details
            series_df: Optional pre-fetched time series data

        Returns:
            List of generated stories
        """
        pattern_name = getattr(pattern_result, "pattern", None)
        if not pattern_name:
            logger.error("Pattern result missing 'pattern' attribute")
            raise ValueError("Pattern result missing 'pattern' attribute")

        metric_id = getattr(pattern_result, "metric_id", None)
        if not metric_id:
            logger.error("Pattern result missing 'metric_id' attribute")
            raise ValueError("Pattern result missing 'metric_id' attribute")

        try:
            # Create story evaluator for the pattern
            evaluator = StoryEvaluatorFactory.create_story_evaluator(pattern_name, series_df=series_df)

            # Run the evaluator with series data
            stories = await evaluator.run(pattern_result, metric)

            logger.info("Generated %d stories for pattern %s, metric %s", len(stories), pattern_name, metric_id)
            return stories
        except Exception as e:
            logger.error("Error evaluating pattern %s for metric %s: %s", pattern_name, metric_id, e)
            raise
