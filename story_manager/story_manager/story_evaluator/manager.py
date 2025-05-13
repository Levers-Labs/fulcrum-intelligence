"""
Story evaluator manager for generating stories from pattern results.
"""

import logging
from typing import Any, TypeVar

from pydantic import BaseModel
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.utilities.context import get_tenant_id
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
        Persist stories to the database.

        Args:
            stories: List of story dictionaries to persist
            db_session: Database session for persisting stories

        Returns:
            List of persisted Story objects
        """
        story_objs = []
        for story_dict in stories:
            tenant_id = get_tenant_id()
            story_dict["tenant_id"] = tenant_id
            story_obj = Story.model_validate(story_dict)
            db_session.add(story_obj)
            story_objs.append(story_obj)

        if story_objs:
            await db_session.commit()
            # Refresh objects to get database-generated fields
            for obj in story_objs:
                await db_session.refresh(obj)

        return story_objs

    async def evaluate_pattern_result(
        self, pattern_result: BaseModel, metric: dict[str, Any], series_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Evaluate a pattern result and generate stories.

        Args:
            pattern_result: Pattern result to evaluate
            metric: Metric details
            series_data: Optional pre-fetched time series data

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
            evaluator = StoryEvaluatorFactory.create_story_evaluator(pattern_name)

            # Run the evaluator with series data
            stories = await evaluator.run(pattern_result, metric, series_data)

            logger.info("Generated %d stories for pattern %s, metric %s", len(stories), pattern_name, metric_id)
            return stories
        except Exception as e:
            logger.error("Error evaluating pattern %s for metric %s: %s", pattern_name, metric_id, e)
            raise
