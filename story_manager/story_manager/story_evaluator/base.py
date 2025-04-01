"""
Base classes for story evaluators.

Story evaluators analyze pattern outputs and generate stories based on specific conditions.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from commons.models.enums import Granularity
from levers.models.common import BasePattern
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator.constants import STORY_TEMPLATES

T = TypeVar("T", bound=BasePattern)
logger = logging.getLogger(__name__)


class StoryEvaluatorBase(Generic[T], ABC):
    """
    Base class for story evaluators.

    Story evaluators take pattern results as input and generate stories based on
    evaluation of the pattern data.
    """

    pattern_name: str
    genre: StoryGenre

    @abstractmethod
    async def evaluate(self, pattern_result: T, metric: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Evaluate the pattern result and generate stories.

        Args:
            pattern_result: The pattern result to evaluate
            metric: Details about the metric

        Returns:
            List of story dictionaries
        """
        pass

    def prepare_story_model(
        self,
        story_type: StoryType,
        story_group: StoryGroup,
        metric_id: str,
        pattern_result: T,
        metric: dict[str, Any],
        title: str,
        detail: str,
        grain: Granularity,
        **extra_data
    ) -> dict[str, Any]:
        """
        Prepare a story model dictionary.

        Args:
            story_type: The type of story
            story_group: The group of the story
            metric_id: The ID of the metric
            pattern_result: The pattern result
            metric: Details about the metric
            title: The story title
            detail: The story detail text
            grain: Granularity of the analysis
            extra_data: Additional data to include in the story

        Returns:
            Story model dictionary
        """
        # Get the story date from the pattern result
        story_date = pattern_result.analysis_date

        return {
            "version": 2,
            "genre": self.genre,
            "story_type": story_type,
            "story_group": story_group,
            "grain": grain,
            "metric_id": metric_id,
            "title": title,
            "detail": detail,
            "title_template": self.get_template_string(story_type, "title"),
            "detail_template": self.get_template_string(story_type, "detail"),
            "story_date": story_date,
            "variables": extra_data,
            "metadata": dict(pattern=pattern_result.pattern),
            "pattern_run_id": pattern_result.pattern_run_id,
        }

    def get_template_string(self, story_type: StoryType, field: str) -> str:
        """
        Get the template string for a story type and field.

        Args:
            story_type: The type of story
            field: The field to get the template for (title or detail)

        Returns:
            The template string
        """

        story_templates = STORY_TEMPLATES.get(story_type, {})
        return story_templates.get(field, "")

    async def run(self, pattern_result: T, metric: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Run the story evaluator on the pattern result.

        Args:
            pattern_result: The pattern result to evaluate
            metric: Details about the metric

        Returns:
            List of generated story dictionaries
        """
        logger.info("Running story evaluator for pattern %s", self.pattern_name)
        stories = await self.evaluate(pattern_result, metric)

        if not stories:
            logger.info("No stories generated for pattern %s", self.pattern_name)
            return []

        logger.info("Generated %d stories for pattern %s", len(stories), self.pattern_name)

        return stories
