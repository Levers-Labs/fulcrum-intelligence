"""
Base classes for story evaluators.

Story evaluators analyze pattern outputs and generate stories based on specific conditions.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

import numpy as np
import pandas as pd

from commons.models.enums import Granularity
from levers.models.common import BasePattern
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator.constants import STORY_TEMPLATES, STORY_TYPE_TIME_DURATIONS

T = TypeVar("T", bound=BasePattern)
logger = logging.getLogger(__name__)


class StoryEvaluatorBase(Generic[T], ABC):
    """
    Base class for story evaluators.

    Story evaluators take pattern results as input and generate stories based on
    evaluation of the pattern data.
    """

    pattern_name: str
    # decimal precision
    precision = 3
    # series data (set by run method)
    _series_data: dict[str, Any]

    # Maps story types to required pattern components for data extraction
    # This should be overridden by subclasses to define which components
    # should be included for each story type
    REQUIRED_PATTERN_COMPONENTS: dict[StoryType, list[str]] = {}

    @abstractmethod
    async def evaluate(self, pattern_result: T, metric: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Evaluate the pattern result and generate stories.

        Args:
            pattern_result: The pattern result to evaluate
            metric: Details about the metric

        Returns:
            list of story dictionaries
        """
        pass

    def extract_data_points(self, pattern_result: T, story_type: StoryType) -> dict[str, Any]:
        """
        Extract data points from pattern result for a specific story type.
        This provides the analytical data used to generate visualizations.

        Args:
            pattern_result: The pattern result containing the data
            story_type: The type of story being generated

        Returns:
            dictionary containing relevant data for the story type
        """
        data = {}

        # Get the relevant components for this story type
        components = self.REQUIRED_PATTERN_COMPONENTS.get(story_type, [])

        # If no components defined for this story type, return empty data
        if not components:
            return data

        # For most stories, we want the first defined component that exists
        for component_name in components:
            component = getattr(pattern_result, component_name, None)
            if component is not None:
                # Merge the component's data into the main data dictionary
                data.update(component.model_dump())

        return data

    async def prepare_story_model(
        self,
        genre: StoryGenre,
        story_type: StoryType,
        story_group: StoryGroup,
        metric_id: str,
        pattern_result: T,
        title: str,
        detail: str,
        grain: Granularity,
        **extra_data,
    ) -> dict[str, Any]:
        """
        Prepare a story model dictionary.

        Args:
            genre: The genre of the story
            story_type: The type of story
            story_group: The group of the story
            metric_id: The ID of the metric
            pattern_result: The pattern result
            title: The story title
            detail: The story detail text
            grain: Granularity of the analysis
            extra_data: Additional data to include in the story

        Returns:
            Story model dictionary
        """
        # Get the story date from the pattern result
        story_date = pattern_result.analysis_date

        # Format the series data for this story
        formatted_series = self.get_story_series(story_type, grain)

        # Extract data from the pattern result
        data = self.extract_data_points(pattern_result, story_type)

        return {
            "version": 2,
            "genre": genre,
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
            "series": formatted_series,
            "metadata": dict(pattern=pattern_result.pattern),
            "pattern_run_id": pattern_result.pattern_run_id,
            "data": data,
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

    def get_story_series(self, story_type: StoryType, grain: Granularity) -> list[dict[str, Any]]:
        """
        Format the time series data for story display.

        Args:
            story_type: Type of the story
            grain: Granularity for which the story is generated

        Returns:
            dictionary with formatted time series data and analytics
        """
        # Process the time series data
        df = pd.DataFrame(self._series_data["data"]) if self._series_data else pd.DataFrame()

        # If series_data is not empty, format it
        if not df.empty:
            series_length = self.get_time_durations(story_type, grain)

            df["date"] = pd.to_datetime(df["date"])
            # Convert 'date' to ISO format strings
            df["date"] = df["date"].dt.date.apply(lambda d: d.isoformat())

            df.replace([float("inf"), float("-inf"), np.NaN], [None, None, None], inplace=True)  # type: ignore
            series = df.tail(series_length) if series_length else df

            # Add the time series data to the result
            return series.to_dict(orient="records")

        return []

    def get_time_durations(self, story_type: StoryType, grain: Granularity) -> int | None:
        """
        Get the time durations for the given grain.

        Args:
            story_type: The story type for which the time durations are retrieved
            grain: The grain for which the time durations are retrieved

        Returns:
            Integer value of the time duration or None if the story type and grain are not in the supported list
        """
        # Check if the story type and grain are in the supported list
        if story_type not in STORY_TYPE_TIME_DURATIONS or grain not in STORY_TYPE_TIME_DURATIONS[story_type]:
            return None

        # Return the time durations
        grain_durations = STORY_TYPE_TIME_DURATIONS[story_type][grain]

        return grain_durations["output"]

    async def run(self, pattern_result: T, metric: dict[str, Any], series_data: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Run the story evaluator on the pattern result.

        Args:
            pattern_result: The pattern result to evaluate
            metric: Details about the metric
            series_data: Pre-fetched time series data from tasks_manager

        Returns:
            list of generated story dictionaries
        """
        logger.info(f"Running story evaluator for pattern {self.pattern_name}")

        # Store raw series data as class attribute
        self._series_data = series_data

        # Generate stories from pattern result
        stories = await self.evaluate(pattern_result, metric)

        if not stories:
            logger.info("No stories generated for pattern %s", self.pattern_name)
            return []

        logger.info(f"Generated {len(stories)} stories for pattern {self.pattern_name}")
        return stories
