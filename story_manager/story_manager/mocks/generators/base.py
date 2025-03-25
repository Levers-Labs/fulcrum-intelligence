from abc import ABC, abstractmethod
from datetime import date
from typing import Any

from jinja2 import Template

from commons.models.enums import Granularity
from commons.utilities.grain_utils import GRAIN_META
from story_manager.core.enums import (
    STORY_TYPES_META,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.story_builder.constants import STORY_GROUP_TIME_DURATIONS


class MockGeneratorBase(ABC):
    """Base class for all mock story generators"""

    genre: StoryGenre
    group: StoryGroup

    def __init__(self, mock_data_service):
        self.data_service = mock_data_service

    @abstractmethod
    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock stories for the given parameters"""
        pass

    @abstractmethod
    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """
        Generate mock time series data specific to this story group and story type

        Each story group should implement its own pattern logic here.
        For example:
        - Long Range needs improving/worsening patterns
        - Goal vs Actual needs on-track/off-track patterns
        - Trend Changes needs upward/downward/stable patterns
        etc.
        """
        pass

    @abstractmethod
    def get_mock_variables(self, metric: dict[str, Any], story_type: StoryType, grain: Granularity) -> dict[str, Any]:
        """
        Generate mock variables specific to this story group and story type

        Each story group should define its own variables structure based on
        what its story types need.
        """
        pass

    def prepare_story_dict(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]],
        variables: dict[str, Any],
        story_date: date | None = None,
    ) -> dict[str, Any]:
        """
        Create a story dictionary with all required fields

        Args:
            metric: Dictionary containing metric information
            story_type: Type of story to create
            grain: Granularity level
            time_series: Time series data for the story
            variables: Variables for rendering templates
            story_date: Date for the story

        Returns:
            Complete story dictionary
        """

        grain_meta = GRAIN_META[grain]
        variables["eoi"] = grain_meta["eoi"]
        variables["pop"] = grain_meta["pop"]
        variables["interval"] = grain_meta["interval"]

        output = STORY_GROUP_TIME_DURATIONS[self.group][grain]["output"]
        title_template = Template(STORY_TYPES_META[story_type]["title"])
        detail_template = Template(STORY_TYPES_META[story_type]["detail"])

        return {
            "metric_id": metric["metric_id"],
            "genre": self.genre,
            "story_group": self.group,
            "story_type": story_type,
            "story_date": story_date or self.data_service.story_date,
            "grain": grain,
            "series": time_series[-output:] if output else time_series,
            "title": title_template.render(variables),
            "detail": detail_template.render(variables),
            "title_template": STORY_TYPES_META[story_type]["title"],
            "detail_template": STORY_TYPES_META[story_type]["detail"],
            "variables": variables,
            "is_salient": True,
            "in_cool_off": False,
            "is_heuristic": True,
        }
