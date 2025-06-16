"""
Base class for V2 story generators.
"""

import random
from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Any

from commons.models.enums import Granularity
from levers.models.common import AnalysisWindow, BasePattern


class StoryGeneratorBase(ABC):
    """Base class for V2 story generators that use pattern results"""

    def __init__(self):
        pass

    @abstractmethod
    async def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None, **kwargs
    ) -> list[dict[str, Any]]:
        """Generate stories for the given parameters"""
        pass

    def create_analysis_window(self, grain: Granularity, story_date: date) -> AnalysisWindow:
        """Create an analysis window for the given grain and date"""

        # Calculate date range based on grain
        if grain == Granularity.DAY:
            days_back = 180
        elif grain == Granularity.WEEK:
            days_back = 365
        elif grain == Granularity.MONTH:
            days_back = 730
        else:
            days_back = 365

        start_date = story_date - timedelta(days=days_back)
        end_date = story_date

        return AnalysisWindow(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            grain=grain,
        )

    def ensure_minimum_stories(
        self, stories: list[dict[str, Any]], metric: dict[str, Any], min_stories: int = 2
    ) -> list[dict[str, Any]]:
        """Ensure we have at least the minimum number of stories per story type"""

        if len(stories) == 0:
            return stories

        # Group stories by story type
        stories_by_type = {}
        for story in stories:
            story_type = story.get("story_type")
            if story_type not in stories_by_type:
                stories_by_type[story_type] = []
            stories_by_type[story_type].append(story)

        enhanced_stories = []

        # For each story type, ensure we have at least min_stories
        for story_type, type_stories in stories_by_type.items():
            enhanced_stories.extend(type_stories)

            # If we have fewer than min_stories, create variations
            while len([s for s in enhanced_stories if s.get("story_type") == story_type]) < min_stories:
                # Create a variation of an existing story
                base_story = random.choice(type_stories)
                variation = self._create_story_variation(base_story, metric)
                enhanced_stories.append(variation)

        return enhanced_stories

    def _create_story_variation(self, base_story: dict[str, Any], metric: dict[str, Any]) -> dict[str, Any]:
        """Create a variation of an existing story"""
        variation = base_story.copy()

        # Add slight variations to variables to make the story different
        if "variables" in variation and variation["variables"]:
            new_variables = variation["variables"].copy()

            # Apply small random variations to numeric values
            for key, value in new_variables.items():
                if isinstance(value, (int, float)) and key not in ["metric_id", "version"]:
                    if isinstance(value, float):
                        new_variables[key] = round(value * random.uniform(0.9, 1.1), 2)
                    elif isinstance(value, int) and value > 1:
                        new_variables[key] = max(1, int(value * random.uniform(0.9, 1.1)))

            variation["variables"] = new_variables

            # Re-render title and detail with new variables
            if "title_template" in variation and "detail_template" in variation:
                from jinja2 import Template

                try:
                    title_template = Template(variation["title_template"])
                    detail_template = Template(variation["detail_template"])
                    variation["title"] = title_template.render(new_variables)
                    variation["detail"] = detail_template.render(new_variables)
                except Exception:
                    # If template rendering fails, just use original values
                    pass

        return variation
