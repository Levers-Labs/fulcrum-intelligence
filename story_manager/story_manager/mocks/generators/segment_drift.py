import random
from datetime import date
from typing import Any, Dict, List

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.mocks.services.data_service import MockDataService
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class SegmentDriftMockGenerator(MockGeneratorBase):
    """Mock generator for Segment Drift stories"""

    genre = StoryGenre.ROOT_CAUSES
    group = StoryGroup.SEGMENT_DRIFT
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    def __init__(self, mock_data_service: MockDataService):
        self.data_service = mock_data_service

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock segment drift stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Check if metric has dimensions
        if not metric.get("dimensions"):
            return []

        # Create fixed segments for each story type
        segments = self._create_fixed_segments()

        # Generate stories for each segment and story type
        for segment in segments:
            story_type = segment["story_type"]

            # Get time series and variables
            time_series = self.get_mock_time_series(grain, story_type, segment)
            variables = self.get_mock_variables(metric, story_type, grain, time_series, segment)

            # Create story
            story = self.prepare_story_dict(
                metric, story_type, grain, time_series, variables, story_date or self.data_service.story_date
            )
            stories.append(story)

        return stories

    def get_mock_time_series(
        self, grain: Granularity, story_type: StoryType, segment: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Generate mock time series data for segment drift stories"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Use first and last date
        first_date = formatted_dates[0]
        last_date = formatted_dates[-1]

        # Create time series based on story type
        if story_type in [StoryType.GROWING_SEGMENT, StoryType.SHRINKING_SEGMENT]:
            # Share-based stories
            time_series = [
                {
                    "date": first_date,
                    "value": segment["previous_share"],
                    "dimension": segment["dimension"],
                    "slice": segment["slice"],
                    "label": "Last month",
                },
                {
                    "date": last_date,
                    "value": segment["current_share"],
                    "dimension": segment["dimension"],
                    "slice": segment["slice"],
                    "label": "This month",
                },
            ]
        else:
            # Value-based stories
            time_series = [
                {
                    "date": first_date,
                    "value": segment["previous_value"],
                    "dimension": segment["dimension"],
                    "slice": segment["slice"],
                    "label": "Before",
                },
                {
                    "date": last_date,
                    "value": segment["current_value"],
                    "dimension": segment["dimension"],
                    "slice": segment["slice"],
                    "label": "After",
                },
            ]

        return time_series

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] = None,
        segment: dict[str, Any] = None,
    ) -> dict[str, Any]:
        """Generate mock variables for segment drift stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Create variables dict with all segment data
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "dimension": segment["dimension"],
            "slice": segment["slice"],
            "previous_share": segment["previous_share"],
            "current_share": segment["current_share"],
            "slice_share_change_percentage": abs(segment["slice_share_change_percentage"]),
            "pressure_direction": segment["pressure_direction"],
            "previous_value": segment["previous_value"],
            "current_value": segment["current_value"],
            "deviation": segment["deviation"],
            "pressure_change": segment["pressure_change"],
            "impact": segment["impact"],
        }

        return variables

    def _create_fixed_segments(self) -> list[dict[str, Any]]:
        """Create fixed segments for each story type"""
        segments = [
            # GROWING_SEGMENT
            {
                "story_type": StoryType.GROWING_SEGMENT,
                "dimension": "Region",
                "slice": "South America",
                "previous_share": 13,
                "current_share": 24,
                "slice_share_change_percentage": 11,
                "pressure_direction": "upward",
                "previous_value": 800,
                "current_value": 1000,
                "deviation": 25,
                "pressure_change": 7,
                "impact": 1.5,
                "sort_value": 1.5,
            },
            # SHRINKING_SEGMENT
            {
                "story_type": StoryType.SHRINKING_SEGMENT,
                "dimension": "Region",
                "slice": "Europe",
                "previous_share": 35,
                "current_share": 22,
                "slice_share_change_percentage": -13,
                "pressure_direction": "downward",
                "previous_value": 1200,
                "current_value": 900,
                "deviation": 25,
                "pressure_change": 8,
                "impact": -1.2,
                "sort_value": 1.2,
            },
            # IMPROVING_SEGMENT
            {
                "story_type": StoryType.IMPROVING_SEGMENT,
                "dimension": "Segment",
                "slice": "Enterprise",
                "previous_share": 20,
                "current_share": 28,
                "slice_share_change_percentage": 8,
                "pressure_direction": "upward",
                "previous_value": 1200,
                "current_value": 1600,
                "deviation": 33.33,
                "pressure_change": 12,
                "impact": 1.8,
                "sort_value": 1.8,
            },
            # WORSENING_SEGMENT
            {
                "story_type": StoryType.WORSENING_SEGMENT,
                "dimension": "Region",
                "slice": "North America",
                "previous_share": 25,
                "current_share": 18,
                "slice_share_change_percentage": -7,
                "pressure_direction": "downward",
                "previous_value": 1576,
                "current_value": 1176,
                "deviation": 25.38,
                "pressure_change": 9,
                "impact": -1.4,
                "sort_value": 1.4,
            },
        ]

        return segments
