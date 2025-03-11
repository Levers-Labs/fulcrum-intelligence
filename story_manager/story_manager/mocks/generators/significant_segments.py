import random
from datetime import date
from typing import Any, Dict, List

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.mocks.services.data_service import MockDataService
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class SignificantSegmentMockGenerator(MockGeneratorBase):
    """Mock generator for Significant Segments stories"""

    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.SIGNIFICANT_SEGMENTS
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    def __init__(self, mock_data_service: MockDataService):
        self.data_service = mock_data_service

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock significant segments stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Check if metric has dimensions
        if not metric.get("dimensions"):
            return []

        # Generate mock segment data
        segment_data = self._generate_mock_segment_data(metric.get("dimensions", []))

        # Sort segments by value
        sorted_segments = sorted(segment_data, key=lambda x: x["value"], reverse=True)

        # Calculate average value
        avg_value = round(sum(segment["value"] for segment in sorted_segments) / len(sorted_segments))

        # Get top 4 segments
        top_segments = sorted_segments[: min(4, len(sorted_segments))]

        # Get bottom 4 segments
        bottom_segments = sorted_segments[-min(4, len(sorted_segments)) :]

        # Generate TOP_4_SEGMENTS story
        top_series = self.get_mock_time_series(grain, StoryType.TOP_4_SEGMENTS, top_segments)
        top_vars = self.get_mock_variables(metric, StoryType.TOP_4_SEGMENTS, grain, top_series, {"average": avg_value})
        top_story = self.prepare_story_dict(
            metric, StoryType.TOP_4_SEGMENTS, grain, top_series, top_vars, story_date or self.data_service.story_date
        )
        stories.append(top_story)

        # Generate BOTTOM_4_SEGMENTS story
        bottom_series = self.get_mock_time_series(grain, StoryType.BOTTOM_4_SEGMENTS, bottom_segments)
        bottom_vars = self.get_mock_variables(
            metric, StoryType.BOTTOM_4_SEGMENTS, grain, bottom_series, {"average": avg_value}
        )
        bottom_story = self.prepare_story_dict(
            metric,
            StoryType.BOTTOM_4_SEGMENTS,
            grain,
            bottom_series,
            bottom_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(bottom_story)

        return stories

    def get_mock_time_series(
        self, grain: Granularity, story_type: StoryType, segments: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Generate mock time series data for significant segments stories"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Use the last date for the current values
        current_date = formatted_dates[-1]

        # Create time series with segment values
        time_series = []

        for segment in segments:
            # Add segment data point
            point = {
                "date": current_date,
                "value": segment["value"],
                "dimension": segment["dimension"],
                "slice": segment["slice"],
                "rank": segment["rank"],
            }
            time_series.append(point)

        return time_series

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] = None,
        extra_data: dict[str, Any] = None,
    ) -> dict[str, Any]:
        """Generate mock variables for significant segments stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Create variables dict
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "average": extra_data.get("average", 0) if extra_data else 0,
        }

        return variables

    def _generate_mock_segment_data(self, dimensions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Generate mock data for segments"""
        segment_data = []

        # Sample dimension values
        dimension_values = {
            "Region": ["North America", "Europe", "Asia", "South America", "Africa", "Australia"],
            "Segment": ["Enterprise", "Mid-Market", "SMB", "Startup"],
            "Owner Id": ["Team A", "Team B", "Team C", "Team D", "Team E"],
            "Enterprise Lifecycle Stage": ["Prospect", "Customer", "Churned", "Renewal"],
            "Enterprise Lifecycle Status": ["Active", "Inactive", "Pending", "Completed"],
        }

        # Generate segments for each dimension
        rank = 1
        for dimension in dimensions:
            dimension_id = dimension.get("dimension_id", "")
            dimension_label = dimension.get("label", dimension_id)

            # Get possible values for this dimension
            values = dimension_values.get(dimension_label, ["Value 1", "Value 2", "Value 3", "Value 4"])

            # Generate segments for each value
            for value in values:
                # Generate a random value for this segment
                segment_value = random.randint(100, 1000)

                segment_data.append(
                    {"dimension": dimension_label, "slice": value, "value": segment_value, "rank": rank}
                )
                rank += 1

        return segment_data
