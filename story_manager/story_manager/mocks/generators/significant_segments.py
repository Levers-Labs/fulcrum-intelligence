import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.story_builder.constants import GRAIN_META


class SignificantSegmentMockGenerator(MockGeneratorBase):
    """Mock generator for Significant Segments stories"""

    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.SIGNIFICANT_SEGMENTS
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    # Default dimension values for mock segments
    DEFAULT_DIMENSION_VALUES = {
        "Region": ["North America", "Europe", "Asia", "South America", "Africa", "Australia"],
        "Segment": ["Enterprise", "Mid-Market", "SMB", "Startup"],
        "Owner Id": ["Team A", "Team B", "Team C", "Team D", "Team E"],
        "Enterprise Lifecycle Stage": ["Prospect", "Customer", "Churned", "Renewal"],
        "Enterprise Lifecycle Status": ["Active", "Inactive", "Pending", "Completed"],
    }

    # Default dimensions if none provided
    DEFAULT_DIMENSIONS = [
        {"dimension_id": "region", "label": "Region"},
        {"dimension_id": "segment", "label": "Segment"},
    ]

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock significant segments stories"""
        if story_date:
            self.data_service.story_date = story_date

        # Get dimensions from metric or use defaults
        dimensions = metric.get("dimensions", self.DEFAULT_DIMENSIONS)

        # Generate mock segment data
        segment_data = self._generate_mock_segment_data(dimensions)

        # Sort segments by value
        sorted_segments = sorted(segment_data, key=lambda x: x["value"], reverse=True)

        # Calculate average value
        avg_value = round(sum(segment["value"] for segment in sorted_segments) / len(sorted_segments))

        # Generate stories for top and bottom segments
        story_types = [StoryType.TOP_4_SEGMENTS, StoryType.BOTTOM_4_SEGMENTS]
        stories = []

        for story_type in story_types:
            # Select segments based on story type
            if story_type == StoryType.TOP_4_SEGMENTS:
                selected_segments = sorted_segments[: min(4, len(sorted_segments))]
            else:  # BOTTOM_4_SEGMENTS
                selected_segments = sorted_segments[-min(4, len(sorted_segments)) :]

            # Generate time series and variables
            time_series = self.get_mock_time_series(grain, story_type, selected_segments)
            variables = self.get_mock_variables(metric, story_type, grain, {"average": avg_value})

            # Create story
            story = self.prepare_story_dict(
                metric,
                story_type,
                grain,
                time_series,
                variables,
                story_date or self.data_service.story_date,
            )
            stories.append(story)

        return stories

    def get_mock_time_series(
        self, grain: Granularity, story_type: StoryType, segments: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock time series data for significant segments stories"""
        # Get date range and dates
        start_date, end_date = self.data_service.get_input_time_range(grain, self.group)
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Use the last date for the current values
        current_date = formatted_dates[-1]

        # Create time series with segment values - match the structure from the real implementation
        time_series = []
        for segment in segments:  # type: ignore
            time_series.append(
                {
                    "date": current_date,
                    "value": segment["value"],
                    "dimension": segment["dimension"],
                    "dimension_id": segment["dimension_id"],
                    "dimension_label": segment["dimension_label"],
                    "member": segment["slice"],
                    "slice": segment["slice"],
                    "rank": segment["rank"],
                }
            )

        return time_series

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        extra_data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Generate mock variables for significant segments stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Create variables dict with guaranteed average value
        return {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "average": extra_data.get("average", 500) if extra_data else 500,  # type: ignore
        }

    def _generate_mock_segment_data(self, dimensions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Generate mock data for segments with guaranteed non-zero values"""
        segment_data = []
        rank = 1

        # Generate segments for each dimension
        for dimension in dimensions:
            dimension_id = dimension.get("dimension_id", "")
            dimension_label = dimension.get("label", dimension_id)

            # Get possible values for this dimension
            values = self.DEFAULT_DIMENSION_VALUES.get(dimension_label, ["Value 1", "Value 2", "Value 3", "Value 4"])

            # Generate segments for each value
            for value in values:
                # Generate a random value between 100 and 1000 (avoiding zeros)
                segment_value = random.randint(100, 1000)  # noqa

                segment_data.append(
                    {
                        "dimension": dimension_label,
                        "dimension_id": dimension_id,
                        "dimension_label": dimension_label,
                        "slice": value,
                        "member": value,  # Add member field to match real implementation
                        "value": segment_value,
                        "rank": rank,
                    }
                )
                rank += 1

        return segment_data
