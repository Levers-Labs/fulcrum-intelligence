import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.story_builder.constants import GRAIN_META


class SegmentDriftMockGenerator(MockGeneratorBase):
    """Mock generator for Segment Drift stories"""

    genre = StoryGenre.ROOT_CAUSES
    group = StoryGroup.SEGMENT_DRIFT

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock segment drift stories with randomized data"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Create fixed segments for each story type, even if no dimensions exist
        segments = self._create_random_segments()

        # Generate stories for each segment and story type
        for segment in segments:
            story_type = segment["story_type"]

            # Get time series and variables
            time_series = self.get_mock_time_series(grain, story_type, segment)
            variables = self.get_mock_variables(metric, story_type, grain, segment)

            # Create story
            story = self.prepare_story_dict(
                metric, story_type, grain, time_series, variables, story_date or self.data_service.story_date
            )
            stories.append(story)

        return stories

    def get_mock_time_series(
        self, grain: Granularity, story_type: StoryType, segment: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock time series data for segment drift stories"""
        # Get date range
        start_date, end_date = self.data_service.get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Generate time series with a trend for growing or shrinking segments
        time_series = []
        for i, _ in enumerate(formatted_dates):
            # Apply realistic changes in values over time
            current_value = segment["previous_value"] + (segment["value_trend"] * i)  # type: ignore

            # Ensure variation and prevent zero deviation
            while current_value == segment["previous_value"]:  # type: ignore
                current_value += random.randint(1, 10)  # noqa

            # Calculate other values based on current_value and previous_value
            current_share = segment["previous_share"] + (segment["share_trend"] * i)  # type: ignore
            slice_share_change_percentage = (
                (current_share - segment["previous_share"]) / segment["previous_share"]  # type: ignore
            ) * 100
            deviation = round((current_value - segment["previous_value"]) / segment["previous_value"] * 100, 2)  # type: ignore
            pressure_change = round(abs(deviation) * random.uniform(0.5, 2), 2)  # noqa
            pressure_direction = "upward" if deviation > 0 else "downward"

            # Ensure deviation is not zero for Improving/Worsening segments
            if story_type in [StoryType.IMPROVING_SEGMENT, StoryType.WORSENING_SEGMENT] and deviation == 0:
                deviation = random.choice([-1, 1]) * random.uniform(1, 10)  # noqa

            # Ensure all required fields are populated
            time_series.append(
                {
                    "dimension": segment["dimension"],  # type: ignore
                    "slice": segment["slice"],  # type: ignore
                    "previous_value": segment["previous_value"],  # type: ignore
                    "current_value": current_value,
                    "deviation": deviation,
                    "previous_share": segment["previous_share"],  # type: ignore
                    "current_share": current_share,
                    "slice_share_change_percentage": round(slice_share_change_percentage, 2),
                    "pressure_change": pressure_change,
                    "pressure_direction": pressure_direction,
                    "impact": segment["impact"],  # type: ignore
                    "sort_value": segment["impact"],  # type: ignore
                    "serialized_key": f"{segment['dimension']}:{segment['slice']}",  # type: ignore
                }
            )
        return time_series

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        segment: dict[str, Any] | None = None,
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
            "dimension": segment["dimension"],  # type: ignore
            "slice": segment["slice"],  # type: ignore
            "previous_share": segment["previous_share"],  # type: ignore
            "current_share": segment["current_share"],  # type: ignore
            "slice_share_change_percentage": round(abs(segment["slice_share_change_percentage"]), 2),  # type: ignore
            "pressure_direction": segment["pressure_direction"],  # type: ignore
            "previous_value": segment["previous_value"],  # type: ignore
            "current_value": segment["current_value"],  # type: ignore
            "deviation": segment["deviation"],  # type: ignore
            "pressure_change": segment["pressure_change"],  # type: ignore
            "impact": segment["impact"],  # type: ignore
        }

        return variables

    def _create_random_segments(self) -> list[dict[str, Any]]:
        """Create random segments for each story type, even if no dimensions exist"""
        segments = []

        # GROWING_SEGMENT
        segments.append(self._create_segment(StoryType.GROWING_SEGMENT, value_trend=10, share_trend=1))

        # SHRINKING_SEGMENT
        segments.append(self._create_segment(StoryType.SHRINKING_SEGMENT, value_trend=-10, share_trend=-1))

        # IMPROVING_SEGMENT
        segments.append(self._create_segment(StoryType.IMPROVING_SEGMENT, value_trend=5, share_trend=0))

        # WORSENING_SEGMENT
        segments.append(self._create_segment(StoryType.WORSENING_SEGMENT, value_trend=-5, share_trend=0))

        return segments

    def _create_segment(self, story_type: StoryType, value_trend: int = 0, share_trend: int = 0) -> dict[str, Any]:
        """Helper to create a randomized segment based on story type"""
        dimension = random.choice(["Region", "Segment", "Category"])  # noqa
        slice_value = random.choice(["Asia", "Europe", "North America", "South America", "Enterprise", "SMB"])  # noqa

        previous_share = random.randint(20, 50)  # noqa
        current_share = (
            previous_share + random.randint(1, 10)  # noqa
            if story_type == StoryType.GROWING_SEGMENT
            else previous_share - random.randint(1, 10)  # noqa
        )
        slice_share_change_percentage = ((current_share - previous_share) / previous_share) * 100

        previous_value = random.randint(100, 1000)  # noqa
        current_value = previous_value + (value_trend if value_trend else random.randint(-50, 50))  # noqa
        deviation = round((current_value - previous_value) / previous_value * 100, 2)

        pressure_direction = "upward" if deviation > 0 else "downward"
        pressure_change = round(abs(deviation) * random.uniform(0.5, 2), 2)  # noqa

        impact = round(random.uniform(1, 3), 2)  # noqa

        return {
            "story_type": story_type,
            "dimension": dimension,
            "slice": slice_value,
            "previous_share": previous_share,
            "current_share": current_share,
            "slice_share_change_percentage": round(slice_share_change_percentage, 2),
            "pressure_direction": pressure_direction,
            "previous_value": previous_value,
            "current_value": current_value,
            "deviation": deviation,
            "pressure_change": pressure_change,
            "impact": impact,
            "sort_value": impact,
            "value_trend": value_trend,  # Used for time series generation
            "share_trend": share_trend,  # Used for share change over time
            "serialized_key": f"{dimension}:{slice_value}",
        }
