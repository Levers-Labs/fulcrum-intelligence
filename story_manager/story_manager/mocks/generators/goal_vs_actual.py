import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class GoalVsActualMockGenerator(MockGeneratorBase):
    """Mock generator for Goal vs Actual stories"""

    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.GOAL_VS_ACTUAL

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock goal vs actual stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Generate on track story
        on_track_series = self.get_mock_time_series(grain, StoryType.ON_TRACK)
        on_track_vars = self.get_mock_variables(metric, StoryType.ON_TRACK, grain, on_track_series)
        on_track_story = self.prepare_story_dict(
            metric,
            StoryType.ON_TRACK,
            grain,
            on_track_series,
            on_track_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(on_track_story)

        # Generate off track story
        off_track_series = self.get_mock_time_series(grain, StoryType.OFF_TRACK)
        off_track_vars = self.get_mock_variables(metric, StoryType.OFF_TRACK, grain, off_track_series)
        off_track_story = self.prepare_story_dict(
            metric,
            StoryType.OFF_TRACK,
            grain,
            off_track_series,
            off_track_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(off_track_story)

        return stories

    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """Generate mock time series data for goal vs actual stories"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Generate values and targets
        values = []
        targets = []

        # Base values
        base_value = random.uniform(100, 500)
        base_target = base_value * (0.9 if story_type == StoryType.ON_TRACK else 1.1)

        for i in range(len(dates)):
            # Generate value with some randomness
            if story_type == StoryType.ON_TRACK:
                # For ON_TRACK, values should generally be above targets
                value = base_value * (1 + random.uniform(0, 0.2))
                target = base_target * (1 + random.uniform(0, 0.1))
            else:
                # For OFF_TRACK, values should generally be below targets
                value = base_value * (1 - random.uniform(0, 0.2))
                target = base_target * (1 + random.uniform(0, 0.1))

            values.append(round(value))
            targets.append(round(target))

            # Update base for next iteration with some trend
            base_value = value * (1 + random.uniform(-0.05, 0.05))
            base_target = target * (1 + random.uniform(-0.02, 0.02))

        # Create time series with values and targets
        return [
            {
                "date": date,
                "value": value,
                "target": target,
                "target_upper_bound": round(target * 1.1),
                "target_lower_bound": round(target * 0.9),
            }
            for date, value, target in zip(formatted_dates, values, targets)
        ]

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Generate mock variables for goal vs actual stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Get required periods
        periods = STORY_GROUP_TIME_DURATIONS[self.group][grain]["input"]

        # Get the latest data point
        latest_point = time_series[-1]
        current_value = latest_point["value"]
        target = latest_point["target"]

        # Calculate growth if we have at least 2 points
        if len(time_series) > 1:
            previous_value = time_series[-2]["value"]
            current_growth = ((current_value - previous_value) / previous_value) * 100
        else:
            current_growth = 5.0 if story_type == StoryType.ON_TRACK else -5.0

        # Calculate deviation from target
        deviation = abs(((current_value - target) / target) * 100)

        # Determine direction
        direction = "up" if current_growth > 0 else "down"

        # Create variables dict
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "duration": periods,
            "current_value": current_value,
            "target": target,
            "current_growth": round(current_growth, 2),
            "deviation": round(deviation, 2),
            "direction": direction,
        }

        return variables
