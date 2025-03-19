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
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock goal vs actual stories"""
        if story_date:
            self.data_service.story_date = story_date

        # Generate both types of stories
        story_types = [StoryType.ON_TRACK, StoryType.OFF_TRACK]
        stories = []

        for story_type in story_types:
            time_series = self.get_mock_time_series(grain, story_type)
            variables = self.get_mock_variables(metric, story_type, grain, time_series)

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

    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """Generate mock time series data for goal vs actual stories"""
        # Get date range and dates
        start_date, end_date = self.data_service.get_input_time_range(grain, self.group)
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Initialize base values
        base_value = random.uniform(100, 500)  # noqa
        base_target = base_value * random.uniform(0.95, 1.05)  # noqa

        values = []
        targets = []
        statuses = []

        # Generate data points - mixed statuses but ensure last point matches story_type
        for i, _ in enumerate(dates):
            # Generate target with some randomness
            target = base_target * (1 + random.uniform(-0.05, 0.05))  # noqa

            # Randomly determine if this point is on track or off track
            # For the last point, ensure it matches the requested story type
            if i == len(dates) - 1:  # Last point
                is_on_track = story_type == StoryType.ON_TRACK
            else:
                is_on_track = random.choice([True, False])  # noqa

            # Set value based on on-track status
            if is_on_track:
                value = target * (1 + random.uniform(0, 0.2))  # noqa
                status = StoryType.ON_TRACK
            else:
                value = target * (1 - random.uniform(0.05, 0.2))  # noqa
                status = StoryType.OFF_TRACK

            values.append(round(value))
            targets.append(round(target))
            statuses.append(status)

            # Update bases for next point to create some trend
            base_target = target

        # Create time series
        return [
            {
                "date": date,
                "value": value,
                "target": target,
                "target_upper_bound": round(target * 1.1),
                "target_lower_bound": round(target * 0.9),
                "status": status,
            }
            for date, value, target, status in zip(formatted_dates, values, targets, statuses)
        ]

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Generate mock variables for goal vs actual stories"""
        grain_meta = GRAIN_META[grain]
        periods = STORY_GROUP_TIME_DURATIONS[self.group][grain]["input"]

        # Get the latest data point
        latest_point = time_series[-1]  # type: ignore
        current_value = latest_point["value"]
        target = latest_point["target"]

        previous_value = time_series[-2]["value"]  # type: ignore
        current_growth = ((current_value - previous_value) / previous_value) * 100

        # Calculate deviation and direction
        deviation = abs(((current_value - target) / target) * 100)
        direction = "up" if current_growth > 0 else "down"

        return {
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
