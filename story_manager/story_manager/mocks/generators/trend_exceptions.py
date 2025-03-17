import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Position,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.story_builder.constants import GRAIN_META


class TrendExceptionsMockGenerator(MockGeneratorBase):
    """Mock generator for Trend Exceptions stories"""

    genre = StoryGenre.TRENDS
    group = StoryGroup.TREND_EXCEPTIONS

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock trend exceptions stories"""
        if story_date:
            self.data_service.story_date = story_date

        # Define story types to generate
        story_types = [StoryType.SPIKE, StoryType.DROP]
        stories = []

        # Generate a story for each type
        for story_type in story_types:
            time_series = self.get_mock_time_series(grain, story_type)
            variables = self.get_mock_variables(metric, story_type, grain, time_series)

            story = self.prepare_story_dict(
                metric, story_type, grain, time_series, variables, story_date or self.data_service.story_date
            )
            stories.append(story)

        return stories

    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """Generate mock time series data for trend exceptions stories"""
        # Get dates for the time series
        start_date, end_date = self.data_service.get_input_time_range(grain, self.group)
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Generate base value and control limits
        base_value = random.uniform(400, 800)  # type: ignore
        central_line = base_value
        ucl = central_line * 1.3  # Upper control limit
        lcl = central_line * 0.7  # Lower control limit

        # Generate normal values for all but the last point
        values = self._generate_normal_values(central_line, len(dates) - 1)

        # Add the exceptional last value based on story type
        if story_type == StoryType.SPIKE:
            # For spike, set the last value above UCL
            spike_value = ucl * random.uniform(1.1, 1.3)  # type: ignore
            values.append(round(spike_value))
        else:  # DROP
            # For drop, set the last value below LCL
            drop_value = lcl * random.uniform(0.7, 0.9)  # type: ignore
            values.append(round(max(10, drop_value)))  # type: ignore

        # Create time series with process control data
        time_series = []
        for date, value in zip(formatted_dates, values):
            time_series.append(
                {
                    "date": date,
                    "value": value,
                    "central_line": round(central_line),
                    "ucl": round(ucl),
                    "lcl": round(lcl),
                    "slope": random.uniform(-0.5, 0.5),  # type: ignore
                    "slope_change": random.uniform(-0.2, 0.2),  # type: ignore
                    "trend_signal_detected": False,
                }
            )

        return time_series

    def _generate_normal_values(self, central_line: float, count: int) -> list[int]:
        """Generate values that stay within control limits"""
        values = []
        for _ in range(count):
            # Generate values that stay close to the central line
            noise = random.uniform(-0.15, 0.15) * central_line  # type: ignore
            value = central_line + noise
            values.append(round(value))
        return values

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Generate mock variables for trend exceptions stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Get the latest data point
        latest_point = time_series[-1]  # type: ignore
        value = latest_point["value"]

        # Calculate deviation and position based on story type
        if story_type == StoryType.SPIKE:
            # For spike, calculate deviation from UCL
            deviation = ((value - latest_point["ucl"]) / latest_point["ucl"]) * 100
            position = Position.ABOVE.value
        else:  # DROP
            # For drop, calculate deviation from LCL
            deviation = ((latest_point["lcl"] - value) / latest_point["lcl"]) * 100
            position = Position.BELOW.value

        # Return variables dictionary
        return {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "deviation": round(abs(deviation), 2),
            "position": position,
        }
