import random
from datetime import date
from typing import Any, Dict, List

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Position,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.mocks.services.data_service import MockDataService
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class TrendExceptionsMockGenerator(MockGeneratorBase):
    """Mock generator for Trend Exceptions stories"""

    genre = StoryGenre.TRENDS
    group = StoryGroup.TREND_EXCEPTIONS
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    def __init__(self, mock_data_service: MockDataService):
        self.data_service = mock_data_service

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock trend exceptions stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Generate spike story
        spike_series = self.get_mock_time_series(grain, StoryType.SPIKE)
        spike_vars = self.get_mock_variables(metric, StoryType.SPIKE, grain, spike_series)
        spike_story = self.prepare_story_dict(
            metric, StoryType.SPIKE, grain, spike_series, spike_vars, story_date or self.data_service.story_date
        )
        stories.append(spike_story)

        # Generate drop story
        drop_series = self.get_mock_time_series(grain, StoryType.DROP)
        drop_vars = self.get_mock_variables(metric, StoryType.DROP, grain, drop_series)
        drop_story = self.prepare_story_dict(
            metric, StoryType.DROP, grain, drop_series, drop_vars, story_date or self.data_service.story_date
        )
        stories.append(drop_story)

        return stories

    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """Generate mock time series data for trend exceptions stories"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Generate values based on story type
        values = []
        base_value = random.uniform(400, 800)

        # Generate control limits
        central_line = base_value
        ucl = central_line * 1.3  # Upper control limit 30% above central line
        lcl = central_line * 0.7  # Lower control limit 30% below central line

        # Generate values that stay within control limits for most points
        for i in range(len(dates) - 1):
            # Generate values that stay close to the central line
            noise = random.uniform(-0.15, 0.15) * central_line
            value = central_line + noise
            values.append(round(value))

        # Set the last value based on story type
        if story_type == StoryType.SPIKE:
            # For spike, set the last value above UCL
            spike_value = ucl * random.uniform(1.1, 1.3)  # 10-30% above UCL
            values.append(round(spike_value))
        else:  # DROP
            # For drop, set the last value below LCL
            drop_value = lcl * random.uniform(0.7, 0.9)  # 10-30% below LCL
            values.append(round(max(10, drop_value)))

        # Create time series with process control data
        time_series = []
        for i, (date, value) in enumerate(zip(formatted_dates, values)):
            # For all points, add process control data
            point = {
                "date": date,
                "value": value,
                "central_line": round(central_line),
                "ucl": round(ucl),
                "lcl": round(lcl),
                "slope": random.uniform(-0.5, 0.5),
                "slope_change": random.uniform(-0.2, 0.2),
                "trend_signal_detected": False,
            }
            time_series.append(point)

        return time_series

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Generate mock variables for trend exceptions stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Get the latest data point
        latest_point = time_series[-1]
        value = latest_point["value"]

        # Calculate deviation based on story type
        if story_type == StoryType.SPIKE:
            # For spike, calculate deviation from UCL
            deviation = ((value - latest_point["ucl"]) / latest_point["ucl"]) * 100
            position = Position.ABOVE.value
        else:  # DROP
            # For drop, calculate deviation from LCL
            deviation = ((latest_point["lcl"] - value) / latest_point["lcl"]) * 100
            position = Position.BELOW.value

        # Create variables dict
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "deviation": round(abs(deviation), 2),
            "position": position,
        }

        return variables
