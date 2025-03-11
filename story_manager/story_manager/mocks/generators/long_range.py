import random
from datetime import date
from typing import Any, Dict, List

import pandas as pd

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.mocks.services.data_service import MockDataService
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class LongRangeMockGenerator(MockGeneratorBase):
    """Mock generator for Long Range stories"""

    genre = StoryGenre.TRENDS
    group = StoryGroup.LONG_RANGE
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    def __init__(self, mock_data_service: MockDataService):
        self.data_service = mock_data_service

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock long range stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Generate improving performance story
        improving_series = self.get_mock_time_series(grain, StoryType.IMPROVING_PERFORMANCE)
        improving_vars = self.get_mock_variables(metric, StoryType.IMPROVING_PERFORMANCE, grain, improving_series)
        improving_story = self.prepare_story_dict(
            metric,
            StoryType.IMPROVING_PERFORMANCE,
            grain,
            improving_series,
            improving_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(improving_story)

        # Generate worsening performance story
        worsening_series = self.get_mock_time_series(grain, StoryType.WORSENING_PERFORMANCE)
        worsening_vars = self.get_mock_variables(metric, StoryType.WORSENING_PERFORMANCE, grain, worsening_series)
        worsening_story = self.prepare_story_dict(
            metric,
            StoryType.WORSENING_PERFORMANCE,
            grain,
            worsening_series,
            worsening_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(worsening_story)

        return stories

    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """Generate mock time series data for long range stories"""

        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Generate values based on story type
        values = []

        base_value = random.uniform(200, 3000)
        current_value = base_value

        for i in range(len(dates)):
            # Add trend component (negative for worsening)
            trend_strength = random.uniform(0.03, 0.07)
            if story_type == StoryType.IMPROVING_PERFORMANCE:
                # Generate a improving trend
                trend = current_value * trend_strength
            else:
                # Generate a worsening trend
                trend = -current_value * trend_strength

            # Add random noise
            volatility = random.uniform(0.05, 0.15)
            noise = random.uniform(-volatility, volatility) * current_value

            # Calculate new value
            current_value = max(1, current_value + trend + noise)
            values.append(round(current_value))

        # Create time series
        return [{"date": date, "value": value} for date, value in zip(formatted_dates, values)]

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Generate mock variables for long range stories"""

        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Get required periods based on grain from constants
        periods = STORY_GROUP_TIME_DURATIONS[self.group][grain]["input"]
        time_series = time_series[-periods:]

        metrics = self.calculate_metrics_from_series(time_series)
        avg_growth = abs(metrics["avg_growth"])
        overall_growth = abs(metrics["overall_growth"])
        start_date = time_series[0]["date"]

        # Create variables dict
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "duration": periods,
            "avg_growth": round(avg_growth, 2),
            "overall_growth": round(overall_growth, 2),
            "start_date": start_date,
        }

        return variables
