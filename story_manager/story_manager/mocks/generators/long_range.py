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

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock long range stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Define story types to generate
        story_types = [StoryType.IMPROVING_PERFORMANCE, StoryType.WORSENING_PERFORMANCE]

        # Generate stories for each type
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
        """Generate mock time series data for long range stories"""
        # Get date range and dates
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Initialize values
        base_value = random.uniform(200, 3000)
        current_value = base_value
        values = []

        # Determine if trend is positive or negative based on story type
        is_improving = story_type == StoryType.IMPROVING_PERFORMANCE

        # Generate values with trend and noise
        for _ in range(len(dates)):
            # Set trend direction based on story type
            trend_strength = random.uniform(0.03, 0.07)
            trend = current_value * trend_strength * (1 if is_improving else -1)

            # Add random noise
            volatility = random.uniform(0.05, 0.15)
            noise = random.uniform(-volatility, volatility) * current_value

            # Calculate new value (ensure it stays positive)
            current_value = max(1, current_value + trend + noise)
            values.append(round(current_value))

        # Create time series
        return [{"date": date, "value": value} for date, value in zip(formatted_dates, values)]

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Generate mock variables for long range stories"""
        # Get grain metadata and required periods
        grain_meta = GRAIN_META[grain]
        periods = STORY_GROUP_TIME_DURATIONS[self.group][grain]["input"]

        # Get the relevant portion of the time series
        time_series = time_series[-periods:]  # type: ignore

        # Calculate metrics
        metrics = self._calculate_metrics_from_series(time_series)  # type: ignore

        return {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "duration": periods,
            "avg_growth": round(abs(metrics["avg_growth"]), 2),
            "overall_growth": round(abs(metrics["overall_growth"]), 2),
            "start_date": time_series[0]["date"],  # type: ignore
        }

    def _calculate_metrics_from_series(self, time_series: list[dict[str, Any]]) -> dict[str, float]:
        """Calculate metrics like average growth and overall growth from a time series"""
        values = [point["value"] for point in time_series]
        series = pd.Series(values)

        # Calculate average growth
        growth_rates = series.pct_change() * 100
        avg_growth = growth_rates.mean()

        # Calculate overall growth
        initial_value = series.iloc[0]
        final_value = series.iloc[-1]
        overall_growth = ((final_value - initial_value) / initial_value) * 100 if initial_value != 0 else 0

        return {"avg_growth": round(avg_growth, 2), "overall_growth": round(overall_growth, 2)}
