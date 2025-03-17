import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class GrowthRatesMockGenerator(MockGeneratorBase):
    """Mock generator for Growth Rates stories"""

    genre = StoryGenre.GROWTH
    group = StoryGroup.GROWTH_RATES

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock growth rates stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Define story types to generate
        story_types = [StoryType.ACCELERATING_GROWTH, StoryType.SLOWING_GROWTH]

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
        """Generate mock time series data for growth rate stories"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)
        num_points = len(dates)

        if story_type == StoryType.ACCELERATING_GROWTH:
            # Generate accelerating growth pattern
            values, growth_rates = self._generate_accelerating_pattern(num_points)
        else:
            # Generate slowing growth pattern
            values, growth_rates = self._generate_slowing_pattern(num_points)

        # Create time series with values and growth rates
        return [
            {"date": date, "value": round(value), "growth_rate": None if i == 0 else round(growth_rate, 2)}
            for i, (date, value, growth_rate) in enumerate(
                zip(formatted_dates, values, [0] + growth_rates)  # Add a placeholder for first point
            )
        ]

    def _generate_accelerating_pattern(self, num_points: int) -> tuple[list[float], list[float]]:
        """Generate an accelerating growth pattern"""
        values = []
        growth_rates = []

        # Start with a base value
        base_value = random.uniform(200, 500)
        values.append(base_value)

        # Start with modest growth, increase over time
        growth = 2.0  # Start with 2% growth

        for i in range(1, num_points):
            # Increase growth rate over time
            if i < num_points // 2:
                # First half: modest increases
                growth += random.uniform(0.2, 0.5)
            else:
                # Second half: more dramatic increases
                growth += random.uniform(0.5, 1.0)

            # Add some variability
            actual_growth = max(0.5, growth * random.uniform(0.9, 1.1))
            growth_rates.append(actual_growth)

            # Calculate next value
            next_value = values[-1] * (1 + actual_growth / 100)
            values.append(next_value)

        return values, growth_rates

    def _generate_slowing_pattern(self, num_points: int) -> tuple[list[float], list[float]]:
        """Generate a slowing growth pattern with visual flattening"""
        values = []
        growth_rates = []

        # Start with a base value
        base_value = random.uniform(200, 500)
        values.append(base_value)

        # Start with high growth, dramatically decrease over time
        initial_growth = 15.0  # Start with 15% growth (higher initial growth)
        growth = initial_growth

        # More dramatic decay toward the end
        for i in range(1, num_points):
            # Calculate position in sequence (0 to 1)
            position = i / (num_points - 1)

            # Apply exponential decay to growth rate
            # This creates a much more dramatic slowdown
            if position < 0.5:
                # First half: gentle decline
                growth = initial_growth * (1 - position * 0.5)
            elif position < 0.8:
                # Next 30%: faster decline
                growth = initial_growth * (1 - 0.25 - (position - 0.5) * 2)
            else:
                # Final 20%: dramatic decline to near-zero or negative
                growth = initial_growth * (1 - 0.85 - (position - 0.8) * 7.5)

            # Add randomness but maintain trend
            actual_growth = growth * random.uniform(0.9, 1.1)

            # Allow slightly negative growth at the very end (creates visual downturn)
            if position > 0.9:
                actual_growth = min(actual_growth, 0.5 - (position - 0.9) * 5)

            growth_rates.append(max(-1.0, actual_growth))  # Cap minimum at -1%

            # Calculate next value
            next_value = values[-1] * (1 + growth_rates[-1] / 100)
            values.append(next_value)

        return values, growth_rates

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Generate mock variables for growth rates stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Get required periods
        periods = STORY_GROUP_TIME_DURATIONS[self.group][grain]["input"]

        # Get valid growth rates (skip the first None value)
        valid_growth_rates = [point["growth_rate"] for point in time_series if point["growth_rate"] is not None]  # type: ignore

        # Get the current growth rate
        current_growth = valid_growth_rates[-1]

        # Calculate average growth rate (excluding the last point)
        if len(valid_growth_rates) > 1:
            avg_growth = sum(valid_growth_rates[:-1]) / (len(valid_growth_rates) - 1)
        else:
            # Fallback if we somehow only have one point
            avg_growth = current_growth * 0.8 if story_type == StoryType.ACCELERATING_GROWTH else current_growth * 1.2

        # Create variables dict
        return {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "duration": periods,
            "current_growth": round(current_growth, 2),
            "avg_growth": round(avg_growth, 2),
        }
