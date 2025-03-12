import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class GrowthRatesMockGenerator(MockGeneratorBase):
    """Mock generator for Growth Rates stories with more pronounced visual patterns"""

    genre = StoryGenre.GROWTH
    group = StoryGroup.GROWTH_RATES

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock growth rates stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Generate accelerating growth story
        accelerating_series = self.get_mock_time_series(grain, StoryType.ACCELERATING_GROWTH)
        accelerating_vars = self.get_mock_variables(metric, StoryType.ACCELERATING_GROWTH, grain, accelerating_series)
        accelerating_story = self.prepare_story_dict(
            metric,
            StoryType.ACCELERATING_GROWTH,
            grain,
            accelerating_series,
            accelerating_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(accelerating_story)

        # Generate slowing growth story
        slowing_series = self.get_mock_time_series(grain, StoryType.SLOWING_GROWTH)
        slowing_vars = self.get_mock_variables(metric, StoryType.SLOWING_GROWTH, grain, slowing_series)
        slowing_story = self.prepare_story_dict(
            metric,
            StoryType.SLOWING_GROWTH,
            grain,
            slowing_series,
            slowing_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(slowing_story)

        return stories

    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """Generate mock time series data with more pronounced visual patterns"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)
        num_points = len(dates)

        # Generate values with more interesting patterns
        if story_type == StoryType.ACCELERATING_GROWTH:
            values, growth_rates = self._generate_accelerating_pattern(num_points)
        else:  # SLOWING_GROWTH
            values, growth_rates = self._generate_slowing_pattern(num_points)

        # Create time series with values and growth rates
        return [
            {"date": date, "value": round(value), "growth_rate": None if i == 0 else round(growth_rate, 2)}
            for i, (date, value, growth_rate) in enumerate(zip(formatted_dates, values, growth_rates))
        ]

    def _generate_accelerating_pattern(self, num_points: int) -> tuple[list[float], list[float]]:
        """Generate a pattern with more pronounced acceleration and variations"""
        # Start with a base value
        base_value = random.uniform(200, 400)
        values = [base_value]

        # Create distinct segments with different growth patterns
        # Divide the timeline into segments
        segment_count = min(4, num_points // 3)
        if segment_count < 2:
            segment_count = 2

        segments = []
        remaining_points = num_points

        # Create segments of different lengths
        for i in range(segment_count - 1):
            segment_length = max(2, random.randint(2, remaining_points // 2))
            segments.append(segment_length)
            remaining_points -= segment_length

        segments.append(remaining_points)  # Last segment gets remaining points

        # Generate growth rates for each segment
        growth_rates = []
        current_base_growth = random.uniform(1.5, 3.0)

        for i, segment_length in enumerate(segments):
            # Increase base growth rate for each segment
            segment_growth = current_base_growth * (1 + 0.4 * i)

            # Add random variations within the segment
            segment_rates = []
            for j in range(segment_length):
                # Add more variation at segment boundaries
                if j < 2 or j >= segment_length - 2:
                    variation = random.uniform(-1.0, 1.0)
                else:
                    variation = random.uniform(-0.5, 0.5)

                rate = segment_growth + variation
                segment_rates.append(max(0.5, rate))

            growth_rates.extend(segment_rates)

            # Ensure a visible jump between segments
            current_base_growth = segment_growth * random.uniform(1.1, 1.3)

        # Ensure the last few points show clear acceleration
        for i in range(min(3, num_points // 4)):
            if num_points > 3 + i:
                growth_rates[-3 - i] = growth_rates[-3 - i] * random.uniform(1.1, 1.2)

        # Calculate values based on growth rates
        for i in range(1, num_points):
            next_value = values[-1] * (1 + growth_rates[i - 1] / 100)
            values.append(next_value)

        return values, growth_rates

    def _generate_slowing_pattern(self, num_points: int) -> tuple[list[float], list[float]]:
        """Simple approach to generate a pattern with dramatic flattening at the end"""
        # Start with a base value
        base_value = random.uniform(100, 400)
        values = [base_value]

        # Use a simple exponential decay function for growth rates
        # Start with high growth and decay to near-zero
        initial_growth_rate = random.uniform(10, 15)  # Start with 10-15% growth
        final_growth_rate = random.uniform(0.2, 1.0)  # End with 0.2-1% growth

        # Calculate decay factor to reach final rate
        decay_factor = (final_growth_rate / initial_growth_rate) ** (1 / (num_points - 1))

        # Generate growth rates with exponential decay
        growth_rates = []
        current_rate = initial_growth_rate

        for _ in range(num_points):
            # Add some random variation
            rate = current_rate * random.uniform(0.8, 1.2)
            growth_rates.append(rate)

            # Decay the rate for next point
            current_rate = current_rate * decay_factor

        # Make the last few points especially flat
        for i in range(min(3, num_points // 4)):
            growth_rates[-i - 1] = random.uniform(0.1, 0.5)

        # Calculate values based on growth rates
        for i in range(1, num_points):
            next_value = values[-1] * (1 + growth_rates[i] / 100)
            values.append(next_value)

        return values, growth_rates

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Generate mock variables for growth rates stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Get required periods
        periods = STORY_GROUP_TIME_DURATIONS[self.group][grain]["input"]

        # Filter out None growth rates
        valid_growth_rates = [point["growth_rate"] for point in time_series if point["growth_rate"] is not None]

        # Get the current growth rate (latest non-None value)
        current_growth = valid_growth_rates[-1]

        # Calculate average growth rate
        if story_type == StoryType.ACCELERATING_GROWTH:
            # For accelerating, use earlier periods for average to show contrast
            if len(valid_growth_rates) > 5:
                avg_growth = sum(valid_growth_rates[:-3]) / len(valid_growth_rates[:-3])
            else:
                avg_growth = sum(valid_growth_rates[:-1]) / max(1, len(valid_growth_rates) - 1)

            # Ensure current is higher than average
            if current_growth <= avg_growth:
                current_growth = avg_growth * random.uniform(1.1, 1.2)

        else:  # SLOWING_GROWTH
            # For slowing, use earlier periods for average to show contrast
            if len(valid_growth_rates) > 5:
                # Use the first half or first two-thirds for a higher average
                cutoff = len(valid_growth_rates) // 2
                avg_growth = sum(valid_growth_rates[:cutoff]) / cutoff
            else:
                avg_growth = sum(valid_growth_rates[:-1]) / max(1, len(valid_growth_rates) - 1)

            # Ensure current is lower than average
            if current_growth >= avg_growth:
                current_growth = avg_growth * random.uniform(0.4, 0.6)

        # Create variables dict
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "duration": periods,
            "current_growth": round(current_growth, 2),
            "avg_growth": round(avg_growth, 2),
        }

        return variables
