import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Movement,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class TrendChangesMockGenerator(MockGeneratorBase):
    """Mock generator for Trend Changes stories"""

    genre = StoryGenre.TRENDS
    group = StoryGroup.TREND_CHANGES

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock trend changes stories"""
        if story_date:
            self.data_service.story_date = story_date

        # Define story types to generate
        story_types = [
            StoryType.STABLE_TREND,
            StoryType.NEW_UPWARD_TREND,
            StoryType.NEW_DOWNWARD_TREND,
            StoryType.PERFORMANCE_PLATEAU,
        ]

        stories = []

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
        """Generate mock time series data for trend changes stories"""
        # Get date range and dates
        start_date, end_date = self.data_service.get_input_time_range(grain, self.group)
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)
        num_points = len(dates)

        # Generate base values
        base_value = random.uniform(400, 800)
        central_line = base_value
        ucl = central_line * 1.3  # Upper control limit
        lcl = central_line * 0.7  # Lower control limit

        # Generate time series based on story type
        if story_type == StoryType.STABLE_TREND:
            values, slopes, trend_signals = self._generate_stable_trend(num_points, base_value)
        elif story_type == StoryType.NEW_UPWARD_TREND:
            values, slopes, trend_signals = self._generate_upward_trend(num_points, base_value)
        elif story_type == StoryType.NEW_DOWNWARD_TREND:
            values, slopes, trend_signals = self._generate_downward_trend(num_points, base_value)
        else:  # PERFORMANCE_PLATEAU
            values, slopes, trend_signals = self._generate_plateau_trend(num_points, base_value)

        # Calculate slope changes
        slope_changes = [0]
        for i in range(1, len(slopes)):
            slope_changes.append(slopes[i] - slopes[i - 1])  # type: ignore

        # Create time series with control limits
        time_series = []
        for i, (date, value, slope, slope_change, is_trend_signal) in enumerate(
            zip(formatted_dates, values, slopes, slope_changes, trend_signals)
        ):
            time_series.append(
                {
                    "date": date,
                    "value": value,
                    "central_line": round(central_line + i * slope * central_line),
                    "ucl": round(ucl + i * slope * ucl),
                    "lcl": round(lcl + i * slope * lcl),
                    "slope": slope,
                    "slope_change": slope_change,
                    "trend_signal_detected": is_trend_signal,
                    "trend_id": 2 if is_trend_signal else 1,
                }
            )

        return time_series

    def _generate_stable_trend(self, num_points: int, base_value: float) -> tuple[list[int], list[float], list[bool]]:
        """Generate a stable trend pattern"""
        values = []
        slope = random.uniform(0.03, 0.08)  # type: ignore

        # Generate values with stable trend and noise
        for i in range(num_points):
            trend = i * slope * base_value
            noise = random.uniform(-0.05, 0.05) * base_value
            value = base_value + trend + noise
            values.append(round(value))

        # No trend signals detected for stable trend
        trend_signals = [False] * num_points
        slopes = [slope] * num_points

        return values, slopes, trend_signals

    def _generate_upward_trend(self, num_points: int, base_value: float) -> tuple[list[int], list[float], list[bool]]:
        """Generate a new upward trend pattern"""
        values = []
        transition_point = num_points - 7  # Transition point between trends

        # Define slopes for prior and new trend (increased growth rates)
        prior_slope = random.uniform(0.04, 0.06)  # Increased to 4-6% growth
        new_slope = random.uniform(0.08, 0.15)  # Significantly higher 8-15% growth

        # Generate values with two different trends
        for i in range(num_points):
            if i < transition_point:
                # Prior trend
                trend = i * prior_slope * base_value
                slope = prior_slope
            else:
                # New upward trend
                prior_trend_value = (transition_point - 1) * prior_slope * base_value
                new_trend_offset = (i - transition_point) * new_slope * base_value
                trend = prior_trend_value + new_trend_offset
                slope = new_slope

            noise = random.uniform(-0.05, 0.05) * base_value
            value = base_value + trend + noise
            values.append(round(value))

        # Set trend signals and slopes
        trend_signals = [False] * transition_point + [True] * (num_points - transition_point)
        slopes = [prior_slope] * transition_point + [new_slope] * (num_points - transition_point)

        return values, slopes, trend_signals

    def _generate_downward_trend(self, num_points: int, base_value: float) -> tuple[list[int], list[float], list[bool]]:
        """Generate a new downward trend pattern"""
        values = []
        transition_point = num_points - 7  # Transition point between trends

        # Define slopes for prior and new trend (more pronounced)
        prior_slope = random.uniform(0.04, 0.06)  # Increased to 4-6% growth
        new_slope = random.uniform(-0.10, -0.05)  # More dramatic -5 to -10% decline

        # Generate values with two different trends
        for i in range(num_points):
            if i < transition_point:
                # Prior trend
                trend = i * prior_slope * base_value
                slope = prior_slope
            else:
                # New downward trend
                prior_trend_value = (transition_point - 1) * prior_slope * base_value
                new_trend_offset = (i - transition_point) * new_slope * base_value
                trend = prior_trend_value + new_trend_offset
                slope = new_slope

            noise = random.uniform(-0.05, 0.05) * base_value
            value = max(10, base_value + trend + noise)  # Ensure value is at least 10
            values.append(round(value))

        # Set trend signals and slopes
        trend_signals = [False] * transition_point + [True] * (num_points - transition_point)
        slopes = [prior_slope] * transition_point + [new_slope] * (num_points - transition_point)

        return values, slopes, trend_signals

    def _generate_plateau_trend(self, num_points: int, base_value: float) -> tuple[list[int], list[float], list[bool]]:
        """Generate a performance plateau pattern"""
        values = []
        transition_point = num_points - 7  # Transition point between trends

        # Define slopes for prior and plateau trend (more dramatic contrast)
        prior_slope = random.uniform(0.06, 0.10)  # Increased to 6-10% growth
        plateau_slope = random.uniform(0.005, 0.02)  # Increased slightly to 0.5-2% growth

        # Generate values with two different trends
        for i in range(num_points):
            if i < transition_point:
                # Prior trend
                trend = i * prior_slope * base_value
                slope = prior_slope
            else:
                # Plateau trend
                prior_trend_value = (transition_point - 1) * prior_slope * base_value
                new_trend_offset = (i - transition_point) * plateau_slope * base_value
                trend = prior_trend_value + new_trend_offset
                slope = plateau_slope

            noise = random.uniform(-0.03, 0.03) * base_value
            value = base_value + trend + noise
            values.append(round(value))

        # Set trend signals and slopes
        trend_signals = [False] * transition_point + [True] * (num_points - transition_point)
        slopes = [prior_slope] * transition_point + [plateau_slope] * (num_points - transition_point)

        return values, slopes, trend_signals

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Generate mock variables for trend changes stories"""
        # Get grain metadata and required periods
        grain_meta = GRAIN_META[grain]
        periods = STORY_GROUP_TIME_DURATIONS[self.group][grain]["output"]

        # Get the output period data
        output_data = time_series[-periods:]  # type: ignore

        # Base variables for all story types
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
        }

        # Add story-specific variables
        if story_type == StoryType.STABLE_TREND:
            variables.update(self._get_stable_trend_variables(output_data, periods))
        elif story_type in [StoryType.NEW_UPWARD_TREND, StoryType.NEW_DOWNWARD_TREND]:
            variables.update(self._get_trend_change_variables(time_series))  # type: ignore
        elif story_type == StoryType.PERFORMANCE_PLATEAU:
            variables.update(self._get_plateau_variables(time_series))  # type: ignore

        return variables

    def _get_stable_trend_variables(self, output_data: list[dict[str, Any]], periods: int) -> dict[str, Any]:
        """Get variables for stable trend story"""
        values = [point["value"] for point in output_data]
        avg_growth = self._calculate_average_growth(values)
        movement = Movement.INCREASE.value if avg_growth > 0 else Movement.DECREASE.value

        return {"avg_growth": round(abs(avg_growth), 2), "trend_duration": periods, "movement": movement}

    def _get_trend_change_variables(self, time_series: list[dict[str, Any]]) -> dict[str, Any]:
        """Get variables for upward/downward trend stories"""
        current_trend = [point for point in time_series if point["trend_id"] == 2]
        previous_trend = [point for point in time_series if point["trend_id"] == 1]

        current_values = [point["value"] for point in current_trend]
        previous_values = [point["value"] for point in previous_trend]

        current_avg_growth = self._calculate_average_growth(current_values)
        previous_avg_growth = self._calculate_average_growth(previous_values)

        return {
            "current_avg_growth": round(abs(current_avg_growth), 2),
            "previous_avg_growth": round(abs(previous_avg_growth), 2),
            "previous_trend_duration": len(previous_trend),
            "trend_start_date": current_trend[0]["date"],
        }

    def _get_plateau_variables(self, time_series: list[dict[str, Any]]) -> dict[str, Any]:
        """Get variables for performance plateau story"""
        current_trend = [point for point in time_series if point["trend_id"] == 2]
        current_values = [point["value"] for point in current_trend]

        avg_value = sum(current_values) / len(current_values)
        current_avg_growth = self._calculate_average_growth(current_values)

        return {
            "avg_value": round(avg_value),
            "current_avg_growth": round(abs(current_avg_growth), 2),
            "trend_start_date": current_trend[0]["date"],
        }

    def _calculate_average_growth(self, values: list[float]) -> float:
        """Calculate average growth rate for a series of values"""
        if len(values) < 2:
            return 0

        growth_rates = []
        for i in range(1, len(values)):
            if values[i - 1] == 0:
                continue
            growth_rate = ((values[i] - values[i - 1]) / values[i - 1]) * 100
            growth_rates.append(growth_rate)

        if not growth_rates:
            return 0

        return sum(growth_rates) / len(growth_rates)
