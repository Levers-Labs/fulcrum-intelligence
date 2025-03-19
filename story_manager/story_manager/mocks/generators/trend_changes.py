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

        # Generate base values with more appropriate ranges
        base_value = random.uniform(100, 200)  # noqa

        # Randomize initial trend direction for more variety
        initial_direction = random.choice([-1, 0, 1])  # -1: down, 0: flat, 1: up  # noqa
        initial_slope = initial_direction * random.uniform(0.001, 0.003)  # noqa

        # Generate time series based on story type
        if story_type == StoryType.STABLE_TREND:
            values, central_lines, trend_signals = self._generate_stable_trend(num_points, base_value, initial_slope)
        elif story_type == StoryType.NEW_UPWARD_TREND:
            values, central_lines, trend_signals = self._generate_upward_trend(num_points, base_value, initial_slope)
        elif story_type == StoryType.NEW_DOWNWARD_TREND:
            values, central_lines, trend_signals = self._generate_downward_trend(num_points, base_value, initial_slope)
        else:  # PERFORMANCE_PLATEAU
            values, central_lines, trend_signals = self._generate_plateau_trend(num_points, base_value, initial_slope)

        # Calculate slopes between consecutive central line points
        slopes = [0.0]  # First point has no slope
        for i in range(1, len(central_lines)):
            slope = (central_lines[i] - central_lines[i - 1]) / central_lines[i - 1]
            slopes.append(slope)

        # Create time series
        time_series = []
        for i, (series_date, value, central_line, slope, is_trend_signal) in enumerate(
            zip(formatted_dates, values, central_lines, slopes, trend_signals)
        ):
            time_series.append(
                {
                    "date": series_date,
                    "value": round(value, 2),
                    "central_line": round(central_line, 2),
                    "ucl": round(central_line * (1 + initial_slope), 2),
                    "lcl": round(central_line * (1 - initial_slope), 2),
                    "slope": slope,
                    "slope_change": slopes[i] - slopes[i - 1] if i > 0 else 0,
                    "trend_signal_detected": is_trend_signal,
                    "trend_id": 2 if is_trend_signal else 1,
                }
            )

        return time_series

    def _generate_dramatic_variation(self, base: float, trend_direction: int, position_weight: float = 1.0) -> float:
        """
        Generate more pronounced variations
        """
        # Increase base variations significantly
        base_variation = 0.05 * (1 + position_weight)  # 5% to 10%
        dramatic_variation = 0.15 * (1 + position_weight)  # 15% to 30%

        # Higher chance of dramatic variations
        dramatic_chance = 0.3 + (0.4 * position_weight)  # 30% to 70% chance

        if random.random() < dramatic_chance:  # noqa
            variation_factor = random.uniform(base_variation, dramatic_variation)  # noqa
        else:
            variation_factor = random.uniform(0.03, base_variation)  # noqa

        if trend_direction > 0:  # Upward trend
            # Ensure more pronounced upward variations
            return random.uniform(variation_factor * 0.8, variation_factor * 1.2) * base  # noqa
        elif trend_direction < 0:  # Downward trend
            # Ensure more pronounced downward variations
            return random.uniform(-variation_factor * 1.2, -variation_factor * 0.8) * base  # noqa
        else:  # Stable trend
            return random.uniform(-variation_factor / 2, variation_factor / 2) * base  # noqa

    def _generate_stable_variation(self, base: float, position_weight: float = 1.0) -> float:
        """Generate smaller, more controlled variations for stable trend"""
        # Much smaller variations for stable trend
        base_variation = 0.015 * (1 + position_weight * 0.5)  # 1.5% to 2.25%
        max_variation = 0.025 * (1 + position_weight * 0.5)  # 2.5% to 3.75%

        # Lower chance of larger variations
        if random.random() < 0.15:  # Only 15% chance of larger variation  # noqa
            variation_factor = random.uniform(base_variation, max_variation)  # noqa
        else:
            variation_factor = random.uniform(0.005, base_variation)  # noqa

        return random.uniform(-variation_factor, variation_factor) * base  # noqa

    def _generate_stable_trend(
        self, num_points: int, base_value: float, initial_slope: float
    ) -> tuple[list[float], list[float], list[bool]]:
        """Generate a stable trend pattern with minimal variations"""
        # Very small consistent slope
        slope = 0.0072 + initial_slope  # 0.72% base slope

        # Generate central lines
        central_lines = [base_value * (1 + slope * i) for i in range(num_points)]

        # Generate values with smaller, more controlled variations
        values = []
        for i, cl in enumerate(central_lines):
            position_weight = i / num_points
            noise = self._generate_stable_variation(cl, position_weight)

            # Ensure values stay closer to center line
            max_deviation = cl * 0.03  # Maximum 3% deviation from center line
            noise = max(min(noise, max_deviation), -max_deviation)

            values.append(cl + noise)

        trend_signals = [False] * num_points
        return values, central_lines, trend_signals

    def _generate_upward_trend(
        self, num_points: int, base_value: float, initial_slope: float
    ) -> tuple[list[float], list[float], list[bool]]:
        """Generate a new upward trend pattern with more pronounced growth"""
        transition_point = num_points - 7

        # Much steeper slopes
        prior_slope = 0.0057 + initial_slope  # 0.57% base growth
        new_slope = 0.0267  # 2.67% new growth - significantly higher

        # Generate central lines
        central_lines = []
        values: list = []
        consecutive_above = 0

        for i in range(num_points):
            if i < transition_point:
                cl = base_value * (1 + prior_slope * i)
            else:
                # More pronounced upward shift at transition
                if i == transition_point:
                    base_value = values[i - 1]  # Use last value as new base
                cl = base_value * (1 + new_slope * (i - transition_point + 1))
            central_lines.append(cl)

            # Position weight for scaling variations
            position_weight = i / num_points

            if i < transition_point:
                # Normal variation for old trend
                noise = self._generate_dramatic_variation(cl, 0, position_weight)
            else:
                # More aggressive variations for new trend
                new_trend_position = (i - transition_point) / (num_points - transition_point)

                if consecutive_above < 7:
                    # Force point above center line with larger variation
                    noise = self._generate_dramatic_variation(cl, 1, new_trend_position) * 1.2
                    consecutive_above += 1
                else:
                    if random.random() < 0.1:  # Only 10% chance of dip  # noqa
                        noise = self._generate_dramatic_variation(cl, -1, new_trend_position) * 0.3
                        consecutive_above = 0
                    else:
                        # Even more pronounced upward variation
                        noise = self._generate_dramatic_variation(cl, 1, new_trend_position) * 1.8
                        consecutive_above += 1

            values.append(cl + noise)

        trend_signals = [False] * transition_point + [True] * (num_points - transition_point)
        return values, central_lines, trend_signals

    def _generate_downward_trend(
        self, num_points: int, base_value: float, initial_slope: float
    ) -> tuple[list[float], list[float], list[bool]]:
        """Generate a new downward trend pattern with more pronounced decline"""
        transition_point = num_points - 7

        # Steeper slopes
        prior_slope = 0.0052 + initial_slope  # 0.52% base growth
        new_slope = -0.0132  # -1.32% new growth - more pronounced decline

        # Generate central lines
        central_lines = []
        values: list = []
        consecutive_below = 0

        for i in range(num_points):
            if i < transition_point:
                cl = base_value * (1 + prior_slope * i)
            else:
                # More pronounced downward shift at transition
                if i == transition_point:
                    base_value = values[i - 1]  # Use last value as new base
                cl = base_value * (1 + new_slope * (i - transition_point + 1))
            central_lines.append(cl)

            # Position weight for scaling variations
            position_weight = i / num_points

            if i < transition_point:
                # Normal variation for old trend
                noise = self._generate_dramatic_variation(cl, 0, position_weight)
            else:
                # More aggressive variations for new trend
                new_trend_position = (i - transition_point) / (num_points - transition_point)

                if consecutive_below < 7:
                    # Force point below center line with larger variation
                    noise = self._generate_dramatic_variation(cl, -1, new_trend_position) * 1.2
                    consecutive_below += 1
                else:
                    if random.random() < 0.1:  # Only 10% chance of spike  # noqa
                        noise = self._generate_dramatic_variation(cl, 1, new_trend_position) * 0.3
                        consecutive_below = 0
                    else:
                        # Even more pronounced downward variation
                        noise = self._generate_dramatic_variation(cl, -1, new_trend_position) * 1.8
                        consecutive_below += 1

            values.append(cl + noise)

        trend_signals = [False] * transition_point + [True] * (num_points - transition_point)
        return values, central_lines, trend_signals

    def _generate_plateau_trend(
        self, num_points: int, base_value: float, initial_slope: float
    ) -> tuple[list[float], list[float], list[bool]]:
        """Generate a performance plateau pattern"""
        transition_point = num_points - 7

        # Define slopes
        prior_slope = 0.0129  # 1.29% daily growth
        plateau_slope = 0.0005  # Nearly flat

        # Generate central lines and values
        central_lines: list = []
        values: list = []

        for i in range(num_points):
            if i < transition_point:
                # Before plateau: normal growth
                cl = base_value * (1 + prior_slope * i)
                # Normal variations before plateau
                position_weight = i / transition_point
                noise = self._generate_dramatic_variation(cl, 0, position_weight)
            else:
                # Plateau phase
                if i == transition_point:
                    # Use last value as base for plateau
                    plateau_base = values[i - 1]
                    cl = plateau_base
                else:
                    # Very slight slope during plateau
                    cl = central_lines[i - 1] * (1 + plateau_slope)

                # Much smaller variations during plateau
                plateau_variation = 0.01  # 1% maximum variation
                noise = random.uniform(-plateau_variation, plateau_variation) * cl  # noqa

                # Occasionally add slightly larger variation (but still controlled)
                if random.random() < 0.1:  # 10% chance  # noqa
                    noise *= 1.5  # Still only 1.5% maximum

            central_lines.append(cl)
            values.append(cl + noise)

        trend_signals = [False] * transition_point + [True] * (num_points - transition_point)
        return values, central_lines, trend_signals

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
