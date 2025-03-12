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
from story_manager.mocks.services.data_service import MockDataService
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class TrendChangesMockGenerator(MockGeneratorBase):
    """Mock generator for Trend Changes stories"""

    genre = StoryGenre.TRENDS
    group = StoryGroup.TREND_CHANGES

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock trend changes stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Generate stable trend story
        stable_series = self.get_mock_time_series(grain, StoryType.STABLE_TREND)
        stable_vars = self.get_mock_variables(metric, StoryType.STABLE_TREND, grain, stable_series)
        stable_story = self.prepare_story_dict(
            metric,
            StoryType.STABLE_TREND,
            grain,
            stable_series,
            stable_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(stable_story)

        # Generate new upward trend story
        upward_series = self.get_mock_time_series(grain, StoryType.NEW_UPWARD_TREND)
        upward_vars = self.get_mock_variables(metric, StoryType.NEW_UPWARD_TREND, grain, upward_series)
        upward_story = self.prepare_story_dict(
            metric,
            StoryType.NEW_UPWARD_TREND,
            grain,
            upward_series,
            upward_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(upward_story)

        # Generate new downward trend story
        downward_series = self.get_mock_time_series(grain, StoryType.NEW_DOWNWARD_TREND)
        downward_vars = self.get_mock_variables(metric, StoryType.NEW_DOWNWARD_TREND, grain, downward_series)
        downward_story = self.prepare_story_dict(
            metric,
            StoryType.NEW_DOWNWARD_TREND,
            grain,
            downward_series,
            downward_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(downward_story)

        # Generate performance plateau story
        plateau_series = self.get_mock_time_series(grain, StoryType.PERFORMANCE_PLATEAU)
        plateau_vars = self.get_mock_variables(metric, StoryType.PERFORMANCE_PLATEAU, grain, plateau_series)
        plateau_story = self.prepare_story_dict(
            metric,
            StoryType.PERFORMANCE_PLATEAU,
            grain,
            plateau_series,
            plateau_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(plateau_story)

        return stories

    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """Generate mock time series data for trend changes stories"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)
        num_points = len(dates)

        # Generate values based on story type
        values = []
        base_value = random.uniform(400, 800)

        # Generate control limits
        central_line = base_value
        ucl = central_line * 1.3  # Upper control limit 30% above central line
        lcl = central_line * 0.7  # Lower control limit 30% below central line

        # Set trend signal detection based on story type
        if story_type == StoryType.STABLE_TREND:
            # For stable trend, no trend signal detected
            trend_signal_detected = [False] * num_points

            # Generate values with a stable trend
            slope = random.uniform(0.01, 0.03)  # Small positive slope
            for i in range(num_points):
                # Generate values that follow a stable trend with small noise
                trend = i * slope * base_value
                noise = random.uniform(-0.05, 0.05) * base_value
                value = base_value + trend + noise
                values.append(round(value))

            # Set slopes for all points
            slopes = [slope] * num_points

        elif story_type == StoryType.NEW_UPWARD_TREND:
            # For new upward trend, trend signal detected in recent points
            trend_signal_detected = [False] * (num_points - 7) + [True] * 7

            # Generate two trend segments
            prior_slope = random.uniform(0.01, 0.02)  # Small positive slope for prior trend
            latest_slope = random.uniform(0.03, 0.05)  # Larger positive slope for new trend

            # Transition point between trends
            transition_point = num_points - 7

            for i in range(num_points):
                if i < transition_point:
                    # Prior trend
                    trend = i * prior_slope * base_value
                    slope = prior_slope
                else:
                    # New upward trend
                    prior_trend_value = (transition_point - 1) * prior_slope * base_value
                    new_trend_offset = (i - transition_point) * latest_slope * base_value
                    trend = prior_trend_value + new_trend_offset
                    slope = latest_slope

                noise = random.uniform(-0.05, 0.05) * base_value
                value = base_value + trend + noise
                values.append(round(value))

            # Set slopes for all points
            slopes = [prior_slope] * transition_point + [latest_slope] * (num_points - transition_point)

        elif story_type == StoryType.NEW_DOWNWARD_TREND:
            # For new downward trend, trend signal detected in recent points
            trend_signal_detected = [False] * (num_points - 7) + [True] * 7

            # Generate two trend segments
            prior_slope = random.uniform(0.01, 0.03)  # Positive slope for prior trend
            latest_slope = random.uniform(-0.03, -0.01)  # Negative slope for new trend

            # Transition point between trends
            transition_point = num_points - 7

            for i in range(num_points):
                if i < transition_point:
                    # Prior trend
                    trend = i * prior_slope * base_value
                    slope = prior_slope
                else:
                    # New downward trend
                    prior_trend_value = (transition_point - 1) * prior_slope * base_value
                    new_trend_offset = (i - transition_point) * latest_slope * base_value
                    trend = prior_trend_value + new_trend_offset
                    slope = latest_slope

                noise = random.uniform(-0.05, 0.05) * base_value
                value = base_value + trend + noise
                values.append(round(max(10, value)))

            # Set slopes for all points
            slopes = [prior_slope] * transition_point + [latest_slope] * (num_points - transition_point)

        else:  # PERFORMANCE_PLATEAU
            # For performance plateau, trend signal detected in recent points
            trend_signal_detected = [False] * (num_points - 7) + [True] * 7

            # Generate two trend segments
            prior_slope = random.uniform(0.02, 0.04)  # Positive slope for prior trend
            latest_slope = random.uniform(0.001, 0.009)  # Very small slope (<1%) for plateau

            # Transition point between trends
            transition_point = num_points - 7

            for i in range(num_points):
                if i < transition_point:
                    # Prior trend
                    trend = i * prior_slope * base_value
                    slope = prior_slope
                else:
                    # Plateau trend
                    prior_trend_value = (transition_point - 1) * prior_slope * base_value
                    new_trend_offset = (i - transition_point) * latest_slope * base_value
                    trend = prior_trend_value + new_trend_offset
                    slope = latest_slope

                noise = random.uniform(-0.03, 0.03) * base_value
                value = base_value + trend + noise
                values.append(round(value))

            # Set slopes for all points
            slopes = [prior_slope] * transition_point + [latest_slope] * (num_points - transition_point)

        # Calculate slope changes
        slope_changes = [0]
        for i in range(1, len(slopes)):
            slope_changes.append(slopes[i] - slopes[i - 1])

        # Create time series with process control data
        time_series = []
        for i, (date, value) in enumerate(zip(formatted_dates, values)):
            # For all points, add process control data
            point = {
                "date": date,
                "value": value,
                "central_line": round(central_line + i * slopes[i] * central_line),
                "ucl": round(ucl + i * slopes[i] * ucl),
                "lcl": round(lcl + i * slopes[i] * lcl),
                "slope": slopes[i],
                "slope_change": slope_changes[i],
                "trend_signal_detected": trend_signal_detected[i],
                "trend_id": 1 if not trend_signal_detected[i] else 2,  # Add trend_id for trend changes
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
        """Generate mock variables for trend changes stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Get required periods
        periods = STORY_GROUP_TIME_DURATIONS[self.group][grain]["output"]

        # Get the output period data
        output_data = time_series[-periods:]

        # Base variables for all story types
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
        }

        if story_type == StoryType.STABLE_TREND:
            # Calculate average growth for stable trend
            values = [point["value"] for point in output_data]
            avg_growth = self._calculate_average_growth(values)

            # Determine movement direction
            movement = Movement.INCREASE.value if avg_growth > 0 else Movement.DECREASE.value

            # Add stable trend variables
            variables.update({"avg_growth": round(abs(avg_growth), 2), "trend_duration": periods, "movement": movement})

        elif story_type in [StoryType.NEW_UPWARD_TREND, StoryType.NEW_DOWNWARD_TREND]:
            # Get current and previous trend data
            current_trend = [point for point in time_series if point["trend_id"] == 2]
            previous_trend = [point for point in time_series if point["trend_id"] == 1]

            # Calculate average growth for current and previous trends
            current_values = [point["value"] for point in current_trend]
            previous_values = [point["value"] for point in previous_trend]

            current_avg_growth = self._calculate_average_growth(current_values)
            previous_avg_growth = self._calculate_average_growth(previous_values)

            # Get trend start date
            trend_start_date = current_trend[0]["date"]

            # Add trend change variables
            variables.update(
                {
                    "current_avg_growth": round(abs(current_avg_growth), 2),
                    "previous_avg_growth": round(abs(previous_avg_growth), 2),
                    "previous_trend_duration": len(previous_trend),
                    "trend_start_date": trend_start_date,
                }
            )

        elif story_type == StoryType.PERFORMANCE_PLATEAU:
            # Get current trend data
            current_trend = [point for point in time_series if point["trend_id"] == 2]

            # Calculate average value and growth for plateau
            current_values = [point["value"] for point in current_trend]
            avg_value = sum(current_values) / len(current_values)
            current_avg_growth = self._calculate_average_growth(current_values)

            # Get trend start date
            trend_start_date = current_trend[0]["date"]

            # Add plateau variables
            variables.update(
                {
                    "avg_value": round(avg_value),
                    "current_avg_growth": round(abs(current_avg_growth), 2),
                    "trend_start_date": trend_start_date,
                }
            )

        return variables

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
