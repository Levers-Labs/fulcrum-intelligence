"""
Mock Performance Status Pattern Generator

Generates mock MetricPerformance pattern results that simulate performance status analysis.
"""

import random
from datetime import date, timedelta
from typing import Any

import pandas as pd

from commons.models.enums import Granularity
from levers.models.patterns import MetricPerformance
from story_manager.mocks.v2.pattern_generators.base import MockPatternGeneratorBase


class MockPerformanceStatusGenerator(MockPatternGeneratorBase):
    """Generator for mock performance status pattern results."""

    pattern_name = "performance_status"

    def generate_pattern_results(
        self, metric: dict[str, Any], grain: Granularity, story_date: date
    ) -> list[MetricPerformance]:
        """
        Generate a single mock MetricPerformance pattern result.

        Returns only one scenario instead of multiple scenarios to avoid
        generating multiple stories of the same type.
        """
        base_data = self._get_base_pattern_data(metric, grain, story_date)

        # Randomly choose one scenario type to generate
        # WEIGHTED scenario choices to ensure HOLD_STEADY appears more frequently
        scenario_choices = [
            "on_track",
            "off_track",
            "improving_status",
            "worsening_status",
            "hold_steady",
            "hold_steady",  # Add one extra weight to hold_steady for ~33% frequency
        ]

        chosen_scenario = random.choice(scenario_choices)  # noqa

        # Generate the chosen scenario
        if chosen_scenario == "on_track":
            scenario_data = self._generate_on_track_scenario(base_data)
        elif chosen_scenario == "off_track":
            scenario_data = self._generate_off_track_scenario(base_data)
        elif chosen_scenario == "improving_status":
            scenario_data = self._generate_improving_status_scenario(base_data)
        elif chosen_scenario == "worsening_status":
            scenario_data = self._generate_worsening_status_scenario(base_data)
        else:  # hold_steady
            scenario_data = self._generate_hold_steady_scenario(base_data)

        try:
            result = MetricPerformance(**scenario_data)
            return [result]  # Return single result
        except Exception:
            return []

    def generate_mock_series_data(
        self, metric: dict[str, Any], grain: Granularity, story_date: date, num_periods: int = 12
    ) -> pd.DataFrame:
        """
        Generate mock time series data that matches the pattern result.

        Args:
            metric: Metric dictionary
            grain: Granularity for the series
            story_date: The story date
            num_periods: Number of historical periods to generate (default: 12)

        Returns:
            DataFrame with mock time series data with exactly num_periods data points
        """
        # Generate date range - always create exactly num_periods data points
        if grain == Granularity.DAY:
            freq = "D"
            # End date should be BEFORE the story_date (last completed period)
            end_date = story_date - timedelta(days=1)
            # Generate exactly num_periods days
            date_range = pd.date_range(end=end_date, periods=num_periods, freq=freq)
        elif grain == Granularity.WEEK:
            freq = "W-MON"
            # Get the Monday before story_date
            days_since_monday = story_date.weekday()
            end_date = story_date - timedelta(days=days_since_monday + 7)
            # Generate exactly num_periods weeks
            date_range = pd.date_range(end=end_date, periods=num_periods, freq=freq)
        else:  # MONTH
            freq = "MS"
            # Get the first day of the previous month
            if story_date.month == 1:
                end_date = date(story_date.year - 1, 12, 1)
            else:
                end_date = date(story_date.year, story_date.month - 1, 1)
            # Generate exactly num_periods months
            date_range = pd.date_range(end=end_date, periods=num_periods, freq=freq)

        # Choose scenario type for target-aware series generation
        scenario_types = ["on_track", "off_track", "improving", "worsening", "steady"]
        scenario_type = random.choice(scenario_types)  # noqa

        target_value = self._generate_mock_value(900)  # Base target

        values = []
        target_values = []

        for i, _ in enumerate(date_range):
            progress = i / len(date_range)

            # Generate target-aware values based on scenario
            if scenario_type == "on_track":
                # Values should be consistently above target
                value = target_value * random.uniform(1.05, 1.25)  # noqa
                target = target_value * random.uniform(0.95, 1.05)  # noqa
            elif scenario_type == "off_track":
                # Values should be consistently below target
                value = target_value * random.uniform(0.75, 0.95)  # noqa
                target = target_value * random.uniform(1.05, 1.25)  # noqa
            elif scenario_type == "improving":
                # Values start below target, move above
                if progress < 0.5:
                    value = target_value * random.uniform(0.8, 0.95)  # noqa
                else:
                    value = target_value * random.uniform(1.05, 1.2)  # noqa
                target = target_value * random.uniform(0.95, 1.05)  # noqa
            elif scenario_type == "worsening":
                # Values start above target, move below
                if progress < 0.5:
                    value = target_value * random.uniform(1.05, 1.2)  # noqa
                else:
                    value = target_value * random.uniform(0.8, 0.95)  # noqa
                target = target_value * random.uniform(0.95, 1.05)  # noqa
            else:  # steady
                # Values hover around target
                value = target_value * random.uniform(0.95, 1.05)  # noqa
                target = target_value * random.uniform(0.95, 1.05)  # noqa

            # Add some noise
            noise = random.uniform(-0.05, 0.05)  # noqa
            value = value * (1 + noise)

            values.append(max(0, value))  # noqa
            target_values.append(max(0, target))  # noqa

        # Create DataFrame
        series_df = pd.DataFrame({"date": date_range, "value": values, "target_value": target_values})

        return series_df

    def _generate_on_track_scenario(self, base_data: dict[str, Any]) -> dict[str, Any]:
        """Generate an on-track performance scenario."""
        current_value = self._generate_mock_value(1000)
        target_value = current_value * random.uniform(0.8, 0.95)  # noqa
        prior_value = current_value * random.uniform(0.85, 0.98)  # noqa

        return {
            **base_data,
            "current_value": current_value,
            "prior_value": prior_value,
            "absolute_delta_from_prior": current_value - prior_value,
            "pop_change_percent": ((current_value - prior_value) / prior_value) * 100,
            "target_value": target_value,
            "status": "on_track",
            "absolute_over_performance": current_value - target_value,
            "percent_over_performance": ((current_value - target_value) / target_value) * 100,
        }

    def _generate_off_track_scenario(self, base_data: dict[str, Any]) -> dict[str, Any]:
        """Generate an off-track performance scenario."""
        current_value = self._generate_mock_value(800)
        target_value = current_value * random.uniform(1.05, 1.3)  # noqa
        prior_value = current_value * random.uniform(1.02, 1.15)  # noqa

        return {
            **base_data,
            "current_value": current_value,
            "prior_value": prior_value,
            "absolute_delta_from_prior": current_value - prior_value,
            "pop_change_percent": ((current_value - prior_value) / prior_value) * 100,
            "target_value": target_value,
            "status": "off_track",
            "absolute_gap": target_value - current_value,
            "percent_gap": ((target_value - current_value) / target_value) * 100,
        }

    def _generate_status_change_scenario(self, base_data: dict[str, Any]) -> dict[str, Any]:
        """Generate a scenario with status change."""
        current_value = self._generate_mock_value(950)
        target_value = self._generate_mock_value(900)
        prior_value = current_value * random.uniform(0.85, 0.95)  # noqa

        # Random status change
        has_flipped = random.choice([True, False])  # noqa
        if has_flipped:
            old_status = random.choice(["on_track", "off_track"])  # noqa
            new_status = "off_track" if old_status == "on_track" else "on_track"
        else:
            old_status = None
            new_status = self._choose_random_target_status()

        status_change = {
            "has_flipped": has_flipped,
            "old_status": old_status,
            "new_status": new_status,
            "old_status_duration_grains": random.randint(1, 10) if has_flipped else None,  # noqa
        }

        scenario = {
            **base_data,
            "current_value": current_value,
            "prior_value": prior_value,
            "absolute_delta_from_prior": current_value - prior_value,
            "pop_change_percent": ((current_value - prior_value) / prior_value) * 100,
            "target_value": target_value,
            "status": new_status,
            "status_change": status_change,
        }

        # Add appropriate gap/over-performance fields
        if new_status == "on_track":
            scenario.update(
                {
                    "absolute_over_performance": current_value - target_value,
                    "percent_over_performance": ((current_value - target_value) / target_value) * 100,
                }
            )
        else:
            scenario.update(
                {
                    "absolute_gap": target_value - current_value,
                    "percent_gap": ((target_value - current_value) / target_value) * 100,
                }
            )

        return scenario

    def _generate_improving_status_scenario(self, base_data: dict[str, Any]) -> dict[str, Any]:
        """Generate an improving status scenario."""
        current_value = self._generate_mock_value(1100)
        target_value = self._generate_mock_value(1000)
        prior_value = current_value * random.uniform(0.85, 0.95)  # noqa

        # Status changed from off_track to on_track
        status_change = {
            "has_flipped": True,
            "old_status": "off_track",
            "new_status": "on_track",
            "old_status_duration_grains": random.randint(2, 8),  # noqa
        }

        return {
            **base_data,
            "current_value": current_value,
            "prior_value": prior_value,
            "absolute_delta_from_prior": current_value - prior_value,
            "pop_change_percent": ((current_value - prior_value) / prior_value) * 100,
            "target_value": target_value,
            "status": "on_track",
            "status_change": status_change,
            "absolute_over_performance": current_value - target_value,
            "percent_over_performance": ((current_value - target_value) / target_value) * 100,
        }

    def _generate_worsening_status_scenario(self, base_data: dict[str, Any]) -> dict[str, Any]:
        """Generate a worsening status scenario."""
        current_value = self._generate_mock_value(800)
        target_value = self._generate_mock_value(1000)
        prior_value = current_value * random.uniform(1.05, 1.2)  # noqa

        # Status changed from on_track to off_track
        status_change = {
            "has_flipped": True,
            "old_status": "on_track",
            "new_status": "off_track",
            "old_status_duration_grains": random.randint(2, 8),  # noqa
        }

        return {
            **base_data,
            "current_value": current_value,
            "prior_value": prior_value,
            "absolute_delta_from_prior": current_value - prior_value,
            "pop_change_percent": ((current_value - prior_value) / prior_value) * 100,
            "target_value": target_value,
            "status": "off_track",
            "status_change": status_change,
            "absolute_gap": target_value - current_value,
            "percent_gap": ((target_value - current_value) / target_value) * 100,
        }

    def _generate_streak_scenario(self, base_data: dict[str, Any]) -> dict[str, Any]:
        """Generate a scenario with streak information."""
        current_value = self._generate_mock_value(1100)
        target_value = self._generate_mock_value(1000)
        prior_value = current_value * random.uniform(0.9, 0.98)  # noqa

        streak_length = random.randint(2, 15)  # noqa
        streak_status = self._choose_random_target_status()

        streak = {
            "length": streak_length,
            "status": streak_status,
            "performance_change_percent_over_streak": self._generate_percentage(-30, 30),
            "absolute_change_over_streak": self._generate_mock_value(100, 0.5),
            "average_change_percent_per_grain": self._generate_percentage(-5, 5),
            "average_change_absolute_per_grain": self._generate_mock_value(20, 0.3),
        }

        scenario = {
            **base_data,
            "current_value": current_value,
            "prior_value": prior_value,
            "absolute_delta_from_prior": current_value - prior_value,
            "pop_change_percent": ((current_value - prior_value) / prior_value) * 100,
            "target_value": target_value,
            "status": streak_status,
            "streak": streak,
        }

        # Add appropriate gap/over-performance fields
        if streak_status == "on_track":
            scenario.update(
                {
                    "absolute_over_performance": current_value - target_value,
                    "percent_over_performance": ((current_value - target_value) / target_value) * 100,
                }
            )
        else:
            scenario.update(
                {
                    "absolute_gap": target_value - current_value,
                    "percent_gap": ((target_value - current_value) / target_value) * 100,
                }
            )

        return scenario

    def _generate_hold_steady_scenario(self, base_data: dict[str, Any]) -> dict[str, Any]:
        """Generate a hold steady scenario."""
        current_value = self._generate_mock_value(1050)
        target_value = self._generate_mock_value(1000)
        prior_value = current_value * random.uniform(0.95, 1.02)  # noqa

        is_above_target = current_value >= target_value

        hold_steady = {
            "is_currently_at_or_above_target": is_above_target,
            "time_to_maintain_grains": random.randint(1, 20) if is_above_target else None,  # noqa
            "current_margin_percent": (
                ((current_value - target_value) / target_value) * 100 if is_above_target else None
            ),
        }

        scenario = {
            **base_data,
            "current_value": current_value,
            "prior_value": prior_value,
            "absolute_delta_from_prior": current_value - prior_value,
            "pop_change_percent": ((current_value - prior_value) / prior_value) * 100,
            "target_value": target_value,
            "status": "on_track" if is_above_target else "off_track",
            "hold_steady": hold_steady,
        }

        # Add appropriate gap/over-performance fields
        if is_above_target:
            scenario.update(
                {
                    "absolute_over_performance": current_value - target_value,
                    "percent_over_performance": ((current_value - target_value) / target_value) * 100,
                }
            )
        else:
            scenario.update(
                {
                    "absolute_gap": target_value - current_value,
                    "percent_gap": ((target_value - current_value) / target_value) * 100,
                }
            )

        return scenario
