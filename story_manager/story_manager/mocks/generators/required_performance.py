import random
from datetime import date, timedelta
from typing import Any

from dateutil.relativedelta import relativedelta

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Movement,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.story_builder.constants import GRAIN_META


class RequiredPerformanceMockGenerator(MockGeneratorBase):
    """Mock generator for Required Performance stories"""

    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.REQUIRED_PERFORMANCE

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock required performance stories"""
        if story_date:
            self.data_service.story_date = story_date

        # Define story types to generate
        story_types = [StoryType.REQUIRED_PERFORMANCE, StoryType.HOLD_STEADY]
        stories = []

        # Generate a story for each type
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
        """Generate mock time series data for required performance stories"""
        # Get dates for the time series
        start_date, end_date = self.data_service.get_input_time_range(grain, self.group)
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Get end of period info
        interval, period_end_date = self._get_end_date_of_period(grain, self.data_service.story_date)

        # Generate base value and targets
        base_value = random.uniform(400, 800)  # noqa

        # Different handling based on story type
        if story_type == StoryType.REQUIRED_PERFORMANCE:
            end_period_target = base_value * random.uniform(1.1, 1.2)  # noqa
            time_series = self._generate_required_performance_series(formatted_dates, base_value, end_period_target)
        else:  # HOLD_STEADY
            end_period_target = base_value * random.uniform(1.05, 1.1)  # Target 5-10% above base  # noqa
            time_series = self._generate_hold_steady_series(formatted_dates, base_value, end_period_target)

        # Store values needed for variables
        self.end_period_target = round(end_period_target)
        self.period_end_date = period_end_date
        self.target_date = period_end_date.strftime("%Y-%m-%d")

        return time_series

    def _generate_required_performance_series(
        self, formatted_dates: list[str], base_value: float, end_period_target: float
    ) -> list[dict[str, Any]]:
        """Generate time series for Required Performance story type"""
        num_points = len(formatted_dates)
        values: list = []
        targets = []
        growth_rates = []

        # Current growth rate (not enough to reach target)
        current_growth_rate = random.uniform(1.5, 3.0)  # noqa

        # For day grain, add more randomization
        is_day_grain = len(formatted_dates) > 20  # Typically day grain has more points

        # Determine a few random points that might exceed target (only for day grain)
        above_target_indices = []
        if is_day_grain:
            # For day grain, randomly select 2-4 points that might exceed target
            num_exceed_points = random.randint(2, 4)  # noqa
            potential_indices = list(range(num_points // 4, num_points - 5))  # Middle section of the series
            if potential_indices:
                above_target_indices = sorted(
                    random.sample(potential_indices, min(num_exceed_points, len(potential_indices)))  # noqa
                )

        for i in range(num_points):
            # Generate value with growth
            if i == 0:
                value = base_value * 0.8  # Start lower
                growth_rate = 0
            else:
                # Apply growth rate with more noise
                if is_day_grain:
                    noise_factor = random.uniform(-1.5, 1.5)  # noqa
                else:
                    noise_factor = random.uniform(-0.5, 0.5)  # noqa

                growth_rate = current_growth_rate + noise_factor  # type: ignore
                value = values[i - 1] * (1 + growth_rate / 100)

            # Generate target (increasing toward end period target)
            progress = i / (num_points - 1) if num_points > 1 else 1
            target = base_value * 0.8 + progress * (end_period_target - base_value * 0.8)

            # Occasionally allow values to exceed target (only at predetermined points)
            if is_day_grain and i in above_target_indices:
                # 80% chance to exceed
                if random.random() < 0.8:  # noqa
                    value = target * random.uniform(1.01, 1.10)  # 1-10% above target  # noqa

            values.append(round(value))
            growth_rates.append(round(growth_rate, 2))
            targets.append(round(target))

        # Ensure last value is below target (maintaining story integrity)
        if values[-1] >= end_period_target:
            values[-1] = end_period_target * random.uniform(0.85, 0.95)  # noqa

        # Create time series
        return [
            {"date": date, "value": value, "target": target, "growth_rate": growth_rate}
            for date, value, target, growth_rate in zip(formatted_dates, values, targets, growth_rates)
        ]

    def _generate_hold_steady_series(
        self, formatted_dates: list[str], base_value: float, end_period_target: float
    ) -> list[dict[str, Any]]:
        """Generate time series for Hold Steady story type"""
        num_points = len(formatted_dates)
        values: list = []
        targets = []
        growth_rates = []

        for i in range(num_points):
            # Generate value
            if i == 0:
                value = base_value
                growth_rate = 0.0
            else:
                # Apply good growth rate with noise
                growth_rate = random.uniform(2.0, 4.0) + random.uniform(-0.5, 0.5)  # noqa
                value = values[i - 1] * (1 + growth_rate / 100)

            values.append(round(value))
            growth_rates.append(round(growth_rate, 2))

            # Generate target (increasing toward end period target)
            progress = i / (num_points - 1) if num_points > 1 else 1
            target = base_value + progress * (end_period_target - base_value)
            targets.append(round(target))

        # Ensure last value is at or above target
        if values[-1] < end_period_target:
            values[-1] = end_period_target * random.uniform(1.05, 1.15)  # noqa

        # Create time series
        return [
            {"date": date, "value": value, "target": target, "growth_rate": growth_rate}
            for date, value, target, growth_rate in zip(formatted_dates, values, targets, growth_rates)
        ]

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Generate mock variables for required performance stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Get current period data and calculate required duration
        current_period = time_series[-1]  # type: ignore
        interval, period_end_date = self._get_end_date_of_period(grain, self.data_service.story_date)
        req_duration = max(1, self._calculate_periods_count(self.data_service.story_date, period_end_date, grain))

        # Create base variables dict
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "duration": len(time_series),  # type: ignore
            "req_duration": req_duration,
            "target": self.end_period_target,
            "target_date": self.target_date,
            "is_min_data": random.choice([True, False]),  # noqa
        }

        # Add story type specific variables
        if story_type == StoryType.REQUIRED_PERFORMANCE:
            current_value = current_period["value"]

            # Calculate required growth rate to reach target
            required_growth = max(0.5, ((self.end_period_target / current_value) ** (1 / req_duration) - 1) * 100)
            current_growth = current_period["growth_rate"]
            growth_deviation = required_growth - current_growth

            # Add required performance variables
            variables.update(
                {
                    "required_growth": round(required_growth, 2),
                    "current_growth": round(current_growth, 2),
                    "growth_deviation": round(abs(growth_deviation), 2),
                    "movement": Movement.INCREASE.value if growth_deviation > 0 else Movement.DECREASE.value,
                }
            )

        return variables

    def _get_end_date_of_period(self, grain: Granularity, story_date: date) -> tuple[Granularity, date]:
        """
        Get the end date of the period of the given grain.

        Logic:
        - for day and week grain, end date will be the end of current month with interval month.
        - for month grain, end date will be the end of current quarter with interval quarter.
        """
        if grain == Granularity.DAY or grain == Granularity.WEEK:
            interval = Granularity.MONTH
            # End of the month
            end_date = (story_date + relativedelta(months=1)).replace(day=1) - timedelta(days=1)
        elif grain == Granularity.MONTH:
            interval = Granularity.QUARTER
            # Determine the end of the current quarter
            quarter_end_month = (story_date.month - 1) // 3 * 3 + 3
            end_date = story_date.replace(month=quarter_end_month) + relativedelta(day=1, months=1, days=-1)
        else:
            raise ValueError(f"Unsupported grain: {grain}")

        return interval, end_date

    def _calculate_periods_count(self, start_date: date, end_date: date, grain: Granularity) -> int:
        """Calculate the number of periods between start and end date for the given grain."""
        if grain == Granularity.DAY:
            return (end_date - start_date).days
        elif grain == Granularity.WEEK:
            return ((end_date - start_date).days + 6) // 7
        elif grain == Granularity.MONTH:
            return (end_date.year - start_date.year) * 12 + end_date.month - start_date.month
        else:
            raise ValueError(f"Unsupported grain: {grain}")
