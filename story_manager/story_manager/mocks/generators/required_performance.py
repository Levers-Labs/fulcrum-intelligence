import random
from datetime import date, timedelta
from typing import Any, Dict, List

from dateutil.relativedelta import relativedelta

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


class RequiredPerformanceMockGenerator(MockGeneratorBase):
    """Mock generator for Required Performance stories"""

    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.REQUIRED_PERFORMANCE
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    def __init__(self, mock_data_service: MockDataService):
        self.data_service = mock_data_service

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock required performance stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Generate required performance story
        required_series = self.get_mock_time_series(grain, StoryType.REQUIRED_PERFORMANCE)
        required_vars = self.get_mock_variables(metric, StoryType.REQUIRED_PERFORMANCE, grain, required_series)
        required_story = self.prepare_story_dict(
            metric,
            StoryType.REQUIRED_PERFORMANCE,
            grain,
            required_series,
            required_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(required_story)

        # Generate hold steady story
        hold_steady_series = self.get_mock_time_series(grain, StoryType.HOLD_STEADY)
        hold_steady_vars = self.get_mock_variables(metric, StoryType.HOLD_STEADY, grain, hold_steady_series)
        hold_steady_story = self.prepare_story_dict(
            metric,
            StoryType.HOLD_STEADY,
            grain,
            hold_steady_series,
            hold_steady_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(hold_steady_story)

        return stories

    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """Generate mock time series data for required performance stories"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)
        num_points = len(dates)

        # Get end of period date and interval
        interval, period_end_date = self._get_end_date_of_period(grain, self.data_service.story_date)

        # Generate values based on story type
        values = []
        targets = []
        growth_rates = []

        base_value = random.uniform(400, 800)

        if story_type == StoryType.REQUIRED_PERFORMANCE:
            # For required performance, current value is below end of period target
            # Generate end of period target that's clearly higher than current value
            current_value = base_value
            end_period_target = current_value * random.uniform(1.1, 1.2)  # Target is 10-20% above current value

            # Generate values with some growth but not enough to reach target
            current_growth_rate = random.uniform(1.5, 3.0)  # Current growth rate (1.5-3%)

            for i in range(num_points):
                # Generate values with the current growth rate
                if i == 0:
                    value = base_value * 0.8  # Start lower
                    growth_rate = 0
                else:
                    # Apply growth rate with some noise
                    growth_rate = current_growth_rate + random.uniform(-0.5, 0.5)
                    value = values[i - 1] * (1 + growth_rate / 100)

                values.append(round(value))
                growth_rates.append(round(growth_rate, 2))

                # Generate target for each point (increasing toward end period target)
                progress = i / (num_points - 1)
                target = base_value * 0.8 + progress * (end_period_target - base_value * 0.8)
                targets.append(round(target))

            # Ensure the last value is below the end period target
            if values[-1] >= end_period_target:
                values[-1] = end_period_target * random.uniform(0.85, 0.95)

        else:  # HOLD_STEADY
            # For hold steady, current value is already at or above end of period target
            # Generate end of period target
            end_period_target = base_value * random.uniform(1.05, 1.1)  # Target is 5-10% above base value

            # Generate values that are already performing well
            for i in range(num_points):
                if i == 0:
                    value = base_value
                    growth_rate = 0
                else:
                    # Apply a good growth rate with some noise
                    growth_rate = random.uniform(2.0, 4.0) + random.uniform(-0.5, 0.5)
                    value = values[i - 1] * (1 + growth_rate / 100)

                values.append(round(value))
                growth_rates.append(round(growth_rate, 2))

                # Generate target for each point (increasing toward end period target)
                progress = i / (num_points - 1)
                target = base_value + progress * (end_period_target - base_value)
                targets.append(round(target))

            # Ensure the last value is at or above the end period target
            if values[-1] < end_period_target:
                values[-1] = end_period_target * random.uniform(1.05, 1.15)

        # Create time series with values, targets, and growth rates
        time_series = []
        for i, (date, value, target, growth_rate) in enumerate(zip(formatted_dates, values, targets, growth_rates)):
            point = {"date": date, "value": value, "target": target, "growth_rate": growth_rate}
            time_series.append(point)

        # Store the end period target and date for use in variables
        self.end_period_target = round(end_period_target)
        self.period_end_date = period_end_date

        # Instead of adding a null point, add the target as a separate variable
        self.target_date = period_end_date.strftime("%Y-%m-%d")

        return time_series

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Generate mock variables for required performance stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Get the current period data
        current_period = time_series[-1]  # Last point is current period

        # Get interval and end date
        interval, period_end_date = self._get_end_date_of_period(grain, self.data_service.story_date)

        # Calculate required duration (periods between now and end of interval)
        req_duration = self._calculate_periods_count(self.data_service.story_date, period_end_date, grain)

        # Ensure req_duration is at least 1
        req_duration = max(1, req_duration)

        # Create base variables dict
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "duration": len(time_series),
            "req_duration": req_duration,
            "target": self.end_period_target,
            "interval": interval.value,
            "target_date": self.target_date,  # Add target date as a variable
        }

        # Randomly decide if we have minimum data
        is_min_data = random.choice([True, False])
        variables["is_min_data"] = is_min_data

        if story_type == StoryType.REQUIRED_PERFORMANCE:
            # Calculate required growth rate
            current_value = current_period["value"]
            end_target = self.end_period_target

            # Calculate required growth rate to reach target
            if req_duration > 0:
                required_growth = ((end_target / current_value) ** (1 / req_duration) - 1) * 100
            else:
                required_growth = 0

            # Ensure required_growth is at least 0.5%
            required_growth = max(0.5, required_growth)

            # Get current growth rate
            current_growth = current_period["growth_rate"]

            # Calculate growth deviation
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
