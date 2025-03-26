import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.story_builder.constants import STORY_GROUP_TIME_DURATIONS


class RecordValuesMockGenerator(MockGeneratorBase):
    """Mock generator for Record Values stories"""

    genre = StoryGenre.BIG_MOVES
    group = StoryGroup.RECORD_VALUES

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock record values stories"""
        if story_date:
            self.data_service.story_date = story_date

        # Define scenarios to generate (story type, is_second_rank)
        scenarios = [
            (StoryType.RECORD_HIGH, False),  # Highest
            (StoryType.RECORD_HIGH, True),  # Second highest
            (StoryType.RECORD_LOW, False),  # Lowest
            (StoryType.RECORD_LOW, True),  # Second lowest
        ]

        stories = []
        for story_type, is_second_rank in scenarios:
            time_series = self.get_mock_time_series(grain, story_type, is_second_rank)
            variables = self.get_mock_variables(metric, story_type, grain, time_series, is_second_rank)

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

    def get_mock_time_series(
        self, grain: Granularity, story_type: StoryType, is_second_rank: bool = False
    ) -> list[dict[str, Any]]:
        """Generate mock time series data for record values stories"""
        # Get dates for the time series
        start_date, end_date = self.data_service.get_input_time_range(grain, self.group)
        formatted_dates = self.data_service.get_formatted_dates(grain, start_date, end_date)

        # Generate base values with random variation
        base_value = random.uniform(400, 800)  # noqa
        values = []

        # Generate all values except the last two (which will be manipulated for record purposes)
        for _ in range(len(formatted_dates) - 2):
            noise = random.uniform(-0.3, 0.3) * base_value  # noqa
            value = max(100, base_value + noise)  # Ensure minimum value
            values.append(round(value))

        # Determine the special values based on story type
        is_high_record = story_type == StoryType.RECORD_HIGH

        # Handle the special cases for second-to-last and last values
        if is_high_record:
            # For record high
            self._add_high_record_values(values, is_second_rank)
        else:
            # For record low
            self._add_low_record_values(values, is_second_rank)

        # Create time series with dates and values
        return [{"date": date, "value": value} for date, value in zip(formatted_dates, values)]

    def _add_high_record_values(self, values: list, is_second_rank: bool) -> None:
        """Add values for record high scenarios"""
        highest_value = max(values) * random.uniform(1.1, 1.2)  # noqa

        if is_second_rank:
            # Second highest: Make second-to-last value the highest, last value slightly lower
            values.append(round(highest_value))  # Second-to-last is highest
            second_highest = highest_value * random.uniform(0.9, 0.95)  # noqa
            values.append(round(second_highest))  # Last is second highest
        else:
            # Highest: Add a random non-record value, then the record value
            values.append(round(random.uniform(min(values), max(values))))  # noqa
            values.append(round(highest_value))  # Last is highest

    def _add_low_record_values(self, values: list, is_second_rank: bool) -> None:
        """Add values for record low scenarios"""
        lowest_value = min(values) * random.uniform(0.7, 0.9)  # noqa
        lowest_value = max(100, lowest_value)  # Ensure minimum value

        if is_second_rank:
            # Second lowest: Make second-to-last value the lowest, last value slightly higher
            values.append(round(lowest_value))  # Second-to-last is lowest
            second_lowest = lowest_value * random.uniform(1.05, 1.15)  # noqa
            values.append(round(max(100, second_lowest)))  # Last is second lowest
        else:
            # Lowest: Add a random non-record value, then the record value
            values.append(round(random.uniform(min(values), max(values))))  # noqa
            values.append(round(lowest_value))  # Last is lowest

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] | None = None,
        is_second_rank: bool = False,
    ) -> dict[str, Any]:
        """Generate mock variables for record values stories"""
        # Get grain metadata
        periods = STORY_GROUP_TIME_DURATIONS[self.group][grain]["input"]

        # Get the latest data point (record point)
        latest_point = time_series[-1]  # type: ignore
        record_value = latest_point["value"]
        record_date = latest_point["date"]

        # Sort the time series by value (high to low for record high, low to high for record low)
        is_high_record = story_type == StoryType.RECORD_HIGH
        sorted_series = sorted(time_series, key=lambda x: x["value"], reverse=is_high_record)  # type: ignore

        # Determine rank based on story type and is_second_rank
        rank = (
            2
            if is_second_rank
            else 1 if is_high_record else (len(time_series) - 1 if is_second_rank else len(time_series))  # type: ignore
        )

        # Find the prior value based on story type and rank
        prior_index = 2 if is_second_rank else 1 if is_high_record else -3 if is_second_rank else -2
        prior_point = sorted_series[prior_index if prior_index >= 0 else len(sorted_series) + prior_index]
        prior_value = prior_point["value"]
        prior_date = prior_point["date"]

        # Calculate deviation (for non-second rank)
        deviation = 0
        if not is_second_rank:
            if is_high_record:
                deviation = ((record_value - prior_value) / prior_value) * 100
            else:
                deviation = ((prior_value - record_value) / prior_value) * 100

        # Return variables dictionary
        return {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "duration": periods,
            "value": record_value,
            "prior_value": prior_value,
            "prior_date": prior_date,
            "deviation": round(abs(deviation), 2),
            "is_second_rank": is_second_rank,
            "record_date": record_date,
            "rank": rank,
        }
