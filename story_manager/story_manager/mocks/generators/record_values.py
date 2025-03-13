import random
from datetime import date
from typing import Any, Dict, List

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.mocks.services.data_service import MockDataService
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class RecordValuesMockGenerator(MockGeneratorBase):
    """Mock generator for Record Values stories"""

    genre = StoryGenre.BIG_MOVES
    group = StoryGroup.RECORD_VALUES

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock record values stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Generate record high stories (both highest and second highest)
        # Highest
        record_high_series = self.get_mock_time_series(grain, StoryType.RECORD_HIGH, is_second_rank=False)
        record_high_vars = self.get_mock_variables(
            metric, StoryType.RECORD_HIGH, grain, record_high_series, is_second_rank=False
        )
        record_high_story = self.prepare_story_dict(
            metric,
            StoryType.RECORD_HIGH,
            grain,
            record_high_series,
            record_high_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(record_high_story)

        # Second highest
        record_high_second_series = self.get_mock_time_series(grain, StoryType.RECORD_HIGH, is_second_rank=True)
        record_high_second_vars = self.get_mock_variables(
            metric, StoryType.RECORD_HIGH, grain, record_high_second_series, is_second_rank=True
        )
        record_high_second_story = self.prepare_story_dict(
            metric,
            StoryType.RECORD_HIGH,
            grain,
            record_high_second_series,
            record_high_second_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(record_high_second_story)

        # Generate record low stories (both lowest and second lowest)
        # Lowest
        record_low_series = self.get_mock_time_series(grain, StoryType.RECORD_LOW, is_second_rank=False)
        record_low_vars = self.get_mock_variables(
            metric, StoryType.RECORD_LOW, grain, record_low_series, is_second_rank=False
        )
        record_low_story = self.prepare_story_dict(
            metric,
            StoryType.RECORD_LOW,
            grain,
            record_low_series,
            record_low_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(record_low_story)

        # Second lowest
        record_low_second_series = self.get_mock_time_series(grain, StoryType.RECORD_LOW, is_second_rank=True)
        record_low_second_vars = self.get_mock_variables(
            metric, StoryType.RECORD_LOW, grain, record_low_second_series, is_second_rank=True
        )
        record_low_second_story = self.prepare_story_dict(
            metric,
            StoryType.RECORD_LOW,
            grain,
            record_low_second_series,
            record_low_second_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(record_low_second_story)

        return stories

    def get_mock_time_series(
        self, grain: Granularity, story_type: StoryType, is_second_rank: bool = False
    ) -> list[dict[str, Any]]:
        """Generate mock time series data for record values stories"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Generate base values
        base_value = random.uniform(400, 800)
        values = []

        # First generate random values for all points except the last one
        for i in range(len(dates) - 1):
            # Generate random values with some variation
            noise = random.uniform(-0.3, 0.3) * base_value
            value = base_value + noise
            values.append(round(max(100, value)))

        # Now handle the last value based on story type and rank
        if story_type == StoryType.RECORD_HIGH:
            if not is_second_rank:
                # For highest: Make last value higher than all others
                highest_value = max(values) * random.uniform(1.1, 1.2)
                values.append(round(highest_value))
            else:
                # For second highest: Make last value second highest
                # First, make the second-to-last value the highest
                highest_value = max(values) * random.uniform(1.1, 1.2)
                values[-1] = round(highest_value)  # Temporarily set as highest

                # Then make the last value slightly lower than highest
                second_highest = highest_value * random.uniform(0.9, 0.95)

                # Add the second highest as the last value
                values.append(round(second_highest))
        else:  # RECORD_LOW
            if not is_second_rank:
                # For lowest: Make last value lower than all others
                lowest_value = min(values) * random.uniform(0.7, 0.9)
                values.append(round(max(100, lowest_value)))
            else:
                # For second lowest: Make last value second lowest
                # First, make the second-to-last value the lowest
                lowest_value = min(values) * random.uniform(0.7, 0.8)
                values[-1] = round(max(100, lowest_value))  # Temporarily set as lowest

                # Then make the last value slightly higher than lowest
                second_lowest = lowest_value * random.uniform(1.05, 1.15)

                # Add the second lowest as the last value
                values.append(round(max(100, second_lowest)))

        # Create time series
        return [{"date": date, "value": value} for date, value in zip(formatted_dates, values)]

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] = None,
        is_second_rank: bool = False,
    ) -> dict[str, Any]:
        """Generate mock variables for record values stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Get required periods
        periods = STORY_GROUP_TIME_DURATIONS[self.group][grain]["input"]

        # Get the latest data point
        latest_point = time_series[-1]
        record_value = latest_point["value"]
        record_date = latest_point["date"]

        # Sort the time series by value
        sorted_series = sorted(time_series, key=lambda x: x["value"], reverse=(story_type == StoryType.RECORD_HIGH))

        # Set rank based on story type and is_second_rank
        if story_type == StoryType.RECORD_HIGH:
            rank = 2 if is_second_rank else 1
        else:  # RECORD_LOW
            rank = len(time_series) - 1 if is_second_rank else len(time_series)

        # Get the prior value
        if story_type == StoryType.RECORD_HIGH:
            if is_second_rank:
                # For second highest, prior is third highest
                prior_point = sorted_series[2]
            else:
                # For highest, prior is second highest
                prior_point = sorted_series[1]
        else:  # RECORD_LOW
            if is_second_rank:
                # For second lowest, prior is third lowest
                prior_point = sorted_series[-3]
            else:
                # For lowest, prior is second lowest
                prior_point = sorted_series[-2]

        prior_value = prior_point["value"]
        prior_date = prior_point["date"]

        # Calculate deviation (only for non-second rank)
        if not is_second_rank:
            if story_type == StoryType.RECORD_HIGH:
                deviation = ((record_value - prior_value) / prior_value) * 100
            else:
                deviation = ((prior_value - record_value) / prior_value) * 100
        else:
            deviation = 0  # Not needed for second rank stories

        # Create variables dict
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "duration": periods,
            "value": record_value,
            "prior_value": prior_value,
            "prior_date": prior_date,
            "deviation": round(abs(deviation), 2),
            "is_second_rank": is_second_rank,
            "record_date": record_date,
            "rank": rank,
        }

        return variables
