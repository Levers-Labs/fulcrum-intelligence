import random
from datetime import date
from typing import Any, Dict, List

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.mocks.services.data_service import MockDataService
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class StatusChangeMockGenerator(MockGeneratorBase):
    """Mock generator for Status Change stories"""

    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.STATUS_CHANGE
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    # Story mapping for status changes
    story_mapping = {
        (StoryType.ON_TRACK, StoryType.OFF_TRACK): StoryType.IMPROVING_STATUS,
        (StoryType.OFF_TRACK, StoryType.ON_TRACK): StoryType.WORSENING_STATUS,
    }

    def __init__(self, mock_data_service: MockDataService):
        self.data_service = mock_data_service

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock status change stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Generate improving status story
        improving_series = self.get_mock_time_series(grain, StoryType.IMPROVING_STATUS)
        improving_vars = self.get_mock_variables(metric, StoryType.IMPROVING_STATUS, grain, improving_series)
        improving_story = self.prepare_story_dict(
            metric,
            StoryType.IMPROVING_STATUS,
            grain,
            improving_series,
            improving_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(improving_story)

        # Generate worsening status story
        worsening_series = self.get_mock_time_series(grain, StoryType.WORSENING_STATUS)
        worsening_vars = self.get_mock_variables(metric, StoryType.WORSENING_STATUS, grain, worsening_series)
        worsening_story = self.prepare_story_dict(
            metric,
            StoryType.WORSENING_STATUS,
            grain,
            worsening_series,
            worsening_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(worsening_story)

        return stories

    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """Generate mock time series data for status change stories with more varied patterns"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)
        num_points = len(dates)

        # Generate values based on story type
        values = []
        targets = []
        statuses = []

        base_value = random.uniform(400, 800)
        base_target = base_value  # Start with target equal to value

        if story_type == StoryType.IMPROVING_STATUS:
            # For improving status (OFF_TRACK -> ON_TRACK)
            # Create a more interesting pattern with some fluctuations

            # Previous duration (how long it was consistently OFF_TRACK)
            prev_duration = random.randint(2, min(5, num_points - 1))

            # Create a pattern with some variation
            for i in range(num_points):
                if i == num_points - 1:
                    # Last point is ON_TRACK (beating target)
                    target = base_target * (1 + random.uniform(0.05, 0.15))
                    value = target * (1 + random.uniform(0.05, 0.15))  # Value above target
                    status = StoryType.ON_TRACK
                elif i >= num_points - prev_duration - 1:
                    # Previous consecutive OFF_TRACK period
                    target = base_target * (1 + random.uniform(0.05, 0.15))
                    value = target * (1 - random.uniform(0.05, 0.15))  # Value below target
                    status = StoryType.OFF_TRACK
                else:
                    # Earlier points can have mixed statuses
                    if random.random() < 0.3:  # 30% chance of being ON_TRACK
                        target = base_target * (1 + random.uniform(0.05, 0.15))
                        value = target * (1 + random.uniform(0.05, 0.15))  # Value above target
                        status = StoryType.ON_TRACK
                    else:
                        target = base_target * (1 + random.uniform(0.05, 0.15))
                        value = target * (1 - random.uniform(0.05, 0.15))  # Value below target
                        status = StoryType.OFF_TRACK

                values.append(round(value))
                targets.append(round(target))
                statuses.append(status)

                # Update base values with some trend
                base_value = value * (1 + random.uniform(-0.05, 0.05))
                base_target = target * (1 + random.uniform(-0.05, 0.05))

        else:  # WORSENING_STATUS
            # For worsening status (ON_TRACK -> OFF_TRACK)
            # Create a more interesting pattern with some fluctuations

            # Previous duration (how long it was consistently ON_TRACK)
            prev_duration = random.randint(2, min(5, num_points - 1))

            # Create a pattern with some variation
            for i in range(num_points):
                if i == num_points - 1:
                    # Last point is OFF_TRACK (missing target)
                    target = base_target * (1 + random.uniform(0.05, 0.15))
                    value = target * (1 - random.uniform(0.05, 0.15))  # Value below target
                    status = StoryType.OFF_TRACK
                elif i >= num_points - prev_duration - 1:
                    # Previous consecutive ON_TRACK period
                    target = base_target * (1 + random.uniform(0.05, 0.15))
                    value = target * (1 + random.uniform(0.05, 0.15))  # Value above target
                    status = StoryType.ON_TRACK
                else:
                    # Earlier points can have mixed statuses
                    if random.random() < 0.3:  # 30% chance of being OFF_TRACK
                        target = base_target * (1 + random.uniform(0.05, 0.15))
                        value = target * (1 - random.uniform(0.05, 0.15))  # Value below target
                        status = StoryType.OFF_TRACK
                    else:
                        target = base_target * (1 + random.uniform(0.05, 0.15))
                        value = target * (1 + random.uniform(0.05, 0.15))  # Value above target
                        status = StoryType.ON_TRACK

                values.append(round(value))
                targets.append(round(target))
                statuses.append(status)

                # Update base values with some trend
                base_value = value * (1 + random.uniform(-0.05, 0.05))
                base_target = target * (1 + random.uniform(-0.05, 0.05))

        # Create time series with values, targets, and statuses
        time_series = []
        for i, (date, value, target, status) in enumerate(zip(formatted_dates, values, targets, statuses)):
            point = {"date": date, "value": value, "target": target, "status": status}
            time_series.append(point)

        return time_series

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Generate mock variables for status change stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Get the current and previous periods
        current_period = time_series[-1]

        # Calculate deviation between value and target
        value = current_period["value"]
        target = current_period["target"]

        if story_type == StoryType.IMPROVING_STATUS:
            # For improving status, value is above target (positive deviation)
            deviation = ((value - target) / target) * 100
        else:  # WORSENING_STATUS
            # For worsening status, value is below target (negative deviation)
            deviation = ((target - value) / target) * 100

        # Count previous duration (how many consecutive periods with the previous status)
        prev_status = StoryType.OFF_TRACK if story_type == StoryType.IMPROVING_STATUS else StoryType.ON_TRACK
        prev_duration = 0

        # Count consecutive occurrences of the previous status (going backwards from second-to-last)
        for i in range(len(time_series) - 2, -1, -1):
            if time_series[i]["status"] == prev_status:
                prev_duration += 1
            else:
                break

        # Create variables dict
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": grain_meta["interval"],
            "deviation": round(abs(deviation), 2),
            "prev_duration": prev_duration,
        }

        return variables
