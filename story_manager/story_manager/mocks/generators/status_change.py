import random
from datetime import date
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase


class StatusChangeMockGenerator(MockGeneratorBase):
    """Mock generator for Status Change stories"""

    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.STATUS_CHANGE
    # Story mapping for status changes
    story_mapping = {
        (StoryType.ON_TRACK, StoryType.OFF_TRACK): StoryType.IMPROVING_STATUS,
        (StoryType.OFF_TRACK, StoryType.ON_TRACK): StoryType.WORSENING_STATUS,
    }

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock status change stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Define story types to generate
        story_types = [StoryType.IMPROVING_STATUS, StoryType.WORSENING_STATUS]

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
        """Generate mock time series data for status change stories"""
        # Get date range and dates
        start_date, end_date = self.data_service.get_input_time_range(grain, self.group)
        formatted_dates = self.data_service.get_formatted_dates(grain, start_date, end_date)

        # Determine status transition based on story type
        is_improving = story_type == StoryType.IMPROVING_STATUS
        current_status = StoryType.ON_TRACK if is_improving else StoryType.OFF_TRACK
        previous_status = StoryType.OFF_TRACK if is_improving else StoryType.ON_TRACK

        # Generate time series data with appropriate pattern
        return self._generate_status_change_series(formatted_dates, current_status, previous_status)

    def _generate_status_change_series(
        self, formatted_dates: list[str], current_status: StoryType, previous_status: StoryType
    ) -> list[dict[str, Any]]:
        """Generate a time series with a status change pattern"""
        num_points = len(formatted_dates)

        # Initialize base values
        base_value = random.uniform(400, 800)  # noqa
        base_target = base_value

        # Previous duration (how long it was consistently in previous status)
        prev_duration = random.randint(2, min(5, num_points - 1))  # noqa

        values: list = []
        targets = []
        statuses = []

        # Generate data points
        for i in range(num_points):
            if i == num_points - 1:
                # Last point has current_status
                status = current_status
                is_on_track = status == StoryType.ON_TRACK
            elif i >= num_points - prev_duration - 1:
                # Previous consecutive status period
                status = previous_status
                is_on_track = status == StoryType.ON_TRACK
            else:
                # Earlier points have mixed statuses
                if random.random() < 0.3:  # 30% chance of opposite status  # noqa
                    status = current_status
                    is_on_track = status == StoryType.ON_TRACK
                else:
                    status = previous_status
                    is_on_track = status == StoryType.ON_TRACK

            # Set target with some randomness
            target = base_target * (1 + random.uniform(0.05, 0.15))  # noqa

            # Set value based on on-track status
            if is_on_track:
                value = target * (1 + random.uniform(0.05, 0.15))  # noqa
            else:
                value = target * (1 - random.uniform(0.05, 0.15))  # noqa

            values.append(round(value))
            targets.append(round(target))
            statuses.append(status)

            # Update base values with some trend for next iteration
            base_value = value * (1 + random.uniform(-0.05, 0.05))  # noqa
            base_target = target * (1 + random.uniform(-0.05, 0.05))  # noqa

        # Create time series
        return [
            {"date": series_date, "value": value, "target": target, "status": status}
            for series_date, value, target, status in zip(formatted_dates, values, targets, statuses)
        ]

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Generate mock variables for status change stories"""

        # Get current period data
        current_period = time_series[-1]  # type: ignore
        value = current_period["value"]
        target = current_period["target"]

        # Calculate deviation based on story type
        if story_type == StoryType.IMPROVING_STATUS:
            # For improving status, value is above target (positive deviation)
            deviation = ((value - target) / target) * 100
        else:  # WORSENING_STATUS
            # For worsening status, value is below target (negative deviation)
            deviation = ((target - value) / target) * 100

        # Determine previous status
        prev_status = StoryType.OFF_TRACK if story_type == StoryType.IMPROVING_STATUS else StoryType.ON_TRACK

        # Count consecutive periods with previous status
        prev_duration = 0
        for i in range(len(time_series) - 2, -1, -1):  # type: ignore
            if time_series[i]["status"] == prev_status:  # type: ignore
                prev_duration += 1
            else:
                break

        # Return variables dictionary
        return {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "deviation": round(abs(deviation), 2),
            "prev_duration": prev_duration,
        }
