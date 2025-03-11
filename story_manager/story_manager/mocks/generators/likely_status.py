import random
from datetime import date, timedelta
from typing import Any, Dict, List

from dateutil.relativedelta import relativedelta

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase
from story_manager.mocks.services.data_service import MockDataService
from story_manager.story_builder.constants import GRAIN_META, STORY_GROUP_TIME_DURATIONS


class LikelyStatusMockGenerator(MockGeneratorBase):
    """Mock generator for Likely Status stories"""

    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.LIKELY_STATUS
    supported_grains = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH]

    def __init__(self, mock_data_service: MockDataService):
        self.data_service = mock_data_service

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date = None
    ) -> list[dict[str, Any]]:
        """Generate mock likely status stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []

        # Generate likely on track story
        on_track_series = self.get_mock_time_series(grain, StoryType.LIKELY_ON_TRACK)
        on_track_vars = self.get_mock_variables(metric, StoryType.LIKELY_ON_TRACK, grain, on_track_series)
        on_track_story = self.prepare_story_dict(
            metric,
            StoryType.LIKELY_ON_TRACK,
            grain,
            on_track_series,
            on_track_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(on_track_story)

        # Generate likely off track story
        off_track_series = self.get_mock_time_series(grain, StoryType.LIKELY_OFF_TRACK)
        off_track_vars = self.get_mock_variables(metric, StoryType.LIKELY_OFF_TRACK, grain, off_track_series)
        off_track_story = self.prepare_story_dict(
            metric,
            StoryType.LIKELY_OFF_TRACK,
            grain,
            off_track_series,
            off_track_vars,
            story_date or self.data_service.story_date,
        )
        stories.append(off_track_story)

        return stories

    def get_mock_time_series(self, grain: Granularity, story_type: StoryType) -> list[dict[str, Any]]:
        """Generate mock time series data for likely status stories"""
        # Get date range
        start_date, end_date = self.data_service._get_input_time_range(grain, self.group)

        # Get dates within range
        dates = self.data_service.get_dates_for_range(grain, start_date, end_date)
        formatted_dates = self.data_service.get_formatted_dates(dates)

        # Get story period
        interval, story_start_date, story_end_date = self._get_story_period(grain, self.data_service.story_date)

        # Generate values based on story type
        values = []
        targets = []
        forecasted = []

        base_value = random.uniform(400, 800)
        base_target = base_value * random.uniform(0.9, 1.1)  # Target around base value

        # Generate end of period target
        end_period_target = base_target * random.uniform(1.05, 1.15)  # Target is 5-15% above base target

        # Generate forecasted value based on story type
        if story_type == StoryType.LIKELY_ON_TRACK:
            # For likely on track, forecasted value is above target
            forecasted_value = end_period_target * random.uniform(1.05, 1.2)  # 5-20% above target
        else:  # LIKELY_OFF_TRACK
            # For likely off track, forecasted value is below target
            forecasted_value = end_period_target * random.uniform(0.7, 0.95)  # 5-30% below target

        # Generate historical values with some trend
        for i in range(len(formatted_dates)):
            # Generate value with some trend and noise
            if i == 0:
                value = base_value
            else:
                # Add trend and noise
                trend = random.uniform(-0.02, 0.04) * base_value
                noise = random.uniform(-0.05, 0.05) * base_value
                value = values[-1] + trend + noise

            values.append(round(value))

            # Generate target with some trend toward end period target
            progress = i / (len(formatted_dates) - 1)
            target = base_target + progress * (end_period_target - base_target)
            targets.append(round(target))

            # Mark as historical (not forecasted)
            forecasted.append(False)

        # Create time series with values, targets, and forecasted flag
        time_series = []
        for i, (date_str, value, target, is_forecasted) in enumerate(zip(formatted_dates, values, targets, forecasted)):
            point = {"date": date_str, "value": value, "target": target, "forecasted": is_forecasted}
            time_series.append(point)

        # Add forecasted points from story_start_date to story_end_date
        forecast_dates = self._get_forecast_dates(grain, story_start_date, story_end_date)
        formatted_forecast_dates = self.data_service.get_formatted_dates(forecast_dates)

        # Generate forecasted values with trend toward the final forecasted value
        last_historical_value = values[-1]
        forecast_count = len(forecast_dates)

        for i, date_str in enumerate(formatted_forecast_dates):
            # Calculate progress toward final forecasted value
            progress = (i + 1) / forecast_count

            # Interpolate between last historical value and final forecasted value
            forecasted_point_value = last_historical_value + progress * (forecasted_value - last_historical_value)

            # Generate target for this forecast date
            target_progress = (len(formatted_dates) + i) / (len(formatted_dates) + forecast_count - 1)
            forecasted_point_target = base_target + target_progress * (end_period_target - base_target)

            # Add forecasted point
            point = {
                "date": date_str,
                "value": round(forecasted_point_value),
                "target": round(forecasted_point_target),
                "forecasted": True,
            }
            time_series.append(point)

        # Store end period values for variables
        self.forecasted_value = round(forecasted_value)
        self.target_value = round(end_period_target)
        self.interval = interval

        return time_series

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
        time_series: list[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Generate mock variables for likely status stories"""
        # Get grain metadata
        grain_meta = GRAIN_META[grain]

        # Calculate deviation between forecasted value and target
        if story_type == StoryType.LIKELY_ON_TRACK:
            # For on track, forecasted value is above target
            deviation = ((self.forecasted_value - self.target_value) / self.target_value) * 100
        else:  # LIKELY_OFF_TRACK
            # For off track, forecasted value is below target
            deviation = ((self.target_value - self.forecasted_value) / self.target_value) * 100

        # Create variables dict
        variables = {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "eoi": grain_meta["eoi"],
            "pop": grain_meta["pop"],
            "interval": self.interval.value,
            "forecasted_value": self.forecasted_value,
            "target": self.target_value,
            "deviation": round(abs(deviation), 2),
        }

        return variables

    def _get_story_period(self, grain: Granularity, story_date: date) -> tuple[Granularity, date, date]:
        """
        Get the interval, start date, and end date for the story period based
        on the grain and current date.
        """
        today = story_date
        if grain == Granularity.DAY:
            interval = Granularity.WEEK
            # end of the week
            end_date = today + timedelta(days=(6 - today.weekday()))
            # start of the week
            start_date = today - timedelta(days=today.weekday())
        elif grain == Granularity.WEEK:
            interval = Granularity.MONTH
            # start of last week of the month
            last_day_of_month = (today + relativedelta(months=1)).replace(day=1) - timedelta(days=1)
            end_date = last_day_of_month - timedelta(days=last_day_of_month.weekday())
            # start of the month
            start_date = last_day_of_month.replace(day=1)
        elif grain == Granularity.MONTH:
            interval = Granularity.QUARTER
            # start of quarter-end month
            quarter_end_month = (today.month - 1) // 3 * 3 + 3
            end_date = today.replace(month=quarter_end_month, day=1)
            # start of the quarter
            start_date = end_date - relativedelta(months=2)
        else:
            raise ValueError(f"Unsupported grain: {grain}")

        return interval, start_date, end_date

    def _get_forecast_dates(self, grain: Granularity, start_date: date, end_date: date) -> list[date]:
        """Get a list of dates for the forecast period"""
        dates = []
        current_date = start_date

        if grain == Granularity.DAY:
            while current_date <= end_date:
                dates.append(current_date)
                current_date += timedelta(days=1)
        elif grain == Granularity.WEEK:
            while current_date <= end_date:
                dates.append(current_date)
                current_date += timedelta(days=7)
        elif grain == Granularity.MONTH:
            while current_date <= end_date:
                dates.append(current_date)
                current_date += relativedelta(months=1)

        return dates
