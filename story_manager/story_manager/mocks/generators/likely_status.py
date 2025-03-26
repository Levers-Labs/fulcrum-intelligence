import random
from datetime import date, timedelta
from typing import Any

from dateutil.relativedelta import relativedelta

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.mocks.generators.base import MockGeneratorBase


class LikelyStatusMockGenerator(MockGeneratorBase):
    """Mock generator for Likely Status stories"""

    genre = StoryGenre.PERFORMANCE
    group = StoryGroup.LIKELY_STATUS

    def generate_stories(
        self, metric: dict[str, Any], grain: Granularity, story_date: date | None = None
    ) -> list[dict[str, Any]]:
        """Generate mock likely status stories"""
        if story_date:
            self.data_service.story_date = story_date

        stories = []
        story_types = [StoryType.LIKELY_ON_TRACK, StoryType.LIKELY_OFF_TRACK]

        for story_type in story_types:
            time_series = self.get_mock_time_series(grain, story_type)
            variables = self.get_mock_variables(metric, story_type, grain)

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
        """Generate mock time series data for likely status stories"""
        # Get historical date range and dates
        start_date, end_date = self.data_service.get_input_time_range(grain, self.group)
        formatted_historical_dates = self.data_service.get_formatted_dates(grain, start_date, end_date)

        # Get forecast period information
        interval, forecast_start_date, forecast_end_date = self._get_story_period(grain, self.data_service.story_date)

        # Initialize base values with scaling control for day grain
        if grain == Granularity.DAY:
            # For day grain, keep values in a more controlled range
            # Target will be in the 500-800 range
            end_period_target = random.uniform(500, 800)  # noqa

            # Set base value to be a percentage of the target
            if story_type == StoryType.LIKELY_OFF_TRACK:
                base_value = end_period_target * random.uniform(0.50, 0.70)  # noqa
                # Target starts slightly lower than end target
                base_target = end_period_target * random.uniform(0.85, 0.95)  # noqa
            else:  # LIKELY_ON_TRACK
                base_value = end_period_target * random.uniform(0.65, 0.85)  # noqa
                # Target starts slightly lower than end target
                base_target = end_period_target * random.uniform(0.85, 0.95)  # noqa
        else:
            # For other grains, use the original logic
            base_value = random.uniform(400, 800)  # noqa
            base_target = base_value * random.uniform(0.9, 1.1)  # noqa
            end_period_target = base_target * random.uniform(1.05, 1.15)  # noqa

        # Determine forecasted value based on story type
        if story_type == StoryType.LIKELY_ON_TRACK:
            forecasted_value = end_period_target * random.uniform(1.05, 1.2)  # noqa
        else:  # LIKELY_OFF_TRACK
            # For day grain, keep the forecasted value lower relative to target but with more variety
            if grain == Granularity.DAY:
                # Select the pattern type for day grain off-track
                pattern_type = random.choice(["moderate_decline", "sharp_decline", "plateau_then_drop"])  # noqa

                # Different forecast values based on pattern type
                if pattern_type == "moderate_decline":
                    # Moderate decline - not too far below target
                    forecasted_value = end_period_target * random.uniform(0.75, 0.85)  # noqa
                elif pattern_type == "sharp_decline":
                    # Sharp decline - significantly below target
                    forecasted_value = end_period_target * random.uniform(0.65, 0.75)  # noqa
                else:  # plateau_then_drop
                    # Plateau followed by drop - moderately below target
                    forecasted_value = end_period_target * random.uniform(0.70, 0.80)  # noqa

                # Store pattern type for later use in generating points
                self.pattern_type = pattern_type
            else:
                forecasted_value = end_period_target * random.uniform(0.7, 0.95)  # noqa

        # Generate historical values and targets with adjustment for day grain
        time_series = self._generate_historical_points(
            formatted_historical_dates, base_value, base_target, end_period_target, grain, story_type
        )

        # Generate forecasted values
        self._add_forecast_points(
            time_series,
            grain,
            forecast_start_date,
            forecast_end_date,
            time_series[-1]["value"],
            forecasted_value,
            base_target,
            end_period_target,
            story_type,
        )

        # Store values needed for variables
        self.forecasted_value = round(forecasted_value)
        self.target_value = round(end_period_target)
        self.interval = interval

        return time_series

    def _generate_historical_points(
        self, formatted_dates, base_value, base_target, end_period_target, grain, story_type
    ) -> list[dict[str, Any]]:
        """Generate historical data points"""
        time_series = []
        current_value = base_value
        is_day_grain = grain == Granularity.DAY
        is_day_grain_off_track = is_day_grain and story_type == StoryType.LIKELY_OFF_TRACK

        total_points = len(formatted_dates)

        # For day grain, limit growth to prevent values from getting too high
        max_value_multiplier = 1.8 if is_day_grain_off_track else 2.2
        max_value = base_value * max_value_multiplier

        # For day grain off-track, we'll use different pattern types
        pattern_type = getattr(self, "pattern_type", "moderate_decline") if is_day_grain_off_track else None

        # Mix up the pattern based on the selected type
        if is_day_grain_off_track:
            if pattern_type == "moderate_decline":
                # More gradual, smoother growth pattern
                decline_start = 0.82  # Start decline later
                plateau_phase = False  # No plateau
            elif pattern_type == "sharp_decline":
                # Sharper growth and decline
                decline_start = 0.78  # Start decline earlier
                plateau_phase = False  # No plateau
            else:  # plateau_then_drop
                # Growth, plateau, then drop
                decline_start = 0.85  # Start decline quite late
                plateau_phase = True  # Include plateau
        else:
            # Default values for other cases
            decline_start = 0.80
            plateau_phase = False

        for i, date_str in enumerate(formatted_dates):
            # Add trend and noise to value
            if i > 0:
                # Calculate position in the sequence (0 to 1)
                position = i / (total_points - 1)

                # For day grain off-track, use the selected pattern
                if is_day_grain_off_track:
                    # Growth phase
                    if position < decline_start - 0.15 if plateau_phase else decline_start:
                        # Growth phase: moderate growth with randomness
                        growth_factor = 1 - (current_value / max_value)  # Slow down as we approach max
                        trend = random.uniform(0.001, 0.008) * base_value * growth_factor  # noqa
                    # Plateau phase (if applicable)
                    elif plateau_phase and position < decline_start:
                        # Plateau phase: minimal growth/plateau
                        trend = random.uniform(-0.002, 0.003) * base_value  # noqa
                    # Decline phase
                    else:
                        # Decline phase: gradual decline with randomness
                        # Vary the decline rate based on pattern type
                        if pattern_type == "sharp_decline":
                            trend = random.uniform(-0.012, -0.006) * base_value  # noqa
                        else:
                            trend = random.uniform(-0.008, -0.002) * base_value  # noqa
                elif is_day_grain:
                    # For day grain on-track, still controlled growth but higher ceiling
                    growth_factor = 1 - (current_value / max_value)
                    if position < 0.80:
                        trend = random.uniform(0.001, 0.01) * base_value * growth_factor  # noqa
                    else:
                        trend = random.uniform(-0.002, 0.008) * base_value * growth_factor  # noqa
                else:
                    # Normal growth trend for other grains
                    trend = random.uniform(-0.02, 0.04) * base_value  # noqa

                # For day grain, use smaller noise to create smoother curves
                if is_day_grain:
                    noise = random.uniform(-0.003, 0.003) * base_value  # noqa
                else:
                    noise = random.uniform(-0.05, 0.05) * base_value  # noqa

                # Update value
                current_value += trend + noise

                # Ensure value doesn't exceed max for day grain
                if is_day_grain and current_value > max_value:
                    current_value = max_value * random.uniform(0.97, 1.0)  # noqa

            # Calculate target with progression toward end period target
            progress = i / (len(formatted_dates) - 1) if len(formatted_dates) > 1 else 0
            target = base_target + progress * (end_period_target - base_target)

            # Add point to time series
            time_series.append(
                {"date": date_str, "value": round(current_value), "target": round(target), "forecasted": False}
            )

        return time_series

    def _add_forecast_points(
        self, time_series, grain, start_date, end_date, last_value, final_value, base_target, end_target, story_type
    ) -> None:
        """Add forecasted points to the time series"""
        forecast_dates = self._get_forecast_dates(grain, start_date, end_date)
        formatted_forecast_dates = self.data_service.get_formatted_dates(forecast_dates)
        min_value = 0

        if not forecast_dates:
            return

        # Historical points already in time series
        historical_count = len(time_series)
        forecast_count = len(formatted_forecast_dates)
        total_points = historical_count + forecast_count

        # For day grain with LIKELY_OFF_TRACK, we need a sharper initial decline
        is_day_grain_off_track = grain == Granularity.DAY and story_type == StoryType.LIKELY_OFF_TRACK

        # For day grain off-track, ensure the final value doesn't go below 40% of the target
        # This is the simple fix to prevent negative or too-low values
        if is_day_grain_off_track:
            min_value = end_target * 0.4  # Set a floor at 40% of target
            if final_value < min_value:
                final_value = min_value

        # Calculate the appropriate curve shape based on the grain and story type
        if is_day_grain_off_track:
            # For day grain and LIKELY_OFF_TRACK, create a much sharper initial decline
            # The decline is frontloaded so it's immediately visible
            for i, date_str in enumerate(formatted_forecast_dates):
                # Use a steep initial decline approach
                progress = i / (forecast_count - 1) if forecast_count > 1 else 1

                # Apply a steep initial decline function
                if progress < 0.3:
                    # First 30% of forecast points: steep 60% decline
                    decline_factor = progress * 2.0  # 0 to 60% decline in first 30% of points
                else:
                    # Remaining points: more gradual approach to final value
                    decline_factor = 0.6 + (progress - 0.3) * 0.4 / 0.7  # Remaining 40% over rest of points

                # Calculate forecast value with frontloaded decline
                # Ensure the value doesn't go below our minimum (simple fix)
                forecast_value = max(last_value - decline_factor * (last_value - final_value), min_value)

                # Targets follow a more linear progression
                target_progress = (historical_count + i) / (total_points - 1) if total_points > 1 else 1
                forecast_target = base_target + target_progress * (end_target - base_target)

                # Add forecasted point
                time_series.append(
                    {
                        "date": date_str,
                        "value": round(forecast_value),
                        "target": round(forecast_target),
                        "forecasted": True,
                    }
                )
        else:
            # For other grains or LIKELY_ON_TRACK, use simple linear interpolation
            for i, date_str in enumerate(formatted_forecast_dates):
                # Calculate progress toward final forecasted value
                progress = (i + 1) / forecast_count

                # Interpolate value between last historical and final forecasted
                forecast_value = last_value + progress * (final_value - last_value)

                # Interpolate target based on overall progression
                target_progress = (historical_count + i) / (total_points - 1) if total_points > 1 else 1
                forecast_target = base_target + target_progress * (end_target - base_target)

                # Add forecasted point
                time_series.append(
                    {
                        "date": date_str,
                        "value": round(forecast_value),
                        "target": round(forecast_target),
                        "forecasted": True,
                    }
                )

    def get_mock_variables(
        self,
        metric: dict[str, Any],
        story_type: StoryType,
        grain: Granularity,
    ) -> dict[str, Any]:
        """Generate mock variables for likely status stories"""

        # Calculate deviation based on story type
        if story_type == StoryType.LIKELY_ON_TRACK:
            deviation = ((self.forecasted_value - self.target_value) / self.target_value) * 100
        else:  # LIKELY_OFF_TRACK
            deviation = ((self.target_value - self.forecasted_value) / self.target_value) * 100

        return {
            "metric": {"id": metric["id"], "label": metric["label"]},
            "grain": grain.value,
            "interval": self.interval.value,
            "forecasted_value": self.forecasted_value,
            "target": self.target_value,
            "deviation": round(abs(deviation), 2),
        }

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
