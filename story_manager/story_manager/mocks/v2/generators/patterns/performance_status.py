"""
Performance Status Pattern Result Generator
"""

import random

from levers.models import MetricGVAStatus
from levers.models.patterns import MetricPerformance
from levers.models.patterns.performance_status import HoldSteady, StatusChange, Streak

from .base import PatternResultGeneratorBase


class PerformanceStatusPatternGenerator(PatternResultGeneratorBase):
    """Generator for MetricPerformance (PerformanceStatus) pattern results"""

    def generate(self) -> MetricPerformance:
        """Generate a realistic MetricPerformance result"""

        # Generate current and prior values
        current_value = self._generate_random_value(1000, 10000)
        prior_value = self._generate_realistic_variation(current_value, 25)
        prior_value = self._ensure_positive(prior_value)

        # Generate target value and determine status
        target_value = self._generate_realistic_variation(current_value, 15)
        target_value = self._ensure_positive(target_value)
        status = MetricGVAStatus.ON_TRACK if current_value >= target_value else MetricGVAStatus.OFF_TRACK

        # Calculate deltas
        absolute_delta = current_value - prior_value
        pop_change_percent = (absolute_delta / prior_value) * 100

        # Generate status-specific fields
        absolute_gap = None
        percent_gap = None
        absolute_over_performance = None
        percent_over_performance = None

        if status == MetricGVAStatus.OFF_TRACK:
            absolute_gap = target_value - current_value
            percent_gap = (absolute_gap / target_value) * 100
        else:
            absolute_over_performance = current_value - target_value
            percent_over_performance = (absolute_over_performance / target_value) * 100

        # Generate status change
        status_change = self._generate_status_change(status)

        # Generate streak
        streak = self._generate_streak(status)

        # Generate hold steady info
        hold_steady = self._generate_hold_steady(status, current_value, target_value)

        return MetricPerformance(
            metric_id=self.metric_id,
            analysis_window=self.analysis_window,
            analysis_date=self.analysis_date,
            current_value=current_value,
            prior_value=prior_value,
            absolute_delta_from_prior=absolute_delta,
            pop_change_percent=pop_change_percent,
            target_value=target_value,
            status=status,
            absolute_gap=absolute_gap,
            percent_gap=percent_gap,
            absolute_over_performance=absolute_over_performance,
            percent_over_performance=percent_over_performance,
            status_change=status_change,
            streak=streak,
            hold_steady=hold_steady,
        )

    def _generate_status_change(self, current_status: MetricGVAStatus) -> StatusChange:
        """Generate status change information"""
        has_flipped = random.random() > 0.7  # 30% chance of status change

        if has_flipped:
            old_status = (
                MetricGVAStatus.OFF_TRACK if current_status == MetricGVAStatus.ON_TRACK else MetricGVAStatus.ON_TRACK
            )
            return StatusChange(
                has_flipped=True,
                old_status=old_status.value,
                new_status=current_status.value,
                old_status_duration_grains=random.randint(3, 12),
            )
        else:
            return StatusChange(
                has_flipped=False,
                old_status=None,
                new_status=current_status.value,
                old_status_duration_grains=None,
            )

    def _generate_streak(self, status: MetricGVAStatus) -> Streak:
        """Generate streak information"""
        return Streak(
            length=random.randint(2, 8),
            status=status,
            performance_change_percent_over_streak=self._generate_random_percentage(-20, 30),
            absolute_change_over_streak=self._generate_random_value(-1000, 2000),
            average_change_percent_per_grain=self._generate_random_percentage(-5, 8),
            average_change_absolute_per_grain=self._generate_random_value(-200, 400),
        )

    def _generate_hold_steady(
        self, status: MetricGVAStatus, current_value: float, target_value: float
    ) -> HoldSteady | None:
        """Generate hold steady scenario"""
        if status == MetricGVAStatus.ON_TRACK and random.random() > 0.6:  # 40% chance for on-track metrics
            return HoldSteady(
                is_currently_at_or_above_target=True,
                time_to_maintain_grains=random.randint(2, 6),
                current_margin_percent=((current_value - target_value) / target_value) * 100,
            )

        return None
