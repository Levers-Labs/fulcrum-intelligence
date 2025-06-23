"""
Mock Historical Performance Pattern Generator

Generates mock HistoricalPerformance pattern results that simulate historical performance analysis.
"""

import logging
import random
from datetime import date, timedelta
from typing import Any

import pandas as pd

from commons.models.enums import Granularity
from levers.models import ComparisonType
from levers.models.patterns import (
    Benchmark,
    BenchmarkComparison,
    HistoricalPerformance,
    PeriodMetrics,
    TrendAnalysis,
)
from story_manager.mocks.v2.pattern_generators.base import MockPatternGeneratorBase

logger = logging.getLogger(__name__)


class MockHistoricalPerformanceGenerator(MockPatternGeneratorBase):
    """Generator for mock historical performance pattern results."""

    pattern_name = "historical_performance"
    rank = 1
    _predetermined_central_line = float = 0.0

    def generate_pattern_results(
        self, metric: dict[str, Any], grain: Granularity, story_date: date, series_df: pd.DataFrame | None = None
    ) -> list[HistoricalPerformance]:
        """
        Generate mock historical performance pattern results using compatible story groups.

        This method implements the grouping approach:
        - [Upward, spike, improving performance, record high, accelerating growth, benchmark]
        - [Downward, drop, worsening, record low, slowing, benchmark]
        - [Performance plateau, worsening, slowing, benchmark]
        - [Stable, improving performance, benchmark]
        """

        # Define compatible story groups with their series patterns
        story_groups = [
            {
                "name": "upward_momentum",
                "series_pattern": "upward_trend",
                "stories": [
                    "new_upward_trend",
                    "spike",
                    "improving_performance",
                    "record_high",
                    "accelerating_growth",
                    "benchmark_comparison",
                ],
            },
            {
                "name": "downward_momentum",
                "series_pattern": "downward_trend",
                "stories": [
                    "new_downward_trend",
                    "drop",
                    "worsening_performance",
                    "record_low",
                    "slowing_growth",
                    "benchmark_comparison",
                ],
            },
            {
                "name": "plateau_pattern",
                "series_pattern": "plateau",
                "stories": ["performance_plateau", "worsening_performance", "slowing_growth", "benchmark_comparison"],
            },
            {
                "name": "stable_pattern",
                "series_pattern": "stable",
                "stories": ["stable_trend", "improving_performance", "benchmark_comparison"],
            },
        ]

        # Randomly select one compatible group
        selected_group = random.choice(story_groups)  # noqa

        # Generate series data using the group's pattern
        if series_df is None:
            series_df = self.generate_mock_series_data_for_pattern(
                metric, grain, story_date, selected_group["series_pattern"]  # type: ignore
            )

        # Create base pattern result
        pattern_result = self._create_base_pattern_result_from_series(metric, grain, story_date, series_df)

        # Add data for ALL stories in the selected group
        for story_type in selected_group["stories"]:
            if story_type == "new_upward_trend":
                self._add_new_upward_trend_data(pattern_result, series_df)
            elif story_type == "new_downward_trend":
                self._add_new_downward_trend_data(pattern_result, series_df)
            elif story_type == "spike":
                self._add_spike_data(pattern_result, series_df)
            elif story_type == "drop":
                self._add_drop_data(pattern_result, series_df)
            elif story_type == "improving_performance":
                self._add_improving_performance_data(pattern_result, series_df)
            elif story_type == "worsening_performance":
                self._add_worsening_performance_data(pattern_result, series_df)
            elif story_type == "record_high":
                self._add_record_high_data(pattern_result, series_df)
            elif story_type == "record_low":
                self._add_record_low_data(pattern_result, series_df)
            elif story_type == "accelerating_growth":
                self._add_accelerating_growth_data(pattern_result, series_df)
            elif story_type == "slowing_growth":
                self._add_slowing_growth_data(pattern_result, series_df)
            elif story_type == "performance_plateau":
                self._add_plateau_data(pattern_result, series_df)
            elif story_type == "stable_trend":
                self._add_stable_trend_data(pattern_result, series_df)
            elif story_type == "benchmark_comparison" and grain in [Granularity.WEEK, Granularity.MONTH]:
                self._add_benchmark_data(pattern_result, grain, series_df)

        try:
            result = HistoricalPerformance(**pattern_result)
            return [result]
        except Exception as e:
            logger.error(f"Error creating HistoricalPerformance: {e}")
            return []

    def generate_mock_series_data_for_pattern(
        self, metric: dict[str, Any], grain: Granularity, story_date: date, series_pattern: str, num_periods: int = 12
    ) -> pd.DataFrame:
        """
        Generate mock time series data based on a specific pattern that supports compatible story types.

        This method uses a proper Wheeler's rules-compliant approach:
        1. Pre-determine central line based on realistic business values
        2. Generate values that naturally satisfy Wheeler's rules from the start
        3. Ensure consistency between series data and trend analysis

        Args:
            metric: Metric dictionary
            grain: Granularity for the series
            story_date: The story date (current period - NOT included in series)
            series_pattern: The pattern type ("upward_trend", "downward_trend", "plateau", "stable")
            num_periods: Number of historical periods to generate (default: 12)

        Returns:
            DataFrame with time series data that matches the specified pattern
        """
        # Generate date range - always create exactly num_periods data points
        if grain == Granularity.DAY:
            freq = "D"
            end_date = story_date - timedelta(days=1)
            date_range = pd.date_range(end=end_date, periods=num_periods, freq=freq)
        elif grain == Granularity.WEEK:
            freq = "W-MON"
            days_since_monday = story_date.weekday()
            end_date = story_date - timedelta(days=days_since_monday + 7)
            date_range = pd.date_range(end=end_date, periods=num_periods, freq=freq)
        else:  # MONTH
            freq = "MS"
            if story_date.month == 1:
                end_date = date(story_date.year - 1, 12, 1)
            else:
                end_date = date(story_date.year, story_date.month - 1, 1)
            date_range = pd.date_range(end=end_date, periods=num_periods, freq=freq)

        # Generate base value and pre-determine central line
        base_value = self._generate_mock_value(1000)

        # Pre-calculate central line that will work with Wheeler's rules
        # This ensures consistency and proper rule satisfaction
        if series_pattern == "upward_trend":
            # Central line should be below the last 7 points
            # Set central line at ~70% of base value, last 7 points will be above it
            central_line = base_value * 0.75
            values = self._generate_upward_trend_values(central_line, num_periods)

        elif series_pattern == "downward_trend":
            # Central line should be above the last 7 points
            # Set central line at ~130% of base value, last 7 points will be below it
            central_line = base_value * 1.3
            values = self._generate_downward_trend_values(central_line, num_periods)

        elif series_pattern == "plateau":
            # Central line near base value, last 6 points cluster around it
            central_line = base_value
            values = self._generate_plateau_values(central_line, num_periods)

        else:  # stable
            # Central line at base value, points fluctuate around it
            central_line = base_value
            values = self._generate_stable_values(central_line, num_periods)

        # Ensure all values are positive
        values = [max(0.1, v) for v in values]

        # Create DataFrame
        series_df = pd.DataFrame({"date": date_range, "value": values})

        # Store central line for trend analysis consistency
        self._predetermined_central_line = central_line  # type: ignore

        return series_df

    def generate_mock_series_data(
        self, metric: dict[str, Any], grain: Granularity, story_date: date, num_periods: int = 12
    ) -> pd.DataFrame:
        """
        Generate mock time series data (compatibility method for main.py).

        This method is called by main.py and delegates to the pattern-based method.
        Since we don't know which pattern will be selected at this point, we generate
        a default stable pattern that works for all story types.

        Args:
            metric: Metric dictionary
            grain: Granularity for the series
            story_date: The story date (current period - NOT included in series)
            num_periods: Number of historical periods to generate (default: 12)

        Returns:
            DataFrame with time series data
        """
        # Use stable pattern as default since it works with all story types
        return self.generate_mock_series_data_for_pattern(metric, grain, story_date, "stable", num_periods)

    def _generate_upward_trend_values(self, central_line: float, num_periods: int) -> list[float]:  # type: ignore
        """Generate values for upward trend that satisfy Wheeler's Rule 2.

        Wheeler's Rule 2: 7 consecutive points above central line indicates upward trend.
        This method ensures NATURAL progression with ALL last 7 points above central line.
        """
        values: list = []

        # Strategy: Start low, gradually increase, ensure last 7 are above central line
        # Generate first points (mostly below central line to show contrast)
        first_points = num_periods - 7

        # Start with values below central line (realistic business recovery scenario)
        for i in range(first_points):
            # Increasing trend from low values up towards central line
            progress = i / max(1, first_points - 1)  # 0 to 1
            start_factor = 0.75  # Start 25% below central line
            end_factor = 0.98  # End just below central line

            base_factor = start_factor + (end_factor - start_factor) * progress
            variation = random.uniform(-0.05, 0.03)  # noqa  # Small variation with slight downward bias

            value = central_line * (base_factor + variation)
            values.append(value)

        # Generate last 7 points: ALL must be above central line (Wheeler's Rule 2)
        for i in range(first_points, num_periods):
            position_in_growth = i - first_points  # 0 to 6

            # Ensure progressively higher values, all above central line
            # Start just above central line, go progressively higher
            min_above_factor = 1.02 + (position_in_growth * 0.02)  # Start 2% above, go to 14% above
            max_above_factor = min_above_factor + 0.08  # 8% range for variation

            above_factor = random.uniform(min_above_factor, max_above_factor)  # noqa
            value = central_line * above_factor

            # Add small realistic noise but keep above central line
            noise = random.uniform(-0.01, 0.02)  # noqa  # Small positive bias
            value = value * (1 + noise)

            # Safety check: ensure it's above central line
            value = max(value, central_line * 1.01)

            values.append(value)

        return values

    def _generate_downward_trend_values(self, central_line: float, num_periods: int) -> list[float]:  # type: ignore
        """Generate values for downward trend that satisfy Wheeler's Rule 2.

        Wheeler's Rule 2: 7 consecutive points below central line indicates downward trend.
        This method ensures NATURAL progression with ALL last 7 points below central line.
        """
        values: list = []

        # Strategy: Start high, gradually decrease, ensure last 7 are below central line
        # Generate first points (mostly above central line to show contrast)
        first_points = num_periods - 7

        # Start with values above central line (realistic business scenario)
        for i in range(first_points):
            # Decreasing trend from high values down towards central line
            progress = i / max(1, first_points - 1)  # 0 to 1
            start_factor = 1.25  # Start 25% above central line
            end_factor = 1.02  # End just above central line

            base_factor = start_factor - (start_factor - end_factor) * progress
            variation = random.uniform(-0.03, 0.05)  # noqa  # Small variation with slight upward bias

            value = central_line * (base_factor + variation)
            values.append(value)

        # Generate last 7 points: ALL must be below central line (Wheeler's Rule 2)
        for i in range(first_points, num_periods):
            position_in_decline = i - first_points  # 0 to 6

            # Ensure progressively lower values, all below central line
            # Start just below central line, go progressively lower
            max_below_factor = 0.98 - (position_in_decline * 0.02)  # Start 2% below, go to 14% below
            min_below_factor = max_below_factor - 0.08  # 8% range for variation

            below_factor = random.uniform(min_below_factor, max_below_factor)  # noqa
            value = central_line * below_factor

            # Add small realistic noise but keep below central line
            noise = random.uniform(-0.02, 0.01)  # noqa  # Small negative bias
            value = value * (1 + noise)

            # Safety check: ensure it's below central line
            value = min(value, central_line * 0.99)

            values.append(value)

        return values

    def _generate_plateau_values(self, central_line: float, num_periods: int) -> list[float]:  # type: ignore
        """Generate values for plateau pattern with minimal variation."""
        values: list = []

        # Generate first points with normal variation
        for _ in range(num_periods - 6):
            variation = random.uniform(-0.08, 0.08)  # noqa  # ±8% variation
            value = central_line * (1 + variation)
            values.append(value)

        # Generate last 6 points: plateau with minimal variation
        for _ in range(num_periods - 6, num_periods):
            # Very small variation around central line for plateau
            variation = random.uniform(-0.03, 0.03)  # noqa  # ±3% variation
            value = central_line * (1 + variation)
            values.append(value)

        return values

    def _generate_stable_values(self, central_line: float, num_periods: int) -> list[float]:  # type: ignore
        """Generate values for stable pattern with consistent fluctuations."""
        values: list = []

        for i in range(num_periods):
            # Stable pattern with realistic market-like fluctuations
            # Random walk around central line
            if i == 0:
                variation = random.uniform(-0.05, 0.05)  # noqa  # ±5% initial variation
            else:
                # Small period-to-period changes
                prev_variation = (values[i - 1] - central_line) / central_line
                change = random.uniform(-0.03, 0.03)  # noqa  # ±3% change
                variation = prev_variation + change
                # Keep within reasonable bounds
                variation = max(-0.15, min(0.15, variation))

            value = central_line * (1 + variation)
            values.append(value)

        return values

    def _calculate_scenario_growth_rate(self, scenario_type: str, period_index: int, total_periods: int) -> Any:
        """Calculate growth rate based on scenario type and period position."""
        progress = period_index / total_periods
        # Add realistic business variation
        noise = random.uniform(-0.002, 0.002)  # noqa Very minimal noise

        if scenario_type == "accelerating":
            # Start with low growth, gradually accelerate with some variation
            base_growth = random.uniform(0.01, 0.025)  # noqa 1-2.5% base growth
            # Smooth acceleration curve with some randomness
            acceleration_factor = 1 + (progress**1.5) * random.uniform(0.8, 1.2)  # noqa More conservative acceleration
            return base_growth * acceleration_factor + noise

        elif scenario_type == "slowing":
            # Start with higher growth, gradually slow down but ALWAYS POSITIVE
            initial_growth = random.uniform(0.05, 0.07)  # noqa 5-7% initial growth
            # Smooth deceleration but never go below 1%
            deceleration_factor = 1 - (progress**1.2) * random.uniform(0.4, 0.6)  # noqa More conservative decline
            current_growth = initial_growth * max(deceleration_factor, 0.2)  # Never below 20% of initial (1-1.4%)
            return max(current_growth + noise, 0.01)  # Ensure minimum 1% growth

        elif scenario_type == "spike":
            # ALWAYS POSITIVE growth - spike is handled by UCL adjustment later
            base_growth = random.uniform(0.02, 0.04)  # noqa Normal base growth
            if progress > 0.9:  # Only in the very last period
                return base_growth + random.uniform(0.01, 0.02)  # noqa Slight increase for spike buildup
            elif progress > 0.7:  # Slight buildup in last 30%
                return base_growth * random.uniform(1.05, 1.15)  # noqa Very gradual buildup
            else:
                return base_growth + noise

        elif scenario_type == "drop":
            # ALWAYS POSITIVE growth - drop is handled by LCL adjustment later
            base_growth = random.uniform(0.02, 0.04)  # noqa Normal base growth
            if progress > 0.9:  # Only in the very last period
                return base_growth * random.uniform(0.7, 0.9)  # noqa Slower growth but still positive
            elif progress > 0.7:  # Slight decline in last 30%
                return base_growth * random.uniform(0.85, 0.95)  # noqa Very gradual slowdown
            else:
                return base_growth + noise

        elif scenario_type == "record_high":
            # Consistent strong growth with gradual acceleration
            base_growth = random.uniform(0.025, 0.04)  # noqa 2.5-4% base growth
            acceleration = 1 + (progress**1.2) * random.uniform(0.2, 0.4)  # noqa Gradual acceleration
            return base_growth * acceleration + noise

        elif scenario_type == "record_low":
            # Gradual decline but ALWAYS POSITIVE
            base_growth = random.uniform(0.02, 0.03)  # noqa Start with some growth
            decline_factor = 1 - (progress**1.5) * random.uniform(0.3, 0.5)  # noqa Very gradual decline
            return max(base_growth * max(decline_factor, 0.3) + noise, 0.005)  # Never below 0.5%

        elif scenario_type == "upward_trend":
            # Consistent upward growth with gradual acceleration
            base_growth = random.uniform(0.02, 0.035)  # noqa 2-3.5% base growth
            trend_factor = 1 + (progress**0.8) * random.uniform(0.15, 0.3)  # noqa Gradual acceleration
            return base_growth * trend_factor + noise

        elif scenario_type == "downward_trend":
            # Declining trend but ALWAYS POSITIVE
            base_growth = random.uniform(0.025, 0.04)  # noqa Start with decent growth
            # Very conservative worsening - businesses adapt and don't just crash
            trend_factor = 1 - (progress**1.3) * random.uniform(0.3, 0.5)  # noqa Gradual decline
            return max(base_growth * max(trend_factor, 0.4) + noise, 0.01)  # Never below 1%

        elif scenario_type == "plateau":
            # Minimal but POSITIVE changes throughout
            return random.uniform(0.005, 0.015) + noise  # noqa Small positive changes only

        elif scenario_type == "improving":
            # Gradual improvement with realistic variation
            base_growth = random.uniform(0.02, 0.035)  # noqa 2-3.5% base growth
            improvement_factor = 1 + (progress**0.8) * random.uniform(0.3, 0.5)  # noqa Gradual improvement
            return base_growth * improvement_factor + noise

        elif scenario_type == "worsening":
            # Gradual worsening but ALWAYS POSITIVE
            base_growth = random.uniform(0.025, 0.04)  # noqa Start with decent growth
            # Conservative worsening - businesses don't typically crash, they adapt
            worsening_factor = 1 - (progress**1.2) * random.uniform(0.3, 0.5)  # noqa Gradual decline
            return max(base_growth * max(worsening_factor, 0.5) + noise, 0.01)  # Never below 1%

        elif scenario_type == "benchmark":
            # Mixed performance for benchmark comparison stories
            if progress < 0.3:
                return random.uniform(0.02, 0.04) + noise  # noqa Early growth
            elif progress < 0.7:
                return random.uniform(0.01, 0.03) + noise  # noqa Middle stability
            else:
                return random.uniform(0.03, 0.05) + noise  # noqa Recent strong performance

        else:  # "stable" or unknown
            # Consistent growth with some variation
            return random.uniform(0.02, 0.035) + noise  # noqa

    def generate_mock_period_metrics(self, series_df: pd.DataFrame) -> list:
        """
        Generate period metrics that align with the series data.

        The key insight is that period_end must match the series data dates
        so that the evaluator can successfully merge them.

        Args:
            series_df: The generated series DataFrame with date and value columns
            grain: Granularity for period calculation

        Returns:
            List of PeriodMetrics objects with calculated pop_growth_percent
        """

        period_metrics = []

        # Add the first period with 0% growth (no previous period to compare to)
        if len(series_df) > 0:
            first_row = series_df.iloc[0]
            # Handle date formatting properly for pandas Timestamp
            first_date = first_row["date"]
            if hasattr(first_date, "strftime"):
                first_date_str = first_date.strftime("%Y-%m-%d")
            elif hasattr(first_date, "date"):
                first_date_str = first_date.date().isoformat()
            else:
                first_date_str = str(first_date)

            period_metrics.append(
                PeriodMetrics(
                    period_start=first_date_str,
                    period_end=first_date_str,
                    pop_growth_percent=0.0,  # First period always has 0% growth
                    pop_acceleration_percent=0.0,
                )
            )

        # Generate period metrics for each subsequent period in the series
        for i in range(1, len(series_df)):
            current_row = series_df.iloc[i]
            previous_row = series_df.iloc[i - 1]

            # CRITICAL: period_end must match the series data date for evaluator merge to work
            # The evaluator merges period_metrics by period_end with series_df by date
            # Handle date formatting properly for pandas Timestamp
            current_date = current_row["date"]
            previous_date = previous_row["date"]

            if hasattr(current_date, "strftime"):
                period_end = current_date.strftime("%Y-%m-%d")
            elif hasattr(current_date, "date"):
                period_end = current_date.date().isoformat()
            else:
                period_end = str(current_date)

            if hasattr(previous_date, "strftime"):
                period_start = previous_date.strftime("%Y-%m-%d")
            elif hasattr(previous_date, "date"):
                period_start = previous_date.date().isoformat()
            else:
                period_start = str(previous_date)

            # Calculate pop_growth_percent from the series data values
            current_value = current_row["value"]
            previous_value = previous_row["value"]

            if previous_value != 0:
                pop_growth = ((current_value - previous_value) / previous_value) * 100
            else:
                pop_growth = 0.0

            # Ensure pop_growth is not null
            if pd.isna(pop_growth):
                pop_growth = 0.0

            # Calculate acceleration (change in growth rate) - avoid nulls
            pop_acceleration = 0.0  # Default to 0.0 instead of None
            if i > 1:
                # Calculate previous period growth
                prev_current_value = previous_row["value"]
                prev_previous_value = series_df.iloc[i - 2]["value"]

                if prev_previous_value != 0:
                    prev_growth = ((prev_current_value - prev_previous_value) / prev_previous_value) * 100
                    if not pd.isna(prev_growth):
                        pop_acceleration = pop_growth - prev_growth

            period_metrics.append(
                PeriodMetrics(
                    period_start=period_start,
                    period_end=period_end,  # This must match series_df dates!
                    pop_growth_percent=pop_growth,
                    pop_acceleration_percent=pop_acceleration,
                )
            )

        return period_metrics

    def generate_mock_trend_analysis(self, series_df: pd.DataFrame) -> list:
        """
        Generate trend analysis objects that align with the series data.

        Args:
            series_df: The generated series DataFrame with date and value columns

        Returns:
            List of TrendAnalysis objects with calculated SPC metrics
        """

        trend_analysis = []

        # Calculate SPC metrics from the series data - use proper SPC methodology
        # Use predetermined central line if available (from pattern generation), otherwise calculate
        if hasattr(self, "_predetermined_central_line"):
            central_line_value = self._predetermined_central_line
        else:
            # Calculate central line as overall mean (proper SPC approach)
            central_line_value = series_df["value"].mean()

        # Calculate moving ranges for control limits
        moving_ranges = []
        for i in range(1, len(series_df)):
            mr = abs(series_df.iloc[i]["value"] - series_df.iloc[i - 1]["value"])
            moving_ranges.append(mr)

        # Average moving range
        avg_moving_range = sum(moving_ranges) / len(moving_ranges) if moving_ranges else 0

        # Control limits using standard SPC multiplier (2.66 for moving ranges)
        ucl_value = central_line_value + (avg_moving_range * 2.66)
        lcl_value = central_line_value - (avg_moving_range * 2.66)

        # Ensure LCL is not negative for business metrics
        lcl_value = max(0, lcl_value)  # type: ignore

        # Calculate slope based on overall trend
        if len(series_df) > 1:
            total_change = (series_df["value"].iloc[-1] - series_df["value"].iloc[0]) / series_df["value"].iloc[0] * 100
            slope_per_period = total_change / len(series_df)
        else:
            slope_per_period = 0.0

        # Generate trend analysis for each data point in the series
        for i, (_, row) in enumerate(series_df.iterrows()):
            # Calculate slope change percent (small random variations)
            slope_change_percent = random.uniform(-2.0, 2.0) if i > 0 else 0.0  # noqa

            # Detect trend signals based on Wheeler's rules
            current_value = row["value"]

            # Rule 1: Point outside control limits
            outside_limits = (current_value > ucl_value) or (current_value < lcl_value)

            # Rule 2: 7 consecutive points above/below central line
            consecutive_signal = False
            if i >= 6:  # Need at least 7 points to check
                # Check last 7 points including current
                recent_points = series_df.iloc[i - 6 : i + 1]["value"]
                all_above = all(val > central_line_value for val in recent_points)  # type: ignore
                all_below = all(val < central_line_value for val in recent_points)  # type: ignore
                consecutive_signal = all_above or all_below

            # Additional pattern-based signal detection
            pattern_signal = False
            if hasattr(self, "selected_series_pattern"):
                if self.selected_series_pattern == "upward_trend" and i >= len(series_df) - 7:
                    # Last 7 points should trigger signals for upward trend
                    pattern_signal = True
                elif self.selected_series_pattern == "downward_trend" and i >= len(series_df) - 7:
                    # Last 7 points should trigger signals for downward trend
                    pattern_signal = True
                elif self.selected_series_pattern == "plateau" and i >= len(series_df) - 6:
                    # Last 6 points should trigger signals for plateau
                    pattern_signal = True
                elif self.selected_series_pattern == "stable":
                    # Stable trend should not trigger signals
                    pattern_signal = False

            # Signal detected if any rule is triggered
            trend_signal_detected = outside_limits or consecutive_signal or pattern_signal

            # Create trend analysis object - date must match series_df for evaluator merge
            # Handle date formatting properly for pandas Timestamp
            date_str = row["date"]
            if hasattr(date_str, "strftime"):
                date_str = date_str.strftime("%Y-%m-%d")
            elif hasattr(date_str, "date"):
                date_str = date_str.date().isoformat()
            else:
                date_str = str(date_str)

            trend_analysis.append(
                TrendAnalysis(
                    value=current_value,
                    date=date_str,  # Must match series_df dates!
                    central_line=central_line_value,
                    ucl=ucl_value,
                    lcl=lcl_value,
                    slope=slope_per_period,
                    slope_change_percent=slope_change_percent,
                    trend_signal_detected=trend_signal_detected,
                )
            )

        return trend_analysis

    def _create_base_pattern_result(
        self, metric: dict[str, Any], grain: Granularity, story_date: date
    ) -> dict[str, Any]:
        """Create a base pattern result with required fields including series data."""

        # Generate series data first
        series_df = self.generate_mock_series_data(metric, grain, story_date)

        # Generate period metrics and trend analysis using the series data
        period_metrics = self.generate_mock_period_metrics(series_df)
        trend_analysis = self.generate_mock_trend_analysis(series_df)

        # Get the last series value to ensure consistency
        last_series_value = series_df.iloc[-1]["value"]

        return {
            "pattern": self.pattern_name,
            "metric_id": str(metric.get("metric_id")),
            "analysis_date": story_date,
            "series_data": series_df.to_dict("records"),  # Convert DataFrame to list of dicts
            "analysis_window": {
                "start_date": (story_date - timedelta(days=365)).isoformat(),
                "end_date": story_date.isoformat(),
                "grain": grain.value,
            },
            "period_metrics": period_metrics,  # List of PeriodMetrics
            "trend_analysis": trend_analysis,  # List of TrendAnalysis
            # Initialize required fields with default values
            "growth_stats": {
                "current_pop_growth": 0.0,
                "average_pop_growth": 0.0,
                "current_growth_acceleration": 0.0,
                "num_periods_accelerating": 0,
                "num_periods_slowing": 0,
            },
            "high_rank": {
                "value": last_series_value,  # Use last series value for consistency
                "rank": 5,  # Mid-range default
                "duration_grains": 1,
            },
            "low_rank": {
                "value": last_series_value,  # Use last series value for consistency
                "rank": 5,  # Mid-range default
                "duration_grains": 1,
            },
        }

    def _create_base_pattern_result_from_series(
        self, metric: dict[str, Any], grain: Granularity, story_date: date, series_df: pd.DataFrame
    ) -> dict[str, Any]:
        """Create a base pattern result using existing series data for consistency."""

        # Generate period metrics and trend analysis using the provided series data
        period_metrics = self.generate_mock_period_metrics(series_df)
        trend_analysis = self.generate_mock_trend_analysis(series_df)

        # Get the last series value to ensure consistency
        last_series_value = series_df.iloc[-1]["value"]

        # Calculate actual growth statistics from series data
        if len(series_df) > 1:
            current_growth = (
                (series_df.iloc[-1]["value"] - series_df.iloc[-2]["value"]) / series_df.iloc[-2]["value"]
            ) * 100
            # Calculate average growth over the series
            total_growth = (
                (series_df.iloc[-1]["value"] - series_df.iloc[0]["value"]) / series_df.iloc[0]["value"]
            ) * 100
            average_growth = total_growth / len(series_df)
        else:
            current_growth = 0.0
            average_growth = 0.0

        # Calculate acceleration from actual period metrics if available
        acceleration = 0.0
        if len(period_metrics) > 1:
            current_period_growth = period_metrics[-1].pop_growth_percent
            previous_period_growth = period_metrics[-2].pop_growth_percent
            acceleration = current_period_growth - previous_period_growth

        return {
            "pattern": self.pattern_name,
            "metric_id": str(metric.get("metric_id")),
            "analysis_date": story_date,
            "series_data": series_df.to_dict("records"),  # Convert DataFrame to list of dicts
            "analysis_window": {
                "start_date": (story_date - timedelta(days=365)).isoformat(),
                "end_date": story_date.isoformat(),
                "grain": grain.value,
            },
            "period_metrics": period_metrics,  # List of PeriodMetrics
            "trend_analysis": trend_analysis,  # List of TrendAnalysis
            # Use actual calculated values from series data
            "growth_stats": {
                "current_pop_growth": current_growth,
                "average_pop_growth": average_growth,
                "current_growth_acceleration": acceleration,
                "num_periods_accelerating": 0,  # Will be set by specific story methods
                "num_periods_slowing": 0,  # Will be set by specific story methods
            },
            "high_rank": {
                "value": last_series_value,  # Use last series value for consistency
                "rank": 5,  # Mid-range default, will be updated by story methods
                "duration_grains": 1,
            },
            "low_rank": {
                "value": last_series_value,  # Use last series value for consistency
                "rank": 5,  # Mid-range default, will be updated by story methods
                "duration_grains": 1,
            },
        }

    def _add_accelerating_growth_data(
        self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None
    ) -> None:
        """Add data to satisfy accelerating growth story conditions."""
        if series_df is not None and len(series_df) > 2:
            # Count periods where growth is actually accelerating in the series
            accelerating_periods = 0
            growth_rates = []

            # Calculate growth rates for each period
            for i in range(1, len(series_df)):
                if series_df.iloc[i - 1]["value"] != 0:
                    growth_rate = (
                        (series_df.iloc[i]["value"] - series_df.iloc[i - 1]["value"]) / series_df.iloc[i - 1]["value"]
                    ) * 100
                    growth_rates.append(growth_rate)
                else:
                    growth_rates.append(0.0)

            # Count accelerating periods (where growth rate is increasing)
            for i in range(1, len(growth_rates)):
                if growth_rates[i] > growth_rates[i - 1]:
                    accelerating_periods += 1

            # For "speeding up" story, ensure we have positive acceleration and accelerating periods
            if accelerating_periods == 0:
                accelerating_periods = 2  # Minimum for a credible "speeding up" story

            # Ensure current acceleration is positive for "speeding up" story
            if len(growth_rates) >= 2:
                # Calculate actual acceleration from last two periods
                current_acceleration = growth_rates[-1] - growth_rates[-2]
                if current_acceleration <= 0:
                    # Force positive acceleration for "speeding up"
                    current_acceleration = abs(current_acceleration) + random.uniform(1.0, 3.0)  # noqa
                pattern_result["growth_stats"]["current_growth_acceleration"] = current_acceleration
            else:
                pattern_result["growth_stats"]["current_growth_acceleration"] = random.uniform(3.0, 8.0)  # noqa

            pattern_result["growth_stats"]["num_periods_accelerating"] = accelerating_periods

            # Update current growth to be the last calculated growth rate
            if growth_rates:
                pattern_result["growth_stats"]["current_pop_growth"] = growth_rates[-1]
        else:
            # Fallback to random values
            pattern_result["growth_stats"]["current_growth_acceleration"] = self._generate_percentage(3, 8)  # Positive
            pattern_result["growth_stats"]["current_pop_growth"] = self._generate_percentage(8, 15)
            pattern_result["growth_stats"]["num_periods_accelerating"] = random.randint(3, 6)  # noqa

    def _add_slowing_growth_data(self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None) -> None:
        """Add data to satisfy slowing growth story conditions."""
        if series_df is not None and len(series_df) > 2:
            # Count periods where growth is actually slowing in the series
            slowing_periods = 0
            growth_rates = []

            # Calculate growth rates for each period
            for i in range(1, len(series_df)):
                if series_df.iloc[i - 1]["value"] != 0:
                    growth_rate = (
                        (series_df.iloc[i]["value"] - series_df.iloc[i - 1]["value"]) / series_df.iloc[i - 1]["value"]
                    ) * 100
                    growth_rates.append(growth_rate)
                else:
                    growth_rates.append(0.0)

            # Count slowing periods (where growth rate is decreasing)
            for i in range(1, len(growth_rates)):
                if growth_rates[i] < growth_rates[i - 1]:
                    slowing_periods += 1

            if slowing_periods == 0:
                slowing_periods = 2  # Minimum for a credible "slowing" story

            # Ensure current acceleration is negative for "slowing" story
            if len(growth_rates) >= 2:
                current_acceleration = growth_rates[-1] - growth_rates[-2]
                if current_acceleration >= 0:
                    # Force negative acceleration for "slowing"
                    current_acceleration = -abs(current_acceleration) - random.uniform(1.0, 3.0)  # noqa
                pattern_result["growth_stats"]["current_growth_acceleration"] = current_acceleration
            else:
                pattern_result["growth_stats"]["current_growth_acceleration"] = random.uniform(-8.0, -2.0)  # noqa

            pattern_result["growth_stats"]["num_periods_slowing"] = slowing_periods

            # Update current growth to be the last calculated growth rate
            if growth_rates:
                pattern_result["growth_stats"]["current_pop_growth"] = growth_rates[-1]
        else:
            # Fallback to random values
            pattern_result["growth_stats"]["current_growth_acceleration"] = self._generate_percentage(-8, -2)
            pattern_result["growth_stats"]["current_pop_growth"] = self._generate_percentage(1, 5)
            pattern_result["growth_stats"]["num_periods_slowing"] = random.randint(3, 6)  # noqa

    def _add_stable_trend_data(self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None) -> None:
        """Add data to satisfy stable trend story conditions."""
        # Calculate actual average growth from series (should be small for stable pattern)
        if series_df is not None and len(series_df) > 1:
            total_growth = (
                (series_df.iloc[-1]["value"] - series_df.iloc[0]["value"]) / series_df.iloc[0]["value"]
            ) * 100
            avg_growth = total_growth / len(series_df)
        else:
            avg_growth = self._generate_percentage(-1, 1)  # Very small growth for stable

        pattern_result["current_trend"] = {
            "trend_type": "stable",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": avg_growth,
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_new_upward_trend_data(self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None) -> None:
        """Add data to satisfy new upward trend story conditions."""
        # Calculate actual growth from series (should be positive for upward pattern)
        if series_df is not None and len(series_df) > 1:
            mid_point = len(series_df) // 2
            recent_growth = (
                (series_df.iloc[-1]["value"] - series_df.iloc[mid_point]["value"]) / series_df.iloc[mid_point]["value"]
            ) * 100
            recent_growth = recent_growth / (len(series_df) - mid_point)  # Per period
        else:
            recent_growth = self._generate_percentage(3, 8)

        pattern_result["current_trend"] = {
            "trend_type": "upward",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": recent_growth,
            "duration_grains": random.randint(3, 6),  # noqa
        }
        # Add different previous trend to make it "new"
        pattern_result["previous_trend"] = {
            "trend_type": "stable",  # Different from current
            "start_date": (pattern_result["analysis_date"] - timedelta(days=90)).isoformat(),
            "average_pop_growth": self._generate_percentage(-1, 1),
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_new_downward_trend_data(
        self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None
    ) -> None:
        """Add data to satisfy new downward trend story conditions."""
        # Calculate actual growth from series (should be negative for downward pattern)
        if series_df is not None and len(series_df) > 1:
            mid_point = len(series_df) // 2
            recent_growth = (
                (series_df.iloc[-1]["value"] - series_df.iloc[mid_point]["value"]) / series_df.iloc[mid_point]["value"]
            ) * 100
            recent_growth = recent_growth / (len(series_df) - mid_point)  # Per period
        else:
            recent_growth = self._generate_percentage(-8, -3)

        pattern_result["current_trend"] = {
            "trend_type": "downward",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": recent_growth,
            "duration_grains": random.randint(3, 6),  # noqa
        }
        # Add different previous trend to make it "new"
        pattern_result["previous_trend"] = {
            "trend_type": "upward",  # Different from current
            "start_date": (pattern_result["analysis_date"] - timedelta(days=90)).isoformat(),
            "average_pop_growth": self._generate_percentage(2, 5),
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_plateau_data(self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None) -> None:
        """Add data to satisfy performance plateau story conditions."""
        # Calculate growth from recent plateau periods (should be very small)
        if series_df is not None and len(series_df) >= 4:
            recent_values = series_df.tail(4)
            growth_rates = []
            for i in range(1, len(recent_values)):
                if recent_values.iloc[i - 1]["value"] != 0:
                    gr = (
                        (recent_values.iloc[i]["value"] - recent_values.iloc[i - 1]["value"])
                        / recent_values.iloc[i - 1]["value"]
                    ) * 100
                    growth_rates.append(gr)
            avg_recent_growth = sum(growth_rates) / len(growth_rates) if growth_rates else 0
        else:
            avg_recent_growth = self._generate_percentage(-0.5, 0.5)  # Very small growth for plateau

        pattern_result["current_trend"] = {
            "trend_type": "plateau",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": avg_recent_growth,
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_spike_data(self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None) -> None:
        """Add data to satisfy spike story conditions.

        Spike definition: Current value significantly above UCL (Upper Control Limit).
        Ensures complete consistency between series_data, trend_analysis, and trend_exception.
        """
        if series_df is not None:
            # Get trend_analysis data which has the proper UCL/LCL from SPC analysis
            trend_analysis = pattern_result.get("trend_analysis", [])
            if not trend_analysis:
                return

            # Get the last trend analysis point (has proper UCL/LCL from SPC)
            last_trend_obj = trend_analysis[-1]
            ucl = last_trend_obj.ucl
            lcl = last_trend_obj.lcl

            # Force spike: 15-25% above UCL
            spike_factor = random.uniform(1.15, 1.25)  # noqa
            spike_value = ucl * spike_factor

            # Update series DataFrame (this affects series_data in pattern_result)
            series_df.iloc[-1, series_df.columns.get_loc("value")] = spike_value  # type: ignore

            # Update series_data in pattern_result to match
            pattern_result["series_data"][-1]["value"] = spike_value  # type: ignore

            # Update trend_analysis object with new value
            updated_trend = TrendAnalysis(
                value=spike_value,  # CRITICAL: Must match series value
                date=last_trend_obj.date,
                central_line=last_trend_obj.central_line,
                ucl=last_trend_obj.ucl,
                lcl=last_trend_obj.lcl,
                slope=last_trend_obj.slope,
                slope_change_percent=last_trend_obj.slope_change_percent,
                trend_signal_detected=True,  # Spike should trigger signal
            )
            trend_analysis[-1] = updated_trend

            # Calculate deviation
            deviation = spike_value - ucl  # type: ignore
            deviation_percent = (deviation / ucl * 100.0) if ucl != 0 else 0

            # Add trend exception data with PERFECTLY CONSISTENT values
            pattern_result["trend_exception"] = {
                "type": "Spike",  # Must match TrendExceptionType enum
                "current_value": spike_value,  # MUST match series AND trend_analysis value
                "normal_range_low": lcl,  # MUST match LCL from trend_analysis
                "normal_range_high": ucl,  # MUST match UCL from trend_analysis
                "absolute_delta_from_normal_range": deviation,
                "magnitude_percent": deviation_percent,
            }

    def _add_drop_data(self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None) -> None:
        """Add data to satisfy drop story conditions.

        Drop definition: Current value significantly below LCL (Lower Control Limit).
        Ensures complete consistency between series_data, trend_analysis, and trend_exception.
        """
        if series_df is not None:
            # Get trend_analysis data which has the proper UCL/LCL from SPC analysis
            trend_analysis = pattern_result.get("trend_analysis", [])
            if not trend_analysis:
                return

            # Get the last trend analysis point (has proper UCL/LCL from SPC)
            last_trend_obj = trend_analysis[-1]
            ucl = last_trend_obj.ucl
            lcl = last_trend_obj.lcl

            # Force drop: 15-25% below LCL
            drop_factor = random.uniform(0.75, 0.85)  # noqa
            drop_value = lcl * drop_factor

            # Ensure drop value is positive
            drop_value = max(0.1, drop_value)

            # Update series DataFrame (this affects series_data in pattern_result)
            series_df.iloc[-1, series_df.columns.get_loc("value")] = drop_value  # type: ignore

            # Update series_data in pattern_result to match
            pattern_result["series_data"][-1]["value"] = drop_value

            # Update trend_analysis object with new value
            updated_trend = TrendAnalysis(
                value=drop_value,  # CRITICAL: Must match series value
                date=last_trend_obj.date,
                central_line=last_trend_obj.central_line,
                ucl=last_trend_obj.ucl,
                lcl=last_trend_obj.lcl,
                slope=last_trend_obj.slope,
                slope_change_percent=last_trend_obj.slope_change_percent,
                trend_signal_detected=True,  # Drop should trigger signal
            )
            trend_analysis[-1] = updated_trend

            # Calculate deviation
            deviation = lcl - drop_value
            deviation_percent = (deviation / abs(lcl) * 100.0) if lcl != 0 else 0

            # Add trend exception data with PERFECTLY CONSISTENT values
            pattern_result["trend_exception"] = {
                "type": "Drop",  # Must match TrendExceptionType enum
                "current_value": drop_value,  # MUST match series AND trend_analysis value
                "normal_range_low": lcl,  # MUST match LCL from trend_analysis
                "normal_range_high": ucl,  # MUST match UCL from trend_analysis
                "absolute_delta_from_normal_range": -deviation,  # Negative for drops
                "magnitude_percent": deviation_percent,
            }

    def _add_improving_performance_data(
        self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None
    ) -> None:
        """Add data to satisfy improving performance story conditions."""
        # Calculate actual growth from series if available
        if series_df is not None and len(series_df) > 1:
            # Use recent growth to show improvement
            recent_growth = (
                (series_df.iloc[-1]["value"] - series_df.iloc[-2]["value"]) / series_df.iloc[-2]["value"]
            ) * 100
            # Ensure it's positive for improving performance
            if recent_growth <= 0:
                recent_growth = self._generate_percentage(5, 12)
        else:
            recent_growth = self._generate_percentage(5, 12)

        pattern_result["current_trend"] = {
            "trend_type": "upward",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": recent_growth,  # Positive for improving
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_worsening_performance_data(
        self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None
    ) -> None:
        """Add data to satisfy worsening performance story conditions."""
        # Calculate actual growth from series if available
        if series_df is not None and len(series_df) > 1:
            # Use recent growth to show worsening
            recent_growth = (
                (series_df.iloc[-1]["value"] - series_df.iloc[-2]["value"]) / series_df.iloc[-2]["value"]
            ) * 100
            # Ensure it's negative for worsening performance
            if recent_growth >= 0:
                recent_growth = self._generate_percentage(-12, -5)
        else:
            recent_growth = self._generate_percentage(-12, -5)

        pattern_result["current_trend"] = {
            "trend_type": "downward",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": recent_growth,  # Negative for worsening
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_record_high_data(self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None) -> None:
        """Add data to satisfy record high story conditions."""
        if series_df is not None:
            # Calculate actual rank from series data
            last_value = series_df.iloc[-1]["value"]
            all_values = series_df["value"].tolist()

            # Randomly decide if this should be rank 1 or 2
            self.rank = random.randint(1, 2)  # noqa

            if self.rank == 1:
                # Make current value the highest (record high)
                max_prev = max(all_values[:-1]) if len(all_values) > 1 else last_value
                new_current_value = max_prev * 1.05  # 5% higher than previous max

                # Update both series data and pattern result
                series_df.iloc[-1, series_df.columns.get_loc("value")] = new_current_value  # type: ignore
                pattern_result["series_data"][-1]["value"] = new_current_value
                pattern_result["high_rank"]["value"] = new_current_value

            else:  # rank == 2
                # Make current value the 2nd highest
                values_sorted = sorted(all_values[:-1], reverse=True)  # Exclude current value
                if len(values_sorted) >= 1:
                    # Set current value to be slightly less than the highest but higher than the rest
                    highest_value = values_sorted[0]
                    if len(values_sorted) >= 2:
                        second_highest = values_sorted[1]
                        new_current_value = (highest_value + second_highest) / 2 + (
                            highest_value - second_highest
                        ) * 0.1
                    else:
                        new_current_value = highest_value * 0.98  # 2% less than highest

                    # Update both series data and pattern result
                    series_df.iloc[-1, series_df.columns.get_loc("value")] = new_current_value  # type: ignore
                    pattern_result["series_data"][-1]["value"] = new_current_value
                    pattern_result["high_rank"]["value"] = new_current_value
                else:
                    # Fallback if not enough data
                    pattern_result["high_rank"]["value"] = last_value

            pattern_result["high_rank"]["rank"] = self.rank

    def _add_record_low_data(self, pattern_result: dict[str, Any], series_df: pd.DataFrame | None = None) -> None:
        """Add data to satisfy record low story conditions."""
        if series_df is not None:
            # Calculate actual rank from series data (for low values, rank 1 = lowest)
            last_value = series_df.iloc[-1]["value"]
            all_values = series_df["value"].tolist()

            # Randomly decide if this should be rank 1 or 2
            self.rank = random.randint(1, 2)  # noqa

            if self.rank == 1:
                # Make current value the lowest (record low)
                min_prev = min(all_values[:-1]) if len(all_values) > 1 else last_value
                new_current_value = min_prev * 0.95  # 5% lower than previous min

                # Update both series data and pattern result
                series_df.iloc[-1, series_df.columns.get_loc("value")] = new_current_value  # type: ignore
                pattern_result["series_data"][-1]["value"] = new_current_value
                pattern_result["low_rank"]["value"] = new_current_value

            else:  # rank == 2
                # Make current value the 2nd lowest
                values_sorted = sorted(all_values[:-1])  # Exclude current value, lowest to highest
                if len(values_sorted) >= 1:
                    # Set current value to be slightly higher than the lowest but lower than the rest
                    lowest_value = values_sorted[0]
                    if len(values_sorted) >= 2:
                        second_lowest = values_sorted[1]
                        new_current_value = (lowest_value + second_lowest) / 2 - (second_lowest - lowest_value) * 0.1
                    else:
                        new_current_value = lowest_value * 1.02  # 2% higher than lowest

                    # Update both series data and pattern result
                    series_df.iloc[-1, series_df.columns.get_loc("value")] = new_current_value  # type: ignore
                    pattern_result["series_data"][-1]["value"] = new_current_value
                    pattern_result["low_rank"]["value"] = new_current_value
                else:
                    # Fallback if not enough data
                    pattern_result["low_rank"]["value"] = last_value

            pattern_result["low_rank"]["rank"] = self.rank

    def _add_benchmark_data(
        self, pattern_result: dict[str, Any], grain: Granularity, series_df: pd.DataFrame | None = None
    ) -> None:
        """Add data to satisfy benchmark story conditions."""

        # Use current value from series data for consistency
        if series_df is not None:
            current_value = series_df.iloc[-1]["value"]

        analysis_date = pattern_result["analysis_date"]

        # Create benchmark comparison data with realistic scenarios
        benchmarks = {}

        if grain == Granularity.WEEK:
            comparison_types = [ComparisonType.LAST_WEEK, ComparisonType.WEEK_IN_LAST_MONTH]
            current_period = "This Week"
        else:  # MONTH
            comparison_types = [ComparisonType.LAST_MONTH, ComparisonType.MONTH_IN_LAST_QUARTER]
            current_period = "This Month"

        for comp_type in comparison_types:
            # Create realistic benchmark scenarios
            if comp_type in [ComparisonType.LAST_WEEK, ComparisonType.LAST_MONTH]:
                # Recent comparison - show some variation
                reference_value = current_value * random.uniform(0.85, 1.15)  # noqa
                ref_date = analysis_date - timedelta(days=7 if grain == Granularity.WEEK else 30)
                ref_period = "Week Ago" if grain == Granularity.WEEK else "Month Ago"
            else:
                # Longer period comparison - show more significant change
                reference_value = current_value * random.uniform(0.7, 1.3)  # noqa
                ref_date = analysis_date - timedelta(days=30 if grain == Granularity.WEEK else 90)
                ref_period = "Month Ago" if grain == Granularity.WEEK else "Quarter Ago"

            absolute_change = current_value - reference_value
            change_percent = (absolute_change / reference_value) * 100 if reference_value != 0 else 0

            benchmarks[comp_type] = Benchmark(
                reference_value=reference_value,
                reference_date=ref_date,
                reference_period=ref_period,
                absolute_change=absolute_change,
                change_percent=change_percent,
            )

        pattern_result["benchmark_comparison"] = BenchmarkComparison(
            current_value=current_value,
            current_period=current_period,
            benchmarks=benchmarks,
        )
