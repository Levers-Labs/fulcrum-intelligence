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
from levers.models.patterns import HistoricalPerformance
from story_manager.mocks.v2.pattern_generators.base import MockPatternGeneratorBase

logger = logging.getLogger(__name__)


class MockHistoricalPerformanceGenerator(MockPatternGeneratorBase):
    """Generator for mock historical performance pattern results."""

    pattern_name = "historical_performance"
    rank = 1

    def generate_pattern_results(
        self, metric: dict[str, Any], grain: Granularity, story_date: date
    ) -> list[HistoricalPerformance]:
        """Generate mock historical performance pattern results with story group coverage."""

        # Define story groups - each group should have at least one story type enabled
        story_groups = {
            "growth": ["accelerating_growth", "slowing_growth"],
            "trends": ["stable_trend", "new_upward_trend", "new_downward_trend", "performance_plateau"],
            "exceptions": ["spike", "drop"],
            "performance": ["improving_performance", "worsening_performance"],
            "records": ["record_high", "record_low"],
        }

        # Add benchmark group only for week/month grains
        if grain in [Granularity.WEEK, Granularity.MONTH]:
            story_groups["benchmarks"] = ["benchmark_comparison"]

        # Create base pattern result
        pattern_result = self._create_base_pattern_result(metric, grain, story_date)

        # Track which story types we're enabling
        enabled_story_types = []

        # Process each story group - randomly select one story type per group
        for _, story_types in story_groups.items():
            selected_story_type = random.choice(story_types)  # noqa
            enabled_story_types.append(selected_story_type)

            # Add story-specific data
            if selected_story_type == "accelerating_growth":
                self._add_accelerating_growth_data(pattern_result)
            elif selected_story_type == "slowing_growth":
                self._add_slowing_growth_data(pattern_result)
            elif selected_story_type == "stable_trend":
                self._add_stable_trend_data(pattern_result)
            elif selected_story_type == "new_upward_trend":
                self._add_new_upward_trend_data(pattern_result)
            elif selected_story_type == "new_downward_trend":
                self._add_new_downward_trend_data(pattern_result)
            elif selected_story_type == "performance_plateau":
                self._add_plateau_data(pattern_result)
            elif selected_story_type == "spike":
                self._add_spike_data(pattern_result)
            elif selected_story_type == "drop":
                self._add_drop_data(pattern_result)
            elif selected_story_type == "improving_performance":
                self._add_improving_performance_data(pattern_result)
            elif selected_story_type == "worsening_performance":
                self._add_worsening_performance_data(pattern_result)
            elif selected_story_type == "record_high":
                self._add_record_high_data(pattern_result)
            elif selected_story_type == "record_low":
                self._add_record_low_data(pattern_result)
            elif selected_story_type == "benchmark_comparison":
                self._add_benchmark_data(pattern_result, grain)

        logger.info(f"Generated pattern result with story types: {enabled_story_types}")

        try:
            result = HistoricalPerformance(**pattern_result)
            return [result]
        except Exception as e:
            logger.error(f"Error creating HistoricalPerformance: {e}")
            return []

    def generate_mock_series_data(
        self, metric: dict[str, Any], grain: Granularity, story_date: date, num_periods: int = 12
    ) -> pd.DataFrame:
        """
        Generate mock time series data that the evaluator can use.

        This creates basic series data with only date and value columns.
        The evaluator will merge period_metrics and trend_analysis data as needed.

        Args:
            metric: Metric dictionary
            grain: Granularity for the series
            story_date: The story date (current period - NOT included in series)
            num_periods: Number of historical periods to generate (default: 12)

        Returns:
            DataFrame with basic time series data (date, value) that matches period_metrics and trend_analysis
        """
        # Generate date range - always create exactly num_periods data points
        if grain == Granularity.DAY:
            freq = "D"
            # End date should be BEFORE the story_date (last completed period)
            end_date = story_date - timedelta(days=1)
            # Generate exactly num_periods days
            date_range = pd.date_range(end=end_date, periods=num_periods, freq=freq)
        elif grain == Granularity.WEEK:
            freq = "W-MON"
            # Get the Monday before story_date
            days_since_monday = story_date.weekday()
            end_date = story_date - timedelta(days=days_since_monday + 7)
            # Generate exactly num_periods weeks
            date_range = pd.date_range(end=end_date, periods=num_periods, freq=freq)
        else:  # MONTH
            freq = "MS"
            # Get the first day of the previous month
            if story_date.month == 1:
                end_date = date(story_date.year - 1, 12, 1)
            else:
                end_date = date(story_date.year, story_date.month - 1, 1)
            # Generate exactly num_periods months
            date_range = pd.date_range(end=end_date, periods=num_periods, freq=freq)

        # Generate realistic patterns based on different scenarios
        base_value = self._generate_mock_value(1000)

        # Choose scenario type and create realistic patterns with weighted selection for spike/drop
        growth_scenarios = [
            "accelerating",
            "slowing",
            "stable",
            "spike",  # More weight for spikes
            "drop",  # More weight for drops
            "record_high",
            "record_low",
        ]
        scenario_type = random.choice(growth_scenarios)  # noqa

        values = []

        for i, _ in enumerate(date_range):
            if i == 0:
                value = base_value
                values.append(value)
            else:
                # Calculate growth rate based on scenario
                growth_rate = self._calculate_scenario_growth_rate(scenario_type, i, len(date_range))

                # Apply growth rate with noise
                actual_growth_rate = max(-0.3, growth_rate)  # Prevent extreme negative growth
                value = values[i - 1] * (1 + actual_growth_rate)

                # Ensure we have valid numbers
                if pd.isna(value) or not isinstance(value, (int, float)):
                    value = values[i - 1]  # Use previous value if calculation failed

                values.append(max(0, value))  # Ensure positive values

        # For record high/low scenarios, ensure the series supports the story
        if scenario_type == "record_high":
            # For 2nd highest: add a higher value in the series (not the most recent)
            # Find a period to be the highest (not the last period)
            if self.rank == 2:
                highest_idx = len(values) - 2  # Earlier in the series
                current_last = values[-1]  # The most recent value should be 2nd highest
                values[highest_idx] = current_last * 1.50  # Make this the highest

        elif scenario_type == "record_low":
            # For 2nd lowest: add a lower value in the series (not the most recent)
            # Find a period to be the lowest (not the last period)
            if self.rank == 2:
                lowest_idx = len(values) - 2  # Earlier in the series
                current_last = values[-1]  # The most recent value should be 2nd lowest
                values[lowest_idx] = current_last * 0.95  # Make this the lowest

        # Create basic DataFrame with only date and value - let evaluator handle the rest
        series_df = pd.DataFrame({"date": date_range, "value": values})

        return series_df

    def _calculate_scenario_growth_rate(self, scenario_type: str, period_index: int, total_periods: int) -> float:
        """Calculate growth rate based on scenario type and period position."""
        progress = period_index / total_periods
        noise = random.uniform(-0.005, 0.005)  # noqa Small noise

        if scenario_type == "accelerating":
            # Start with low growth, accelerate over time
            base_growth = 0.01  # 1% base growth
            acceleration = progress * 0.08  # Up to 8% additional growth
            return base_growth + acceleration + noise

        elif scenario_type == "slowing":
            # Start with high growth, slow down over time
            base_growth = 0.09  # 9% initial growth
            deceleration = progress * 0.08  # Lose up to 8% growth
            return base_growth - deceleration + noise

        elif scenario_type == "spike":
            # Sudden spike in the last few periods
            if progress > 0.8:  # Last 20% of periods
                return random.uniform(0.10, 0.20)  # noqa 10-20% growth
            else:
                return random.uniform(0.02, 0.04) + noise  # noqa Normal growth

        elif scenario_type == "drop":
            # Sudden drop in the last few periods
            if progress > 0.8:  # Last 20% of periods
                return random.uniform(-0.20, -0.10)  # noqa 10-20% decline
            else:
                return random.uniform(0.02, 0.04) + noise  # noqa Normal growth

        elif scenario_type == "record_high":
            # Strong growth throughout with acceleration at the end
            base_growth = random.uniform(0.05, 0.08)  # noqa 5-8% base growth
            if progress > 0.7:  # Last 30% of periods
                base_growth *= 1.5  # Accelerate growth
            return base_growth + noise

        elif scenario_type == "record_low":
            # Declining growth throughout with acceleration at the end
            base_growth = random.uniform(-0.03, -0.01)  # noqa Slight decline
            if progress > 0.7:  # Last 30% of periods
                base_growth *= 2  # Accelerate decline
            return base_growth + noise

        else:  # "stable"
            # Consistent growth with some variation
            return random.uniform(0.02, 0.04) + noise  # noqa

    def generate_mock_period_metrics(self, series_df: pd.DataFrame, grain: Granularity) -> list:
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
        from levers.models.patterns import PeriodMetrics

        period_metrics = []

        # Add the first period with 0% growth (no previous period to compare to)
        if len(series_df) > 0:
            first_row = series_df.iloc[0]
            period_metrics.append(
                PeriodMetrics(
                    period_start=first_row["date"].strftime("%Y-%m-%d"),
                    period_end=first_row["date"].strftime("%Y-%m-%d"),
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
            period_end = current_row["date"].strftime("%Y-%m-%d")
            period_start = previous_row["date"].strftime("%Y-%m-%d")

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

    def generate_mock_trend_analysis(self, series_df: pd.DataFrame, grain: Granularity) -> list:
        """
        Generate trend analysis objects that align with the series data.

        Args:
            series_df: The generated series DataFrame with date and value columns
            grain: Granularity for the analysis

        Returns:
            List of TrendAnalysis objects with calculated SPC metrics
        """
        from levers.models.patterns import TrendAnalysis

        trend_analysis = []

        # Calculate SPC metrics from the series data
        window_size = min(6, len(series_df) // 2)  # Use smaller window to avoid nulls

        # Calculate central line (moving average)
        central_line = series_df["value"].rolling(window=window_size, center=True).mean()

        # Calculate control limits based on standard deviation
        rolling_std = series_df["value"].rolling(window=window_size, center=True).std()
        ucl = central_line + (2 * rolling_std)
        lcl = central_line - (2 * rolling_std)

        # Fill null values at the beginning and end of the series
        central_line = central_line.bfill().ffill()
        ucl = ucl.bfill().ffill()
        lcl = lcl.bfill().ffill()

        # Ensure no null values remain by using global values if needed
        if central_line.isna().any():
            central_line = central_line.fillna(series_df["value"].mean())
        if ucl.isna().any():
            ucl = ucl.fillna(series_df["value"].mean() * 1.2)
        if lcl.isna().any():
            lcl = lcl.fillna(series_df["value"].mean() * 0.8)

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

            # Detect trend signals based on control limits
            current_value = row["value"]
            current_ucl = ucl.iloc[i]
            current_lcl = lcl.iloc[i]

            # Signal detected if value is outside control limits
            trend_signal_detected = (current_value > current_ucl) or (current_value < current_lcl)

            # Create trend analysis object - date must match series_df for evaluator merge
            trend_analysis.append(
                TrendAnalysis(
                    value=current_value,
                    date=row["date"].strftime("%Y-%m-%d"),  # Must match series_df dates!
                    central_line=central_line.iloc[i],
                    ucl=current_ucl,
                    lcl=current_lcl,
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
        period_metrics = self.generate_mock_period_metrics(series_df, grain)
        trend_analysis = self.generate_mock_trend_analysis(series_df, grain)

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

    def _add_accelerating_growth_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy accelerating growth story conditions."""
        pattern_result["growth_stats"]["current_growth_acceleration"] = self._generate_percentage(3, 8)  # Positive
        pattern_result["growth_stats"]["current_pop_growth"] = self._generate_percentage(8, 15)
        pattern_result["growth_stats"]["num_periods_accelerating"] = random.randint(3, 6)  # noqa

    def _add_slowing_growth_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy slowing growth story conditions."""
        pattern_result["growth_stats"]["current_growth_acceleration"] = self._generate_percentage(-8, -2)  # Negative
        pattern_result["growth_stats"]["current_pop_growth"] = self._generate_percentage(1, 5)
        pattern_result["growth_stats"]["num_periods_slowing"] = random.randint(3, 6)  # noqa

    def _add_stable_trend_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy stable trend story conditions."""
        pattern_result["current_trend"] = {
            "trend_type": "stable",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": self._generate_percentage(-2, 2),
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_new_upward_trend_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy new upward trend story conditions."""
        pattern_result["current_trend"] = {
            "trend_type": "upward",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": self._generate_percentage(3, 8),
            "duration_grains": random.randint(3, 6),  # noqa
        }
        # Add different previous trend to make it "new"
        pattern_result["previous_trend"] = {
            "trend_type": "stable",  # Different from current
            "start_date": (pattern_result["analysis_date"] - timedelta(days=90)).isoformat(),
            "average_pop_growth": self._generate_percentage(-1, 1),
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_new_downward_trend_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy new downward trend story conditions."""
        pattern_result["current_trend"] = {
            "trend_type": "downward",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": self._generate_percentage(-8, -3),
            "duration_grains": random.randint(3, 6),  # noqa
        }
        # Add different previous trend to make it "new"
        pattern_result["previous_trend"] = {
            "trend_type": "upward",  # Different from current
            "start_date": (pattern_result["analysis_date"] - timedelta(days=90)).isoformat(),
            "average_pop_growth": self._generate_percentage(2, 5),
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_plateau_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy performance plateau story conditions."""
        pattern_result["current_trend"] = {
            "trend_type": "plateau",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": self._generate_percentage(-1, 1),
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_spike_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy spike story conditions."""
        # Use the last value from series data to ensure consistency
        current_value = pattern_result["series_data"][-1]["value"]
        pattern_result["trend_exception"] = {
            "type": "Spike",
            "current_value": current_value,
            "normal_range_low": current_value * 0.7,
            "normal_range_high": current_value * 0.9,
            "absolute_delta_from_normal_range": current_value * 0.1,
            "magnitude_percent": 15.0,
        }

    def _add_drop_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy drop story conditions."""
        # Use the last value from series data to ensure consistency
        current_value = pattern_result["series_data"][-1]["value"]
        pattern_result["trend_exception"] = {
            "type": "Drop",
            "current_value": current_value,
            "normal_range_low": current_value * 1.1,
            "normal_range_high": current_value * 1.3,
            "absolute_delta_from_normal_range": current_value * 0.1,
            "magnitude_percent": 15.0,
        }

    def _add_improving_performance_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy improving performance story conditions."""
        pattern_result["current_trend"] = {
            "trend_type": "upward",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": self._generate_percentage(5, 12),  # Positive for improving
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_worsening_performance_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy worsening performance story conditions."""
        pattern_result["current_trend"] = {
            "trend_type": "downward",
            "start_date": pattern_result["analysis_date"].isoformat(),
            "average_pop_growth": self._generate_percentage(-12, -5),  # Negative for worsening
            "duration_grains": random.randint(6, 12),  # noqa
        }

    def _add_record_high_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy record high story conditions."""
        self.rank = random.randint(1, 2)  # noqa
        pattern_result["high_rank"]["rank"] = self.rank  # Must be <= 2
        # Value is already set correctly in _create_base_pattern_result

    def _add_record_low_data(self, pattern_result: dict[str, Any]) -> None:
        """Add data to satisfy record low story conditions."""
        self.rank = random.randint(1, 2)  # noqa
        pattern_result["low_rank"]["rank"] = self.rank
        # Value is already set correctly in _create_base_pattern_result

    def _add_benchmark_data(self, pattern_result: dict[str, Any], grain: Granularity) -> None:
        """Add data to satisfy benchmark story conditions."""
        from levers.models import ComparisonType
        from levers.models.patterns import Benchmark, BenchmarkComparison

        current_value = pattern_result["high_rank"]["value"] * 2.57
        analysis_date = pattern_result["analysis_date"]

        # Create benchmark comparison data
        benchmarks = {}

        if grain == Granularity.WEEK:
            comparison_types = [ComparisonType.LAST_WEEK, ComparisonType.WEEK_IN_LAST_MONTH]
            current_period = "This Week"
        else:  # MONTH
            comparison_types = [ComparisonType.LAST_MONTH, ComparisonType.MONTH_IN_LAST_QUARTER]
            current_period = "This Month"

        for comp_type in comparison_types:
            reference_value = current_value * random.uniform(0.8, 1.2)  # noqa
            absolute_change = current_value - reference_value
            change_percent = (absolute_change / reference_value) * 100 if reference_value != 0 else 0

            if comp_type in [ComparisonType.LAST_WEEK, ComparisonType.LAST_MONTH]:
                ref_date = analysis_date - timedelta(days=7 if grain == Granularity.WEEK else 30)
                ref_period = "Week Ago" if grain == Granularity.WEEK else "Month Ago"
            else:
                ref_date = analysis_date - timedelta(days=30 if grain == Granularity.WEEK else 90)
                ref_period = "Month Ago" if grain == Granularity.WEEK else "Quarter Ago"

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
