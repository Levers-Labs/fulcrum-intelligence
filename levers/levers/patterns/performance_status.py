"""
Performance Status Pattern

This module implements the PerformanceStatusPattern which analyzes whether a metric
is on/off track versus its target. It classifies the current status, tracks status
changes, and provides details about any gap or overperformance.
"""

from datetime import datetime

import pandas as pd

from levers.exceptions import ValidationError
from levers.models.common import AnalysisWindow
from levers.models.patterns.performance_status import (
    HoldSteady,
    MetricGVAStatus,
    MetricPerformance,
    StatusChange,
    Streak,
)
from levers.patterns.base import Pattern
from levers.primitives import (
    calculate_difference,
    calculate_percentage_difference,
    classify_metric_status,
    detect_status_changes,
    monitor_threshold_proximity,
    track_status_durations,
)


class PerformanceStatusPattern(Pattern[MetricPerformance]):
    """Pattern for analyzing metric performance status against targets."""

    name = "performance_status"
    version = "1.0"
    description = "Analyzes a metric's performance status against its target"
    required_primitives = [
        "calculate_difference",
        "calculate_percentage_difference",
        "classify_metric_status",
        "detect_status_changes",
        "monitor_threshold_proximity",
        "track_status_durations",
    ]
    output_model: type[MetricPerformance] = MetricPerformance

    def analyze(  # type: ignore
        self,
        metric_id: str,
        data: pd.DataFrame,
        analysis_window: AnalysisWindow,
        threshold_ratio: float = 0.05,
    ) -> MetricPerformance:
        """
        Execute the performance status pattern.

        Args:
            metric_id: The ID of the metric being analyzed
            data: DataFrame containing columns: date, value, target
            analysis_window: AnalysisWindow object specifying the analysis time window
            threshold_ratio: Tolerance ratio for status classification (default: 0.05)

        Returns:
            MetricPerformance object with analysis results

        Raises:
            ValidationError: If input validation fails or calculation errors occur
        """
        try:
            # Validate input data and preprocess
            required_columns = ["date", "value"]
            self.validate_data(data, required_columns)

            # Process data
            df = self.preprocess_data(data, analysis_window)

            # Handle empty data
            if df.empty:
                return self.handle_empty_data(metric_id, analysis_window)

            # Extract grain from validated window
            grain = analysis_window.grain

            # Calculate current and prior values
            current_value = float(df["value"].iloc[-1])
            prior_value = float(df["value"].iloc[-2]) if len(df) > 1 else None

            # Calculate target value if target column exists
            target_value = None
            has_target = "target" in df.columns
            if has_target:
                target_values = df["target"].dropna()
                if not target_values.empty:
                    target_value = float(target_values.iloc[-1])

            # Calculate status
            if target_value is not None:
                status = classify_metric_status(current_value, target_value, threshold_ratio)
                abs_diff = calculate_difference(current_value, target_value)

                try:
                    pct_diff = calculate_percentage_difference(current_value, target_value)
                except Exception:
                    pct_diff = None
            else:
                status = MetricGVAStatus.NO_TARGET
                abs_diff = None
                pct_diff = None

            # Initialize result dictionary with base fields
            analysis_date = df["date"].iloc[-1].date()
            result = {
                "pattern_name": self.name,
                "version": self.version,
                "grain": grain,
                "analysis_date": analysis_date,
                "metric_id": metric_id,
                "evaluation_time": datetime.now(),
                "current_value": current_value,
                "target_value": target_value,
                "status": status,
                "threshold_ratio": threshold_ratio,
            }

            # Add prior value and delta calculations if available
            if prior_value is not None:
                result["prior_value"] = prior_value
                result["absolute_delta_from_prior"] = calculate_difference(current_value, prior_value)
                try:
                    result["pop_change_percent"] = calculate_percentage_difference(current_value, prior_value)
                except Exception:
                    result["pop_change_percent"] = None

            # Add gap or overperformance information
            if status == MetricGVAStatus.OFF_TRACK:
                result["absolute_gap"] = abs(abs_diff) if abs_diff and abs_diff < 0 else 0.0
                result["percent_gap"] = abs(pct_diff) if pct_diff and pct_diff < 0 else 0.0
            elif status == MetricGVAStatus.ON_TRACK and target_value is not None:
                result["absolute_over_performance"] = max(0.0, abs_diff) if abs_diff is not None else 0.0
                result["percent_over_performance"] = max(0.0, pct_diff) if pct_diff is not None else 0.0

            # Calculate status change info if historical data is available
            if has_target and len(df) > 1:
                # Calculate status for each row with target
                df["status"] = df.apply(
                    lambda row: classify_metric_status(
                        row["value"], row["target"] if pd.notna(row.get("target")) else None, threshold_ratio
                    ),
                    axis=1,
                )

                status_change_info = self._calculate_status_change(df, status)
                if status_change_info:
                    result["status_change"] = status_change_info

            # Calculate streak info if historical data is available
            if len(df) > 1:
                streak_info = self._calculate_streak_info(df, status)
                if streak_info:
                    result["streak"] = streak_info

            # Calculate hold steady info
            if target_value is not None:
                hold_steady_info = self._calculate_hold_steady(current_value, target_value)
                if hold_steady_info:
                    result["hold_steady"] = hold_steady_info

            # Create and validate output
            return self.validate_output(result)

        except Exception as e:
            # Re-raise with pattern context
            raise ValidationError(
                f"Error in performance status calculation: {str(e)}",
                {
                    "pattern": self.name,
                    "metric_id": metric_id,
                },
            ) from e

    def _calculate_status_change(self, data: pd.DataFrame, current_status: MetricGVAStatus) -> StatusChange | None:
        """
        Calculate information about status changes using the detect_status_changes primitive.

        Args:
            data: DataFrame with historical status data
            current_status: Current status value

        Returns:
            StatusChange object or None if no status change
        """
        # Use the detect_status_changes primitive
        if "date" in data.columns:
            status_changes_df = detect_status_changes(data, status_col="status", sort_by_date="date")
        else:
            status_changes_df = detect_status_changes(data, status_col="status")

        # Check if there was a status change in the most recent row
        if not status_changes_df.empty and status_changes_df.iloc[-1].get("status_flip", False):
            last_row = status_changes_df.iloc[-1]

            # Calculate duration of old status
            old_status_durations = None
            if "prev_status" in last_row:
                old_status = last_row["prev_status"]
                # Use track_status_durations to get duration info
                status_runs = track_status_durations(data, status_col="status")
                if not status_runs.empty:
                    # Find the last run with the old status
                    old_status_runs = status_runs[status_runs["status"] == old_status]
                    if not old_status_runs.empty:
                        old_status_durations = old_status_runs.iloc[-1]["run_length"]

            return StatusChange(
                has_flipped=True,
                old_status=last_row.get("prev_status"),
                new_status=current_status,
                old_status_duration_grains=old_status_durations,
            )

        return None

    def _calculate_streak_info(self, data: pd.DataFrame, current_status: MetricGVAStatus) -> Streak | None:
        """
        Calculate information about the current streak.

        Args:
            data: DataFrame with historical value data
            current_status: Current status value

        Returns:
            Streak object or None if no streak
        """
        if len(data) < 2:
            return None

        values = data["value"].values

        # Find the streak length
        streak_length = 1
        current_direction = None

        for i in range(len(values) - 1, 0, -1):
            if values[i] > values[i - 1]:
                direction = "increasing"
            elif values[i] < values[i - 1]:
                direction = "decreasing"
            else:
                direction = "stable"

            if current_direction is None:
                current_direction = direction
            if direction != current_direction:
                break

            streak_length += 1

        if streak_length < 2:
            return None

        # Calculate change over streak
        streak_start_value = values[-(streak_length)]
        streak_end_value = values[-1]
        # Use numeric primitive for difference calculation
        abs_change = calculate_difference(streak_end_value, streak_start_value)

        # Calculate percentage change using numeric primitive
        try:
            pct_change = calculate_percentage_difference(streak_end_value, streak_start_value)
            avg_pct_change = pct_change / streak_length
        except Exception:
            pct_change = None
            avg_pct_change = None

        return Streak(
            length=streak_length,
            status=current_status,
            performance_change_percent_over_streak=pct_change,
            absolute_change_over_streak=abs_change,
            average_change_percent_per_grain=avg_pct_change,
            average_change_absolute_per_grain=abs_change / streak_length,
        )

    def _calculate_hold_steady(self, value: float, target: float) -> HoldSteady | None:
        """
        Calculate information for the 'hold steady' scenario using the monitor_threshold_proximity primitive.

        Args:
            value: Current value
            target: Target value

        Returns:
            HoldSteady object or None if not applicable
        """
        # Check if value is at or above target
        is_above_target = value >= target
        if not is_above_target:
            return None

        # Calculate margin percentage using numeric primitive
        try:
            margin_percent = calculate_percentage_difference(value, target)
        except Exception:
            margin_percent = None

        # Use monitor_threshold_proximity to check if value is close to target
        is_close = monitor_threshold_proximity(value, target)

        return HoldSteady(
            is_currently_at_or_above_target=True,
            time_to_maintain_grains=3 if is_close else 5,  # Adjust based on proximity to target
            current_margin_percent=margin_percent,
        )
