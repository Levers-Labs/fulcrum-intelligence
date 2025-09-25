"""
Story evaluator for the forecasting pattern.
"""

import logging
from typing import Any

import numpy as np
import pandas as pd

from commons.models.enums import Granularity
from levers.models import ForecastVsTargetStats, PacingProjection, RequiredPerformance
from levers.models.enums import MetricGVAStatus, PeriodType
from levers.models.patterns.forecasting import Forecasting
from levers.primitives import get_period_range_for_grain
from levers.primitives.period_grains import get_period_end_date
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator import StoryEvaluatorBase, render_story_text

logger = logging.getLogger(__name__)


class ForecastingEvaluator(StoryEvaluatorBase[Forecasting]):
    """
    Evaluates forecasting pattern results and generates stories.

    Stories types generated:
    - FORECASTED_ON_TRACK: Forecast shows metric will beat target
    - FORECASTED_OFF_TRACK: Forecast shows metric will miss target
    - PACING_ON_TRACK: Current pacing indicates metric will beat target
    - PACING_OFF_TRACK: Current pacing indicates metric will miss target
    - REQUIRED_PERFORMANCE: Shows required growth to meet target
    """

    pattern_name = "forecasting"

    async def evaluate(self, pattern_result: Forecasting, metric: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Evaluate the forecasting pattern result and generate stories.

        Args:
            pattern_result: Forecasting pattern result
            metric: Metric details

        Returns:
            List of story dictionaries
        """
        stories = []
        metric_id = pattern_result.metric_id
        grain = Granularity(pattern_result.analysis_window.grain)

        # Generate forecast vs target stories for each period
        if pattern_result.forecast_vs_target_stats:
            for forecast_stats in pattern_result.forecast_vs_target_stats:
                if not forecast_stats or not forecast_stats.status:
                    continue

                if forecast_stats.status == MetricGVAStatus.ON_TRACK:
                    stories.append(
                        self._create_forecasted_on_track_story(pattern_result, metric_id, metric, grain, forecast_stats)
                    )
                elif forecast_stats.status == MetricGVAStatus.OFF_TRACK:
                    stories.append(
                        self._create_forecasted_off_track_story(
                            pattern_result, metric_id, metric, grain, forecast_stats
                        )
                    )

        # Generate pacing stories for each period
        if pattern_result.pacing:
            for pacing in pattern_result.pacing:
                if not pacing or not pacing.status or pacing.projected_value is None:
                    continue

                if pacing.status == MetricGVAStatus.ON_TRACK:
                    stories.append(self._create_pacing_on_track_story(pattern_result, metric_id, metric, grain, pacing))
                elif pacing.status == MetricGVAStatus.OFF_TRACK:
                    stories.append(
                        self._create_pacing_off_track_story(pattern_result, metric_id, metric, grain, pacing)
                    )

        # Generate required performance stories for each period
        if pattern_result.required_performance:
            for required_perf in pattern_result.required_performance:
                if (
                    required_perf
                    and required_perf.required_pop_growth_percent is not None
                    and required_perf.remaining_periods is not None
                    and required_perf.remaining_periods > 0
                    and required_perf.previous_periods is not None
                    and required_perf.previous_periods > 2
                ):
                    stories.append(
                        self._create_required_performance_story(pattern_result, metric_id, metric, grain, required_perf)
                    )

        return stories

    def _populate_template_context(
        self, pattern_result: Forecasting, metric: dict, grain: Granularity, **kwargs
    ) -> dict[str, Any]:
        """
        Populate context for template rendering.

        Args:
            pattern_result: Forecasting pattern result
            metric: Metric details
            grain: Granularity of the analysis
            forecast_stats: Specific forecast stats object
            pacing: Specific pacing object
            required_perf: Specific required performance object
            **kwargs: Additional keyword arguments

        Returns:
            Template context dictionary
        """

        forecast_stats = kwargs.get("forecast_stats", None)
        pacing = kwargs.get("pacing", None)
        required_perf = kwargs.get("required_perf", None)

        period_map = {
            PeriodType.END_OF_WEEK: "week",
            PeriodType.END_OF_MONTH: "month",
            PeriodType.END_OF_QUARTER: "quarter",
            PeriodType.END_OF_YEAR: "year",
        }

        context = self.prepare_base_context(metric, grain)

        if forecast_stats:
            context["period"] = (
                period_map.get(forecast_stats.period, forecast_stats.period.value) if forecast_stats.period else None
            )
            context.update(
                {
                    "forecasted_value": forecast_stats.forecasted_value,
                    "target_value": forecast_stats.target_value,
                    "gap_percent": abs(forecast_stats.gap_percent or 0),
                }
            )

        if pacing:
            context["period"] = period_map.get(pacing.period, pacing.period.value) if pacing.period else None
            analysis_dt = pd.to_datetime(pattern_result.analysis_date)
            period_end_date = get_period_end_date(analysis_dt, pacing.period)
            context.update(
                {
                    "percent_elapsed": pacing.period_elapsed_percent or 0,
                    "projected_value": pacing.projected_value,
                    "target_value": pacing.target_value,
                    "gap_percent": abs(pacing.gap_percent or 0),
                    "period_end_date": period_end_date.strftime("%Y-%m-%d"),
                }
            )

        if required_perf:
            context["period"] = (
                period_map.get(required_perf.period, required_perf.period.value) if required_perf.period else None
            )
            # Determine trend direction
            growth_difference = required_perf.growth_difference or 0
            trend_direction = "increase" if growth_difference > 0 else "decrease"

            context.update(
                {
                    "required_growth": abs(required_perf.required_pop_growth_percent or 0),
                    "remaining_periods": required_perf.remaining_periods,
                    "growth_difference": abs(growth_difference),
                    "trend_direction": trend_direction,
                    "previous_growth": abs(required_perf.previous_pop_growth_percent or 0),
                    "previous_periods": required_perf.previous_periods,
                    "target_value": self._get_target_value(pattern_result, required_perf.period),
                }
            )

        return context

    def _create_forecasted_on_track_story(
        self,
        pattern_result: Forecasting,
        metric_id: str,
        metric: dict,
        grain: Granularity,
        forecast_stats: ForecastVsTargetStats,
    ) -> dict[str, Any]:
        """
        Create a FORECASTED_ON_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis
            forecast_stats: Specific forecast stats object

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.FORECASTED_ON_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(
            pattern_result=pattern_result, metric=metric, grain=grain, forecast_stats=forecast_stats
        )

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare combined historical + forecast series data
        series_data = self._prepare_forecast_series_data(pattern_result, grain, forecast_stats)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,
            series_data=series_data,
            **context,
        )

    def _create_forecasted_off_track_story(
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity, forecast_stats
    ) -> dict[str, Any]:
        """
        Create a FORECASTED_OFF_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis
            forecast_stats: Specific forecast stats object

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.FORECASTED_OFF_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(
            pattern_result=pattern_result, metric=metric, grain=grain, forecast_stats=forecast_stats
        )

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare combined historical + forecast series data
        series_data = self._prepare_forecast_series_data(pattern_result, grain, forecast_stats)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,
            series_data=series_data,
            **context,
        )

    def _create_pacing_on_track_story(
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity, pacing: PacingProjection
    ) -> dict[str, Any]:
        """
        Create a PACING_ON_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis
            pacing: Specific pacing object

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.PACING_ON_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(
            pattern_result=pattern_result, metric=metric, grain=grain, pacing=pacing
        )

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        series_df = self._prepare_pacing_series_data(pattern_result, grain, pacing)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,
            series_data=series_data,
            **context,
        )

    def _create_pacing_off_track_story(
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity, pacing: PacingProjection
    ) -> dict[str, Any]:
        """
        Create a PACING_OFF_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis
            pacing: Specific pacing object

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.PACING_OFF_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(
            pattern_result=pattern_result, metric=metric, grain=grain, pacing=pacing
        )

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        series_df = self._prepare_pacing_series_data(pattern_result, grain, pacing)
        series_data = self.export_dataframe_as_story_series(series_df, story_type, story_group, grain)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,
            series_data=series_data,
            **context,
        )

    def _create_required_performance_story(
        self,
        pattern_result: Forecasting,
        metric_id: str,
        metric: dict,
        grain: Granularity,
        required_perf: RequiredPerformance,
    ) -> dict[str, Any]:
        """
        Create a REQUIRED_PERFORMANCE story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis
            required_perf: Specific required performance object

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.REQUIRED_PERFORMANCE
        story_type = StoryType.REQUIRED_PERFORMANCE

        # Prepare context for template rendering
        context = self._populate_template_context(
            pattern_result=pattern_result, metric=metric, grain=grain, required_perf=required_perf
        )

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare required performance series data for visualization
        series_data = self._prepare_required_performance_series_data(pattern_result, required_perf)

        # Prepare the story model
        return self.prepare_story_model(
            genre=StoryGenre.PERFORMANCE,
            story_type=story_type,
            story_group=story_group,
            metric_id=metric_id,
            pattern_result=pattern_result,
            title=title,
            detail=detail,
            grain=grain,
            series_data=series_data,
            **context,
        )

    def _calculate_cumulative_series(
        self, df: pd.DataFrame, include_bounds: bool = True, last_cumulative_value: float = 0.0
    ) -> pd.DataFrame:
        """
        Calculate cumulative sums for a DataFrame.

        Args:
            df: DataFrame with columns: date, value, and optionally lower_bound, upper_bound
            include_bounds: Whether to calculate cumulative sums for bounds
            last_cumulative_value: last cumulative value to add to the sum (default: 0.0)

        Returns:
            DataFrame with cumulative sums applied
        """
        if df.empty:
            return df

        df = df.copy().sort_values("date")

        if last_cumulative_value > 0:
            # Add cumulative_value column with cumulative sum of values rounded to 2 decimal places
            df["cumulative_value"] = (last_cumulative_value + df["value"].cumsum()).round(2)
        else:
            # Add cumulative_value column with cumulative sum of values rounded to 2 decimal places
            df["cumulative_value"] = df["value"].cumsum().round(2)

        if include_bounds:
            if "lower_bound" in df.columns and "upper_bound" in df.columns:
                # Initialize cumulative bound columns
                df["cumulative_lower_bound"] = None
                df["cumulative_upper_bound"] = None

                # Only calculate bounds for forecast data (where bounds are not None)
                forecast_mask = df["lower_bound"].notna() & df["upper_bound"].notna()
                if forecast_mask.any():
                    # Calculate bounds as a percentage of cumulative value
                    # Use the original forecast bounds to estimate uncertainty percentage
                    forecast_rows = df[forecast_mask]

                    for idx in forecast_rows.index:
                        cumulative_val = df.loc[idx, "cumulative_value"]
                        original_val = df.loc[idx, "value"]
                        original_lower = df.loc[idx, "lower_bound"]
                        original_upper = df.loc[idx, "upper_bound"]

                        # Calculate uncertainty percentage from original bounds
                        if original_val > 0:  # type: ignore
                            lower_uncertainty = abs(original_val - original_lower) / original_val  # type: ignore
                            upper_uncertainty = abs(original_upper - original_val) / original_val  # type: ignore
                        else:
                            lower_uncertainty = 0.1  # default 10% uncertainty
                            upper_uncertainty = 0.1

                        # Apply uncertainty to cumulative value for new columns
                        df.loc[idx, "cumulative_lower_bound"] = round(cumulative_val * (1 - lower_uncertainty), 2)
                        df.loc[idx, "cumulative_upper_bound"] = round(cumulative_val * (1 + upper_uncertainty), 2)

        return df

    def _prepare_forecast_series_data(
        self, pattern_result: Forecasting, target_grain: Granularity, forecast_stats=None
    ) -> list[dict[str, Any]]:
        """
        Prepare combined historical and forecast series data for forecasting stories.

        This method combines the actual time series data (from self.series_df) with
        the forecast data (from pattern_result.forecast) to create a complete
        visualization dataset that shows both past actuals and future projections.

        The forecast data is always in day grain and needs to be converted to the target grain.
        For forecast on/off track stories, only include forecast dates up to the specific period end date.

        Args:
            pattern_result: Forecasting pattern result containing forecast data
            target_grain: Target granularity for the series data
            forecast_stats: Specific forecast stats object (used to determine period end date)

        Returns:
            List of dictionaries with combined historical and forecast data, including:
            - data: DataFrame with combined historical and forecast data
            - forecast: DataFrame with forecast data
        """
        analysis_date = pd.to_datetime(pattern_result.analysis_date)
        period_start_date, period_end_date = get_period_range_for_grain(
            grain=self._get_period_grain(forecast_stats.period),  # type: ignore
            analysis_date=analysis_date,
        )
        period_start_date = pd.to_datetime(period_start_date)
        period_end_date = pd.to_datetime(period_end_date)

        # Start with actual data (if available)
        if self.series_df is not None and not self.series_df.empty:
            actual_df = self.series_df.copy()
            actual_df["date"] = pd.to_datetime(actual_df["date"])
            actual_df = actual_df[actual_df["date"] >= period_start_date]

            # Add forecast-specific columns for actual data
            actual_df["lower_bound"] = None
            actual_df["upper_bound"] = None
        else:
            # Create empty DataFrame with proper columns if no actual data
            actual_df = pd.DataFrame(columns=["date", "value", "lower_bound", "upper_bound"])

        # Convert daily forecast data to target grain
        forecast_data = []
        for forecast in pattern_result.forecast:
            forecast_point = {
                "date": pd.to_datetime(forecast.date),
                "value": forecast.forecasted_value,
                "lower_bound": forecast.lower_bound,
                "upper_bound": forecast.upper_bound,
            }
            forecast_data.append(forecast_point)

        forecast_df = pd.DataFrame(forecast_data)
        if not forecast_df.empty:
            forecast_df["date"] = pd.to_datetime(forecast_df["date"])
            # Filter to only include forecast dates up to the period end date
            forecast_df = forecast_df[forecast_df["date"] <= period_end_date].copy()

        # Convert forecast from day grain to target grain
        if not forecast_df.empty:
            forecast_df = self._convert_daily_forecast_to_grain(forecast_df, target_grain)

        # Split data like in the pattern: actual from period_start to analysis_date (exclusive), forecast from
        # analysis_date onwards
        period_actuals = (
            actual_df[(actual_df["date"] >= period_start_date) & (actual_df["date"] < analysis_date)]
            if not actual_df.empty
            else pd.DataFrame(columns=["date", "value", "lower_bound", "upper_bound"])
        )
        period_actuals = self._calculate_cumulative_series(period_actuals, include_bounds=False)

        # Get the last cumulative value from actuals to continue the cumulative series in forecast
        last_cumulative_value = 0.0
        if not period_actuals.empty and "cumulative_value" in period_actuals.columns:
            last_cumulative_value = period_actuals["cumulative_value"].iloc[-1]

        if not forecast_df.empty:
            period_forecast = forecast_df[forecast_df["date"] >= analysis_date]
        else:
            period_forecast = pd.DataFrame(columns=["date", "value", "lower_bound", "upper_bound"])
            period_forecast["value"] = period_forecast["value"].astype("float64")
            period_forecast["lower_bound"] = period_forecast["lower_bound"].astype("float64")
            period_forecast["upper_bound"] = period_forecast["upper_bound"].astype("float64")
        period_forecast = self._calculate_cumulative_series(
            period_forecast, include_bounds=True, last_cumulative_value=last_cumulative_value
        )

        # Convert DataFrames to JSON-serializable format
        # Convert date column to ISO format strings
        if not period_actuals.empty and "date" in period_actuals.columns:
            period_actuals = period_actuals.copy()
            period_actuals["date"] = period_actuals["date"].dt.strftime("%Y-%m-%d")

        if not period_forecast.empty and "date" in period_forecast.columns:
            period_forecast = period_forecast.copy()
            period_forecast["date"] = period_forecast["date"].dt.strftime("%Y-%m-%d")

        # Replace inf, -inf, and NaN with None for JSON serialization
        period_actuals = period_actuals.replace([float("inf"), float("-inf"), pd.NA], 0)  # type: ignore
        period_forecast = period_forecast.replace([float("inf"), float("-inf"), pd.NA], 0)  # type: ignore

        # Prepare result
        result = {
            "data": period_actuals.to_dict(orient="records"),
            "forecast": period_forecast.to_dict(orient="records"),
        }

        return [result]

    def _convert_daily_forecast_to_grain(
        self, daily_forecast_df: pd.DataFrame, target_grain: Granularity
    ) -> pd.DataFrame:
        """
        Convert daily forecast data to the specified grain.

        Args:
            daily_forecast_df: DataFrame with daily forecast data
            target_grain: Target granularity (DAY, WEEK, MONTH)

        Returns:
            DataFrame aggregated to target grain
        """
        if target_grain == Granularity.DAY:
            return daily_forecast_df

        df = daily_forecast_df.copy()
        df["date"] = pd.to_datetime(df["date"])

        if target_grain == Granularity.WEEK:
            # Group by week starting Monday and set date to Monday of the week
            # First, find the Monday of each week for each date
            df["monday_date"] = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")

        elif target_grain == Granularity.MONTH:
            # Group by month starting first day of month
            df["month_date"] = df["date"].dt.to_period("M").dt.start_time.dt.date
        else:
            # For other grains, return daily data
            return daily_forecast_df

        # Aggregate by period
        if target_grain == Granularity.WEEK:
            aggregated = (
                df.groupby("monday_date")
                .agg({"value": "sum", "lower_bound": "sum", "upper_bound": "sum"})
                .reset_index()
            )
            aggregated.rename(columns={"monday_date": "date"}, inplace=True)
            # Convert date back to datetime for consistency
            aggregated["date"] = pd.to_datetime(aggregated["date"])

        elif target_grain == Granularity.MONTH:
            aggregated = (
                df.groupby("month_date").agg({"value": "sum", "lower_bound": "sum", "upper_bound": "sum"}).reset_index()
            )
            aggregated.rename(columns={"month_date": "date"}, inplace=True)
            # Convert date back to datetime for consistency
            aggregated["date"] = pd.to_datetime(aggregated["date"])

        return aggregated

    def _prepare_required_performance_series_data(
        self, pattern_result: Forecasting, required_perf=None
    ) -> list[dict[str, Any]]:
        """
        Prepare series data for required performance visualization.

        This method creates a dataset showing:
        - Actual growth rates (calculated from actual values)
        - Required growth rate for remaining periods (from required_performance)

        Uses the same date generation logic as forecast series for consistency.

        Args:
            pattern_result: Forecasting pattern result containing required_performance data
            required_perf: The specific required performance object

        Returns:
            List of dictionaries with series data, including:
            - data: DataFrame with series data and pop_growth_percent
        """
        analysis_date = pd.to_datetime(pattern_result.analysis_date)
        period_start_date, _ = get_period_range_for_grain(
            grain=self._get_period_grain(required_perf.period),  # type: ignore
            analysis_date=analysis_date,
        )
        period_start_date = pd.to_datetime(period_start_date)

        # Prepare historical data with growth rates
        if self.series_df is None or self.series_df.empty:
            return []

        df = self.series_df.copy()  # type: ignore
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"] >= period_start_date]
        df = df.sort_values("date")
        df["pop_growth_percent"] = round(df["value"].pct_change() * 100, 2)

        # Convert DataFrames to JSON-serializable format
        # Convert date column to ISO format strings
        if not df.empty and "date" in df.columns:
            df = df.copy()
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")

        # Replace inf, -inf, and NaN with None for JSON serialization
        df = df.replace([float("inf"), float("-inf"), pd.NA, None, np.nan], 0)  # type: ignore

        result = {
            "data": df.to_dict(orient="records"),
        }

        return [result]

    def _prepare_pacing_series_data(
        self, pattern_result: Forecasting, grain: Granularity, pacing: PacingProjection
    ) -> pd.DataFrame:
        """
        Prepare series data for pacing visualization.
        """
        analysis_date = pd.to_datetime(pattern_result.analysis_date)
        period_start_date, period_end_date = get_period_range_for_grain(
            grain=self._get_period_grain(pacing.period), analysis_date=analysis_date  # type: ignore
        )
        period_start_date = pd.to_datetime(period_start_date)
        period_end_date = pd.to_datetime(period_end_date)

        if self.series_df is None or self.series_df.empty:
            return pd.DataFrame(columns=["date", "value", "pop_growth_percent"])

        actual_df = self.series_df.copy()
        actual_df["date"] = pd.to_datetime(actual_df["date"])
        actual_df = actual_df[actual_df["date"] >= period_start_date]
        actual_df = actual_df.sort_values("date")

        # Calculate cumulative sums for pacing stories (without bounds)
        actual_df = self._calculate_cumulative_series(actual_df, include_bounds=False)

        return actual_df

    def _get_period_grain(self, period: PeriodType) -> Granularity:
        """
        Get the grain for a given period.
        """
        if period == PeriodType.END_OF_WEEK:
            return Granularity.WEEK
        elif period == PeriodType.END_OF_MONTH:
            return Granularity.MONTH
        elif period == PeriodType.END_OF_QUARTER:
            return Granularity.QUARTER
        elif period == PeriodType.END_OF_YEAR:
            return Granularity.YEAR
        else:
            return Granularity.DAY

    def _get_target_value(self, pattern_result: Forecasting, period: PeriodType) -> float:
        """
        Get the target value for a given period.

        Args:
            pattern_result: Forecasting pattern result
            period: Period type

        Returns:
            Target value for the given period
        """
        if pattern_result.forecast_vs_target_stats:
            for forecast_stats in pattern_result.forecast_vs_target_stats:
                if forecast_stats and forecast_stats.period == period:  # type: ignore
                    return forecast_stats.target_value  # type: ignore
        return 0.0
