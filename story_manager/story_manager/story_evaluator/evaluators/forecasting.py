"""
Story evaluator for the forecasting pattern.
"""

import logging
from typing import Any

import pandas as pd

from commons.models.enums import Granularity
from levers.models import ForecastVsTargetStats, PacingProjection, RequiredPerformance
from levers.models.enums import MetricGVAStatus, PeriodType
from levers.models.patterns.forecasting import Forecasting
from levers.primitives import get_period_end_date
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
                if forecast_stats and forecast_stats.status:
                    if forecast_stats.status == MetricGVAStatus.ON_TRACK:
                        stories.append(
                            self._create_forecasted_on_track_story(
                                pattern_result, metric_id, metric, grain, forecast_stats
                            )
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
                if pacing and pacing.status and pacing.projected_value is not None:
                    if pacing.status == "on_track":
                        stories.append(
                            self._create_pacing_on_track_story(pattern_result, metric_id, metric, grain, pacing)
                        )
                    elif pacing.status == "off_track":
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
            context.update(
                {
                    "percent_elapsed": pacing.period_elapsed_percent or 0,
                    "projected_value": pacing.projected_value,
                    "target_value": pacing.target_value,
                    "gap_percent": abs(pacing.gap_percent or 0),
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
                    "target_value": context.get("target_value"),  # Use target from forecast_stats if available
                    "growth_difference": abs(growth_difference),
                    "trend_direction": trend_direction,
                    "previous_growth": abs(required_perf.previous_pop_growth_percent or 0),
                    "previous_periods": required_perf.previous_periods,
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
        series_df = self._prepare_forecast_series_data(pattern_result, grain, forecast_stats)
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
        series_df = self._prepare_forecast_series_data(pattern_result, grain, forecast_stats)
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
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.REQUIRED_PERFORMANCE

        # Prepare context for template rendering
        context = self._populate_template_context(
            pattern_result=pattern_result, metric=metric, grain=grain, required_perf=required_perf
        )

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare required performance series data for visualization
        series_df = self._prepare_required_performance_series_data(pattern_result, grain, required_perf)
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

    def _prepare_forecast_series_data(
        self, pattern_result: Forecasting, target_grain: Granularity, forecast_stats=None
    ) -> pd.DataFrame:
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
            DataFrame with combined historical and forecast data, including:
            - date: Date of the data point
            - value: Actual value (for historical) or forecasted_value (for forecast)
            - lower_bound: Lower confidence bound (forecast only)
            - upper_bound: Upper confidence bound (forecast only)
        """
        # Start with actual data (if available)
        if self.series_df is not None and not self.series_df.empty:
            actual_df = self.series_df.copy()
            actual_df["date"] = pd.to_datetime(actual_df["date"])

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

        if forecast_data:
            forecast_df = pd.DataFrame(forecast_data)
            forecast_df["date"] = pd.to_datetime(forecast_df["date"])

            # Filter forecast data to only include dates up to the period end date if forecast_stats is provided
            if forecast_stats and forecast_stats.period:
                analysis_date = pd.to_datetime(pattern_result.analysis_date)
                period_end_date = get_period_end_date(analysis_date, forecast_stats.period)
                period_end_date = pd.to_datetime(period_end_date)

                # Filter to only include forecast dates up to the period end date
                forecast_df = forecast_df[forecast_df["date"] <= period_end_date].copy()

            # Convert forecast from day grain to target grain
            if not forecast_df.empty:
                forecast_df = self._convert_daily_forecast_to_grain(forecast_df, target_grain)
        else:
            forecast_df = pd.DataFrame(columns=["date", "value", "lower_bound", "upper_bound"])

        # Combine actual and forecast data
        combined_df = pd.concat([actual_df, forecast_df], ignore_index=True)

        return combined_df

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
        self, pattern_result: Forecasting, grain: Granularity, required_perf=None
    ) -> pd.DataFrame:
        """
        Prepare series data for required performance visualization.

        This method creates a dataset showing:
        - Actual growth rates (calculated from actual values)
        - Required growth rate for remaining periods (from required_performance)

        Uses the same date generation logic as forecast series for consistency.

        Args:
            pattern_result: Forecasting pattern result containing required_performance data
            grain: Granularity of the analysis
            required_perf: The specific required performance object

        Returns:
            DataFrame with columns: ["date", "value", "required_growth_percent", "pop_growth_percent"]
        """
        # Return empty if no series data
        if self.series_df is None or self.series_df.empty:
            return pd.DataFrame(columns=["date", "value", "required_growth_percent", "pop_growth_percent"])

        # Prepare historical data with growth rates
        actual_df = self.series_df.copy()
        actual_df["date"] = pd.to_datetime(actual_df["date"])
        actual_df = actual_df.sort_values("date")
        actual_df["pop_growth_percent"] = actual_df["value"].pct_change() * 100
        actual_df["required_growth_percent"] = None

        # Remove first row (NaN growth)
        historical_df = actual_df.iloc[1:].copy()

        # Return if no required performance data
        if not required_perf or required_perf.required_pop_growth_percent is None:
            return historical_df

        # Extract required growth parameters
        required_growth = required_perf.required_pop_growth_percent
        remaining_periods = required_perf.remaining_periods or 0
        if remaining_periods <= 0:
            return historical_df

        # Get last historical date and period end date
        last_date = (
            historical_df["date"].max() if not historical_df.empty else pd.to_datetime(pattern_result.analysis_date)
        )

        period_end_date = None
        if required_perf.period:
            analysis_date = pd.to_datetime(pattern_result.analysis_date)
            period_end_date = pd.to_datetime(get_period_end_date(analysis_date, required_perf.period))

        # Generate daily forecast data for future period (same as forecast method)
        if period_end_date:
            # Create daily dates from last_date+1 to period_end_date
            daily_future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), end=period_end_date, freq="D")
        else:
            # Fallback: generate based on remaining_periods (approximate)
            days_ahead = remaining_periods * 7 if grain == Granularity.WEEK else remaining_periods
            daily_future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=days_ahead, freq="D")

        if len(daily_future_dates) == 0:
            return historical_df

        # Create daily DataFrame with forecast values (placeholder values for conversion)
        daily_forecast_df = pd.DataFrame(
            {
                "date": daily_future_dates,
                "value": required_growth,  # Use required growth as placeholder value
                "lower_bound": None,
                "upper_bound": None,
            }
        )

        # Convert daily data to target grain using SAME method as forecast
        future_df = self._convert_daily_forecast_to_grain(daily_forecast_df, grain)

        # Filter to period end date if specified (same logic as forecast)
        if period_end_date:
            future_df = future_df[future_df["date"] <= period_end_date]

        # Replace forecast values with required performance data
        future_df["value"] = None
        future_df["required_growth_percent"] = required_growth
        future_df["pop_growth_percent"] = None
        future_df = future_df.drop(columns=["lower_bound", "upper_bound"], errors="ignore")

        # Combine historical and future data
        result_df = pd.concat([historical_df, future_df], ignore_index=True)
        return result_df.sort_values("date").reset_index(drop=True)
