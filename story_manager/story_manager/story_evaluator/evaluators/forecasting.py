"""
Story evaluator for the forecasting pattern.
"""

import logging
from typing import Any

import pandas as pd

from commons.models.enums import Granularity
from levers.models.enums import MetricGVAStatus
from levers.models.patterns.forecasting import Forecasting
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

        # Generate forecast vs target stories
        if pattern_result.forecast_vs_target_stats:
            forecast_stats = pattern_result.forecast_vs_target_stats
            if forecast_stats.status == MetricGVAStatus.ON_TRACK:
                stories.append(self._create_forecasted_on_track_story(pattern_result, metric_id, metric, grain))
            elif forecast_stats.status == MetricGVAStatus.OFF_TRACK:
                stories.append(self._create_forecasted_off_track_story(pattern_result, metric_id, metric, grain))

        # Generate pacing stories
        if pattern_result.pacing:
            pacing = pattern_result.pacing
            if pacing.status and pacing.projected_value is not None:
                if pacing.status == "on_track":
                    stories.append(self._create_pacing_on_track_story(pattern_result, metric_id, metric, grain))
                elif pacing.status == "off_track":
                    stories.append(self._create_pacing_off_track_story(pattern_result, metric_id, metric, grain))

        # Generate required performance story
        if pattern_result.required_performance:
            required_perf = pattern_result.required_performance
            if (
                required_perf.required_pop_growth_percent is not None
                and required_perf.remaining_periods is not None
                and required_perf.remaining_periods > 0
            ):
                stories.append(self._create_required_performance_story(pattern_result, metric_id, metric, grain))

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
            **kwargs: Additional keyword arguments

        Returns:
            Template context dictionary
        """

        context = self.prepare_base_context(metric, grain)

        context["period"] = pattern_result.forecast_period_grain

        forecast_stats = pattern_result.forecast_vs_target_stats
        if forecast_stats:
            context.update(
                {
                    "forecasted_value": forecast_stats.forecasted_value,
                    "target_value": forecast_stats.target_value,
                    "gap_percent": abs(forecast_stats.gap_percent or 0),
                }
            )

        pacing = pattern_result.pacing
        if pacing and forecast_stats:
            context.update(
                {
                    "percent_elapsed": pacing.period_elapsed_percent or 0,
                    "projected_value": pacing.projected_value,
                    "target_value": forecast_stats.target_value,
                    "gap_percent": abs(pacing.gap_percent or 0),
                }
            )

        required_perf = pattern_result.required_performance
        if required_perf and forecast_stats:
            # Determine trend direction
            growth_difference = required_perf.growth_difference or 0
            trend_direction = "increase" if growth_difference > 0 else "decrease"

            context.update(
                {
                    "required_growth": abs(required_perf.required_pop_growth_percent or 0),
                    "remaining_periods": required_perf.remaining_periods,
                    "target_value": forecast_stats.target_value,
                    "growth_difference": abs(growth_difference),
                    "trend_direction": trend_direction,
                    "previous_growth": abs(required_perf.previous_pop_growth_percent or 0),
                    "previous_periods": required_perf.previous_num_periods,
                }
            )

        return context

    def _create_forecasted_on_track_story(
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a FORECASTED_ON_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.FORECASTED_ON_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare combined historical + forecast series data
        series_df = self._prepare_forecast_series_data(pattern_result)
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
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a FORECASTED_OFF_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.FORECASTED_OFF_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare combined historical + forecast series data
        series_df = self._prepare_forecast_series_data(pattern_result)
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
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a PACING_ON_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.PACING_ON_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)

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
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a PACING_OFF_TRACK story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.PACING_OFF_TRACK

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)

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
        self, pattern_result: Forecasting, metric_id: str, metric: dict, grain: Granularity
    ) -> dict[str, Any]:
        """
        Create a REQUIRED_PERFORMANCE story.

        Args:
            pattern_result: Forecasting pattern result
            metric_id: Metric ID
            metric: Metric details
            grain: Granularity of the analysis

        Returns:
            Story dictionary
        """
        story_group = StoryGroup.LIKELY_STATUS
        story_type = StoryType.REQUIRED_PERFORMANCE

        # Prepare context for template rendering
        context = self._populate_template_context(pattern_result, metric, grain)

        # Render title and detail from templates
        title = render_story_text(story_type, "title", context)
        detail = render_story_text(story_type, "detail", context)

        # Prepare required performance series data for visualization
        series_df = self._prepare_required_performance_series_data(pattern_result, grain)
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

    def _prepare_forecast_series_data(self, pattern_result: Forecasting) -> pd.DataFrame:
        """
        Prepare combined historical and forecast series data for forecasting stories.

        This method combines the actual time series data (from self.series_df) with
        the forecast data (from pattern_result.period_forecast) to create a complete
        visualization dataset that shows both past actuals and future projections.

        Args:
            pattern_result: Forecasting pattern result containing period_forecast data

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

        forecast_data = []
        for forecast in pattern_result.period_forecast:  # type: ignore
            forecast_point = {
                "date": pd.to_datetime(forecast.date),
                "value": forecast.forecasted_value,
                "lower_bound": forecast.lower_bound,
                "upper_bound": forecast.upper_bound,
            }
            forecast_data.append(forecast_point)

        forecast_df = pd.DataFrame(forecast_data)

        # Combine actual and forecast data
        combined_df = pd.concat([actual_df, forecast_df], ignore_index=True)

        return combined_df

    def _prepare_required_performance_series_data(
        self, pattern_result: Forecasting, grain: Granularity
    ) -> pd.DataFrame:
        """
        Prepare series data for required performance visualization.

        This method creates a dataset showing:
        - Actual growth rates (calculated from actual values)
        - Required growth rate for remaining periods (from required_performance)

        Args:
            pattern_result: Forecasting pattern result containing required_performance data

        Returns:
            DataFrame with growth rate data over time
        """
        series_data = []

        if self.series_df is None or self.series_df.empty:
            return pd.DataFrame(columns=["date", "value", "required_growth_percent", "pop_growth_percent"])

        # Calculate historical growth rates from actual data
        actual_df = self.series_df.copy()
        actual_df["date"] = pd.to_datetime(actual_df["date"])
        actual_df = actual_df.sort_values("date")

        # Calculate period-over-period growth rates
        actual_df["pop_growth_percent"] = actual_df["value"].pct_change() * 100

        # Add historical growth rate points (excluding first NaN point)
        for _, row in actual_df.iloc[1:].iterrows():
            if pd.notna(row["pop_growth_percent"]):
                series_data.append(
                    {
                        "date": row["date"],
                        "value": row["value"],
                        "pop_growth_percent": row["pop_growth_percent"],
                        "required_growth_percent": None,
                    }
                )

        # Add required growth rate for future periods
        required_perf = pattern_result.required_performance
        required_growth = required_perf.required_pop_growth_percent  # type: ignore
        remaining_periods = required_perf.remaining_periods or 0  # type: ignore

        last_date = (
            max(point["date"] for point in series_data) if series_data else pd.to_datetime(pattern_result.analysis_date)
        )

        # Create future dates for required growth visualization
        freq = {"day": "D", "week": "W-MON", "month": "M", "quarter": "Q", "year": "Y"}.get(grain.value, "D")

        # Generate future dates for required growth
        future_dates = pd.date_range(
            start=last_date + pd.Timedelta(days=1), periods=min(remaining_periods, 12), freq=freq
        )

        for future_date in future_dates:
            series_data.append(
                {
                    "date": future_date,
                    "value": None,
                    "required_growth_percent": required_growth,
                    "pop_growth_percent": None,
                }
            )

        return pd.DataFrame(series_data)
