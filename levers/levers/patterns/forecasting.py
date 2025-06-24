"""
Forecasting Pattern

This module implements the Forecasting pattern which generates forecasts for a given metric,
including projections for specific future period end-dates and a detailed daily forecast.
"""

import logging
from datetime import date, datetime
from typing import Any

import pandas as pd

from levers.exceptions import ValidationError
from levers.models import (
    AnalysisWindow,
    AnalysisWindowConfig,
    DailyForecast,
    DataSource,
    DataSourceType,
    Granularity,
    PacingProjection,
    PatternConfig,
    RequiredPerformance,
    StatisticalForecast,
    WindowStrategy,
)
from levers.models.enums import PeriodType
from levers.models.patterns import Forecasting
from levers.patterns import Pattern
from levers.primitives import (
    calculate_pop_growth,
    calculate_required_growth,
    classify_metric_status,
    forecast_with_confidence_intervals,
    get_period_end_date,
    get_period_range_for_grain,
)

logger = logging.getLogger(__name__)


class ForecastingPattern(Pattern[Forecasting]):
    """Pattern for generating forecasts for metrics with multiple forecast periods and daily projections."""

    name = "forecasting"
    version = "1.0"
    description = (
        "Generates forecasts for a given metric, including projections for specific future period end-dates "
        "and a detailed daily forecast"
    )
    required_primitives = [
        "forecast_with_confidence_intervals",
        "calculate_required_growth",
        "classify_metric_status",
        "calculate_pop_growth",
        "get_period_end_date",
        "get_period_range_for_grain",
    ]
    output_model: type[Forecasting] = Forecasting

    @classmethod
    def get_default_config(cls) -> PatternConfig:
        """Get the default configuration for the forecasting pattern."""
        return PatternConfig(
            pattern_name=cls.name,
            description=cls.description,
            version=cls.version,
            data_sources=[
                DataSource(source_type=DataSourceType.METRIC_WITH_TARGETS, is_required=True, data_key="data"),
            ],
            analysis_window=AnalysisWindowConfig(
                strategy=WindowStrategy.FIXED_TIME, days=365, min_days=90, max_days=730, include_today=True
            ),
            settings={
                "forecast_horizon_days": 90,
                "confidence_level": 0.95,
                "pacing_status_threshold_pct": 5.0,
                "num_past_periods_for_growth": 4,
            },
        )

    def analyze(  # type: ignore
        self,
        metric_id: str,
        data: pd.DataFrame,
        analysis_window: AnalysisWindow,
        analysis_date: date | None = None,
        forecast_horizon_days: int = 90,
        confidence_level: float = 0.95,
        pacing_status_threshold_pct: float = 5.0,
        num_past_periods_for_growth: int = 4,
    ) -> Forecasting:
        """
        Execute the forecasting pattern.

        Args:
            metric_id: The ID of the metric being analyzed
            data: DataFrame containing columns: date, value, grain, and target_value, target_date, etc.
            analysis_window: AnalysisWindow object specifying the analysis time window
            analysis_date: Date from which forecasts are made (defaults to today)
            forecast_horizon_days: Number of days to forecast ahead
            confidence_level: Confidence level for prediction intervals
            pacing_status_threshold_pct: Threshold percentage for pacing status
            num_past_periods_for_growth: Number of past periods for historical growth calculation

        Returns:
            Forecasting object with forecast results
        """
        try:
            # Set analysis date to today if not provided
            if analysis_date is None:
                analysis_date = date.today()

            analysis_dt = pd.to_datetime(analysis_date)
            grain = analysis_window.grain

            # Validate input data
            required_columns = ["date", "value"]
            self.validate_data(data, required_columns)

            # Process data and determine grain
            df = self.preprocess_data(data, analysis_window)

            # Handle empty data
            if df.empty:
                return self.handle_empty_data(metric_id, analysis_window)

            # Filter data for the primary grain and up to analysis date
            hist_df = df[(df.get("grain", grain) == grain) & (pd.to_datetime(df["date"]) <= analysis_dt)].copy()

            if hist_df.empty:
                return self.handle_empty_data(metric_id, analysis_window)

            hist_df = hist_df.sort_values("date").reset_index(drop=True)

            # Generate daily forecast with confidence intervals
            daily_forecast_data = []
            try:
                daily_fc_df = forecast_with_confidence_intervals(
                    df=hist_df,
                    value_col="value",
                    periods=forecast_horizon_days,
                    confidence_level=confidence_level,
                    date_col="date",
                    grain=grain,
                )

                for _, row in daily_fc_df.iterrows():
                    daily_forecast_data.append(
                        DailyForecast(
                            date=pd.to_datetime(row["date"]).strftime("%Y-%m-%d"),
                            forecasted_value=round(row["forecast"], 2) if pd.notna(row["forecast"]) else None,
                            lower_bound=round(row["lower_bound"], 2) if pd.notna(row["lower_bound"]) else None,
                            upper_bound=round(row["upper_bound"], 2) if pd.notna(row["upper_bound"]) else None,
                            confidence_level=confidence_level,
                        )
                    )

            except Exception as e:
                logger.warning(f"Error generating daily forecast: {str(e)}")
                # Continue with empty daily forecast

            # Process single forecast period (defaulting to end of month)
            period_name = PeriodType.END_OF_MONTH

            # Get latest actual value for required growth calculations
            latest_actual_value = hist_df["value"].iloc[-1] if not hist_df.empty else 0

            try:
                forecast_target_date = get_period_end_date(analysis_dt, period_name)

                # Initialize sections
                statistical = StatisticalForecast()

                # Statistical Forecast Section
                fc_row = None
                if daily_forecast_data:
                    # Find forecast for the target date
                    target_date_str = forecast_target_date.strftime("%Y-%m-%d")
                    fc_row = next((item for item in daily_forecast_data if item.date == target_date_str), None)

                if fc_row:
                    statistical.forecasted_value = fc_row.forecasted_value
                    statistical.lower_bound = fc_row.lower_bound
                    statistical.upper_bound = fc_row.upper_bound

                statistical.confidence_level = confidence_level

                # Get target value if available
                current_target_value = None
                has_targets = False

                # Check if data has target information and it's not all NaN/zero
                if "target_value" in data.columns:
                    # Filter target data for the forecast target date
                    target_rows = data[
                        (pd.to_datetime(data["date"]) == forecast_target_date)
                        & (data["target_value"].notna())
                        & (data["target_value"] != 0)
                    ]

                    if not target_rows.empty:
                        current_target_value = float(target_rows["target_value"].iloc[0])
                        has_targets = True

                statistical.target_value = current_target_value

                # Calculate gap and status only if we have both forecast and target
                if statistical.forecasted_value is not None and current_target_value is not None:
                    gap_pct = (statistical.forecasted_value / current_target_value - 1) * 100
                    statistical.forecasted_gap_percent = round(gap_pct, 2)

                    statistical.forecast_status = classify_metric_status(
                        statistical.forecasted_value,
                        current_target_value,
                        threshold_ratio=pacing_status_threshold_pct / 100.0,
                    )

                # Pacing Projection Section - only calculate if we have targets
                pacing = None
                if has_targets:
                    pacing_info = self._calculate_pacing_projection(
                        hist_df, analysis_dt, period_name, current_target_value, pacing_status_threshold_pct
                    )
                    if pacing_info:
                        pacing = PacingProjection(
                            percent_of_period_elapsed=pacing_info.get("percent_of_period_elapsed"),
                            current_cumulative_value=pacing_info.get("current_cumulative_value"),
                            projected_value=pacing_info.get("pacing_projected_value"),
                            gap_percent=pacing_info.get("pacing_gap_percent"),
                            status=pacing_info.get("pacing_status"),
                        )

                # Required Performance Section - only calculate if we have targets
                required_performance = None
                if has_targets and current_target_value is not None:
                    remaining_grains = self._calculate_remaining_grains(analysis_dt, forecast_target_date, grain)

                    required_performance = RequiredPerformance(remaining_grains_count=remaining_grains)

                    if remaining_grains > 0:
                        req_growth = calculate_required_growth(
                            current_value=latest_actual_value,
                            target_value=current_target_value,
                            periods_remaining=remaining_grains,
                        )
                        required_performance.required_pop_growth_percent = (
                            round(req_growth * 100, 2) if req_growth is not None else None
                        )

                    # Calculate historical growth
                    if len(hist_df) >= num_past_periods_for_growth + 1:
                        past_df = hist_df.tail(num_past_periods_for_growth + 1).copy()
                        past_df_growth = calculate_pop_growth(past_df, date_col="date", value_col="value", periods=1)
                        avg_past_growth = past_df_growth["pop_growth"].mean()
                        required_performance.past_pop_growth_percent = (
                            round(avg_past_growth, 2) if pd.notna(avg_past_growth) else None
                        )

                    # Calculate delta from historical growth
                    if (
                        required_performance.required_pop_growth_percent is not None
                        and required_performance.past_pop_growth_percent is not None
                    ):
                        required_performance.delta_from_historical_growth = round(
                            required_performance.required_pop_growth_percent
                            - required_performance.past_pop_growth_percent,
                            2,
                        )

            except Exception as e:
                logger.warning(f"Error processing period {period_name}: {str(e)}")
                # Initialize empty sections on error
                statistical = StatisticalForecast()
                pacing = None
                required_performance = None

            # Create result
            result = Forecasting(
                pattern=self.name,
                version=self.version,
                metric_id=metric_id,
                analysis_date=analysis_date,
                evaluation_time=datetime.now(),
                analysis_window=analysis_window,
                period_name=period_name,
                statistical=statistical,
                pacing=pacing,
                required_performance=required_performance,
                daily_forecast=daily_forecast_data,
            )

            return self.validate_output(result)

        except Exception as e:
            raise ValidationError(
                f"Error in forecasting pattern calculation: {str(e)}",
                {"pattern": self.name, "metric_id": metric_id},
            ) from e

    def _calculate_pacing_projection(
        self,
        hist_df: pd.DataFrame,
        analysis_dt: pd.Timestamp,
        period_name: str,
        target_value: float | None,
        threshold_pct: float,
    ) -> dict[str, Any]:
        """Calculate pacing projection for the current period."""
        result: dict = {}

        # Determine period type
        if period_name == PeriodType.END_OF_WEEK:
            pacing_period_grain = Granularity.WEEK
        elif period_name == PeriodType.END_OF_MONTH:
            pacing_period_grain = Granularity.MONTH
        elif period_name == PeriodType.END_OF_QUARTER:
            pacing_period_grain = Granularity.QUARTER
        elif period_name == PeriodType.END_OF_YEAR:
            pacing_period_grain = Granularity.YEAR
        elif period_name == PeriodType.END_OF_NEXT_MONTH:
            pacing_period_grain = Granularity.MONTH

        try:
            current_period_start, current_period_end = get_period_range_for_grain(analysis_dt, pacing_period_grain)

            if analysis_dt >= current_period_start:
                elapsed_days = (analysis_dt - current_period_start).days + 1
                total_days_in_period = (current_period_end - current_period_start).days + 1

                result["percent_of_period_elapsed"] = (
                    round((elapsed_days / total_days_in_period) * 100.0, 2) if total_days_in_period > 0 else 0.0
                )

                # Cumulative value from hist_df
                actuals_in_current_period = hist_df[
                    (pd.to_datetime(hist_df["date"]) >= current_period_start)
                    & (pd.to_datetime(hist_df["date"]) <= analysis_dt)
                ]
                current_cumulative_value = actuals_in_current_period["value"].sum()
                result["current_cumulative_value"] = round(current_cumulative_value, 2)

                if 0 < result["percent_of_period_elapsed"] < 100:
                    result["pacing_projected_value"] = round(
                        (current_cumulative_value / result["percent_of_period_elapsed"]) * 100.0, 2
                    )
                elif result["percent_of_period_elapsed"] >= 100:
                    result["pacing_projected_value"] = round(current_cumulative_value, 2)

                # Calculate pacing status
                if result.get("pacing_projected_value") is not None and target_value is not None and target_value != 0:
                    pacing_gap_pct = (result["pacing_projected_value"] / target_value - 1) * 100
                    result["pacing_gap_percent"] = round(pacing_gap_pct, 2)
                    result["pacing_status"] = classify_metric_status(
                        result["pacing_projected_value"], target_value, threshold_ratio=threshold_pct / 100.0
                    )
            else:
                result["percent_of_period_elapsed"] = 0.0
                result["current_cumulative_value"] = 0.0
                result["pacing_status"] = "not_yet_started"

        except Exception as e:
            logger.warning(f"Error calculating pacing projection: {str(e)}")

        return result

    def _calculate_remaining_grains(self, analysis_dt: pd.Timestamp, target_date: pd.Timestamp, grain: str) -> int:
        """Calculate remaining grains based on the main grain of the metric."""
        remaining_grains_count = 0

        try:
            if grain == Granularity.DAY:
                remaining_grains_count = (target_date - analysis_dt).days
            elif grain == Granularity.WEEK:
                # Count weeks from next week start to target date's week
                next_week_start = (analysis_dt + pd.offsets.Week(weekday=0) + pd.Timedelta(weeks=1)).normalize()
                if next_week_start <= target_date:
                    temp_date = next_week_start
                    while temp_date <= target_date:
                        remaining_grains_count += 1
                        temp_date += pd.Timedelta(weeks=1)
            elif grain == Granularity.MONTH:
                # Count months from next month start to target date's month
                next_month_start = (analysis_dt.replace(day=1) + pd.offsets.MonthBegin(1)).normalize()
                if next_month_start <= target_date:
                    temp_date = next_month_start
                    while temp_date <= target_date:
                        remaining_grains_count += 1
                        temp_date = (temp_date.replace(day=1) + pd.offsets.MonthBegin(1)).normalize()
        except Exception as e:
            logger.warning(f"Error calculating remaining grains: {str(e)}")

        return max(0, remaining_grains_count)
