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
    DataSource,
    DataSourceType,
    Forecast,
    ForecastVsTargetStats,
    Granularity,
    PacingProjection,
    PatternConfig,
    RequiredPerformance,
    WindowStrategy,
)
from levers.models.enums import ForecastMethod, PeriodType
from levers.models.forecasting import ForecastWindow
from levers.models.patterns import Forecasting
from levers.patterns import Pattern
from levers.primitives import (
    calculate_cumulative_aggregate,
    calculate_pop_growth,
    calculate_remaining_periods,
    calculate_required_growth,
    classify_metric_status,
    forecast_with_confidence_intervals,
    get_period_end_date,
    get_period_range_for_grain,
)
from levers.primitives.period_grains import get_dates_for_a_range

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
        "calculate_remaining_periods",
        "calculate_cumulative_aggregate",
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
                DataSource(
                    source_type=DataSourceType.METRIC_TIME_SERIES, is_required=True, data_key="data", look_forward=False
                ),
                DataSource(
                    source_type=DataSourceType.TARGETS,
                    is_required=True,
                    data_key="target",
                    look_forward=True,
                ),
            ],
            analysis_window=AnalysisWindowConfig(
                strategy=WindowStrategy.FIXED_TIME, days=365, min_days=90, max_days=730, include_today=False
            ),
            settings={
                "confidence_level": 0.95,
                "pacing_status_threshold_pct": 5.0,
            },
        )

    def analyze(  # type: ignore
        self,
        metric_id: str,
        data: pd.DataFrame,
        target: pd.DataFrame,
        analysis_window: AnalysisWindow,
        analysis_date: date | None = None,
        confidence_level: float = 0.95,
        pacing_status_threshold_pct: float = 5.0,
    ) -> Forecasting:
        """
        Execute the forecasting pattern.

        Args:
            metric_id: The ID of the metric being analyzed
            data: DataFrame containing columns: date, value, grain, and target_value, target_date, etc.
            target: DataFrame containing columns: date, target_value, target_date, etc.
            analysis_window: AnalysisWindow object specifying the analysis time window
            analysis_date: Date from which forecasts are made (defaults to today)
            confidence_level: Confidence level for prediction intervals
            pacing_status_threshold_pct: Threshold percentage for pacing status

        Returns:
            Forecasting object with forecast results

        Raises:
            ValidationError: If input validation fails or calculation errors occur
        """
        try:
            # Set analysis date to today if not provided
            if analysis_date is None:
                analysis_date = date.today()

            analysis_dt = pd.to_datetime(analysis_date)
            grain = analysis_window.grain

            if grain != Granularity.DAY:
                logger.info(f"Forecasting pattern is not supported for grain: {grain}")
                return self.handle_empty_data(
                    metric_id,
                    analysis_window,
                    error=dict(
                        message="Forecasting pattern is not supported for grain: {grain}",
                        type="grain_not_supported",
                    ),
                )

            # Validate input data
            required_columns = ["date", "value"]
            self.validate_data(data, required_columns)

            # Process data
            df = self.preprocess_data(data, analysis_window)

            # Handle empty data
            if df.empty:
                return self.handle_empty_data(metric_id, analysis_window)

            # Trim daily_df to only include data up to analysis_date
            # This ensures the forecast starts from analysis_date, not from the last historical date
            df["date"] = pd.to_datetime(df["date"])
            df = df[pd.to_datetime(df["date"]) <= analysis_dt].copy()

            # Get the number of periods to forecast
            forecast_periods = self._get_forecast_periods_count(analysis_dt)

            # Generate forecast
            forecast_df = forecast_with_confidence_intervals(
                df=df,
                value_col="value",
                confidence_level=confidence_level,
                date_col="date",
                grain=Granularity.DAY,
                method=ForecastMethod.PROPHET,
                periods=forecast_periods + 1,  # include current period
                analysis_date=analysis_dt,
            )

            # Handle empty forecast
            if forecast_df.empty:
                return self.handle_empty_data(metric_id, analysis_window)

            # Handle empty target
            if target.empty or "target_value" not in target.columns or "date" not in target.columns:
                logger.error(f"Target data is empty or missing required columns. Target: {target}")
                return self._handle_min_data(metric_id, analysis_window, forecast_data=forecast_df)

            forecast_vs_target_stats = []
            pacing = []
            required_performance = []

            # Process single forecast period
            periods = self._get_periods_for_grain(grain)
            for period in periods:
                # Get period start date instead of end date for target matching
                pacing_grain = self._get_pacing_grain_for_period(period)
                period_start_date, period_end_date = get_period_range_for_grain(
                    analysis_dt, pacing_grain, include_today=True
                )
                target_value = self._get_target_value(target, period_start_date, pacing_grain)

                if target_value is None:
                    logger.error(f"No target value found for period: {period} and target date: {period_start_date}")
                    continue

                # Get forecast vs target stats
                forecast_vs_target_stats.append(
                    self._get_forecast_vs_target_stats(
                        forecast_df=forecast_df,
                        actual_df=df,
                        target_value=target_value,
                        period_end_date=period_end_date,
                        analysis_dt=analysis_dt,
                        period_start_date=period_start_date,
                        period=period,
                    )
                )

                # Get pacing projection
                pacing.append(
                    self._calculate_pacing_projection(
                        df=df,
                        analysis_dt=analysis_dt,
                        target_value=target_value,
                        pacing_status_threshold_pct=pacing_status_threshold_pct,
                        pacing_grain=pacing_grain,
                        period=period,
                    )
                )

                # Get required performance
                required_performance.append(
                    self._calculate_required_performance(
                        df=df,
                        analysis_dt=analysis_dt,
                        grain=grain,
                        target_value=target_value,
                        period=period,
                        period_start_date=period_start_date,
                        period_end_date=period_end_date,
                    )
                )

            forecast_data = self._prepare_forecast_data(forecast_df)  # type: ignore

            # Get the forecast window
            forecast_window = self._get_forecast_window(forecast_df)

            # Create result
            result = Forecasting(
                pattern=self.name,
                version=self.version,
                metric_id=metric_id,
                analysis_date=analysis_date,
                evaluation_time=datetime.now(),
                analysis_window=analysis_window,
                num_periods=len(df),
                forecast_vs_target_stats=forecast_vs_target_stats,  # type: ignore
                pacing=pacing,  # type: ignore
                required_performance=required_performance,  # type: ignore
                forecast=forecast_data,  # type: ignore
                forecast_window=forecast_window,
            )

            return self.validate_output(result)

        except Exception as e:
            raise ValidationError(
                f"Error in forecasting pattern calculation: {str(e)}",
                {"pattern": self.name, "metric_id": metric_id},
            ) from e

    def _get_forecast_vs_target_stats(
        self,
        forecast_df: pd.DataFrame,
        actual_df: pd.DataFrame,
        target_value: float,
        period: PeriodType,
        analysis_dt: pd.Timestamp,
        **kwargs,
    ) -> ForecastVsTargetStats:
        """
        Get the forecast vs target stats.

        This method calculates the forecast vs target stats by comparing the cumulative
        values (actual + forecast) for the period against the target value.

        Args:
            forecast_df: DataFrame containing forecasted values and confidence intervals.
            actual_df: DataFrame containing actual historical values
            target_value: Target value for the period.
            period: The period type (endOfMonth, endOfQuarter, etc.)
            analysis_dt: Analysis date (start of forecast period)
            **kwargs: Additional parameters including:
                - period_start_date: Start date for the period (already calculated in loop)
                - period_end_date: End date for the forecast period (already calculated in loop)

        Returns:
            ForecastVsTargetStats: Forecast vs target stats.
            The forecast vs target stats include:
                - Period
                - Cumulative forecasted value for the period
                - Target value
                - gap percent
                - status
        """
        # Initialize sections
        stats = ForecastVsTargetStats(period=period, target_value=target_value)

        # Get period start date from kwargs (already calculated in the loop)
        period_start_date = kwargs.get("period_start_date")
        period_end_date = kwargs.get("period_end_date")

        # Get actual values from period start to analysis_dt (exclusive)
        period_actuals = actual_df[
            (pd.to_datetime(actual_df["date"]) >= period_start_date) & (pd.to_datetime(actual_df["date"]) < analysis_dt)  # type: ignore
        ]

        # Get forecast values from analysis_dt to period_end_date (inclusive)
        period_forecast = forecast_df[
            (pd.to_datetime(forecast_df["date"]) >= analysis_dt)
            & (pd.to_datetime(forecast_df["date"]) <= period_end_date)  # type: ignore
        ]

        # Merge actuals and forecast series
        combined_series = pd.concat(
            [
                period_actuals["value"] if not period_actuals.empty else pd.Series([]),
                period_forecast["forecast"] if not period_forecast.empty else pd.Series([]),
            ],
            ignore_index=True,
        )

        # Calculate cumulative value (actual + forecast) for the period in one call
        cumulative_forecast_value = (
            calculate_cumulative_aggregate(
                series=combined_series,  # type: ignore
                aggregation_method="sum",  # TODO:  this should come from the metric definition
            )
            or 0
        )

        stats.forecasted_value = round(cumulative_forecast_value, 2)

        # Calculate gap and status only if we have both forecast and target
        if stats.forecasted_value is not None and target_value != 0:
            gap_pct = (target_value - stats.forecasted_value) / target_value * 100
            stats.gap_percent = abs(round(gap_pct, 2))

            stats.status = classify_metric_status(stats.forecasted_value, target_value)

        return stats

    def _calculate_pacing_projection(
        self,
        df: pd.DataFrame,
        analysis_dt: pd.Timestamp,
        target_value: float | None,
        pacing_status_threshold_pct: float,
        pacing_grain: Granularity,
        period: PeriodType,
    ) -> PacingProjection:
        """
        Calculate pacing projection for the period. This method calculates the pacing projection for the period based
        on actual values up to analysis_dt and projected values for the remaining period using pop growth.
        The projected value is calculated by combining actual values with projected values based on historical
        pop growth for the remaining dates in the period.

        Args:
            df: DataFrame containing the data.
            analysis_dt: Analysis date.
            target_value: Target value.
            pacing_status_threshold_pct: Pacing status threshold percentage.
            pacing_grain: Granularity for the pacing period.
            period: Period type.

        Returns:
            PacingProjection: Pacing projection.
            The pacing projection includes:
                - Percent of period elapsed
                - Cumulative value
                - Projected value
                - Gap percent
                - Status
        """
        # Initialize result
        result = PacingProjection(target_value=target_value, period=period)

        # Get the start and end dates for the pacing period
        # Use include_today=True to get the current active period (not the previous completed period)
        pacing_period_start, pacing_period_end = get_period_range_for_grain(
            analysis_dt, pacing_grain, include_today=True
        )

        # Check if analysis date is within the pacing period
        if not analysis_dt >= pacing_period_start:
            return result

        # Calculate elapsed days and total days
        elapsed_days = (analysis_dt - pacing_period_start).days + 1
        total_days = (pacing_period_end - pacing_period_start).days + 1

        # Percent of period elapsed
        result.period_elapsed_percent = round((elapsed_days / total_days) * 100.0, 2) if total_days > 0 else 0.0

        # Get actual values from period start to analysis_dt
        pacing_period_actuals = df[
            (pd.to_datetime(df["date"]) >= pacing_period_start) & (pd.to_datetime(df["date"]) <= analysis_dt)
        ]

        # If period is already complete, just use actual values
        if analysis_dt >= pacing_period_end:
            combined_series = pacing_period_actuals["value"]
            result.projected_value = round(
                calculate_cumulative_aggregate(
                    series=combined_series,
                    aggregation_method="sum",
                )
                or 0,
                2,
            )
        else:
            # Calculate pop growth from recent actual data for projections
            pop_growth_rate = 0.0
            if len(df) >= 4:
                recent_df = df.tail(4).copy()
                pop_growth_df = calculate_pop_growth(recent_df, date_col="date", value_col="value")
                if not pop_growth_df.empty and not pop_growth_df["pop_growth"].isna().all():
                    pop_growth_rate = pop_growth_df["pop_growth"].mean() / 100.0  # Convert percentage to decimal

            remaining_dates = get_dates_for_a_range(
                analysis_dt + pd.Timedelta(days=1), pacing_period_end, Granularity.DAY
            )

            projected_values = []
            if not pacing_period_actuals.empty and len(remaining_dates) > 0:
                # Use last actual value as base for projections
                last_actual_value = pacing_period_actuals["value"].iloc[-1]
                for i, _ in enumerate(remaining_dates):
                    # Apply pop growth for each remaining day
                    # exponential growth
                    # projected_value = last_actual_value * ((1 + pop_growth_rate) ** (i + 1))
                    # linear growth
                    projected_value = last_actual_value + (last_actual_value * pop_growth_rate * (i + 1))

                    projected_values.append(projected_value)
            # Combine actual and projected values
            combined_series = pd.concat(
                [
                    pacing_period_actuals["value"] if not pacing_period_actuals.empty else pd.Series([]),
                    pd.Series(projected_values) if projected_values else pd.Series([]),
                ],
                ignore_index=True,
            )

            # Calculate cumulative value and projected value
            cumulative_value = (
                calculate_cumulative_aggregate(
                    series=combined_series,
                    aggregation_method="sum",
                )
                or 0
            )

            result.projected_value = round(cumulative_value, 2)

        # Calculate pacing status
        if result.projected_value is not None and target_value is not None and target_value != 0:
            pacing_gap_pct = (result.projected_value / target_value - 1) * 100
            result.gap_percent = abs(round(pacing_gap_pct, 2))
            result.status = classify_metric_status(
                result.projected_value, target_value, threshold_ratio=pacing_status_threshold_pct / 100.0
            )

        return result

    def _calculate_required_performance(
        self, df: pd.DataFrame, analysis_dt: pd.Timestamp, grain: Granularity, **kwargs
    ) -> RequiredPerformance:
        """
        Calculate the required performance data for the period.
        This method calculates the required performance for the period based on the actual values in the period.
        The required performance is calculated by taking the remaining periods count and calculating the required
        growth.
        The required growth is calculated by taking the current value and target value and remaining periods.
        The past pop growth percent is calculated by taking the average of the past pop growth percent.

        Args:
            df: DataFrame containing the data.
            analysis_dt: Analysis date.
            grain: Grain.
            **kwargs: Additional keyword arguments.
                - target_value: Target value. (not used)
                - period_start_date: Period start date.
                - period_end_date: Period end date. (not used)
                - period: Period type.

        Returns:
            RequiredPerformance: Required performance.
            The required performance includes:
                - Remaining periods count
                - Required pop growth percent
                - Previous pop growth percent
                - Growth difference
        """

        # Get the forecast target date and value
        target_value = kwargs.get("target_value", 0)
        period_start_date = kwargs.get("period_start_date")
        period_end_date = kwargs.get("period_end_date")

        # Get the period
        period: PeriodType = kwargs.get("period")  # type: ignore

        # Get the number of past periods for growth calculation
        past_periods_count = (analysis_dt - period_start_date).days + 1  # type: ignore
        if past_periods_count < 1:
            return RequiredPerformance(period=period, previous_periods=past_periods_count)

        # Get the latest actual value
        latest_actual_value = float(df["value"].iloc[-1])

        # For forecasting, calculate remaining periods based on target date
        remaining_periods_count = calculate_remaining_periods(analysis_dt, period_end_date, grain)  # type: ignore

        required_performance = RequiredPerformance(remaining_periods=remaining_periods_count)

        # Calculate required growth if there are remaining periods
        if remaining_periods_count > 0:
            req_growth = calculate_required_growth(
                current_value=latest_actual_value,
                target_value=target_value,
                remaining_periods=remaining_periods_count,
            )
            required_performance.required_pop_growth_percent = (
                round(req_growth * 100, 2) if req_growth is not None else None
            )

        # Calculate past pop growth percent
        if len(df) >= past_periods_count:
            past_df = df.tail(past_periods_count).copy()
            past_df_growth = calculate_pop_growth(past_df, date_col="date", value_col="value", periods=1)
            avg_past_growth = past_df_growth["pop_growth"].mean()
            required_performance.previous_pop_growth_percent = (
                round(avg_past_growth, 2) if pd.notna(avg_past_growth) else None
            )

        # Calculate delta from historical growth
        if (
            required_performance.required_pop_growth_percent is not None
            and required_performance.previous_pop_growth_percent is not None
        ):
            required_performance.growth_difference = round(
                required_performance.required_pop_growth_percent - required_performance.previous_pop_growth_percent,
                2,
            )
        required_performance.previous_periods = past_periods_count
        required_performance.period = period
        return required_performance

    def _prepare_forecast_data(self, forecast_df: pd.DataFrame) -> list[Forecast] | list:  # type: ignore
        """
        Prepare forecast data.
        This method prepares the forecast data for the period.
        The forecast data is a list of Forecast objects.

        Args:
            forecast_df: DataFrame containing the forecast data.

        Returns:
            list[Forecast]: List of Forecast objects.
            The Forecast object includes:
                - Date
                - Forecasted value
                - Lower bound
                - Upper bound
                - Confidence level
        """
        forecast: list = []
        if forecast_df.empty:
            return forecast

        # Create a list of Forecast objects from the forecast DataFrame
        for _, row in forecast_df.iterrows():
            forecast.append(
                Forecast(
                    date=pd.to_datetime(row["date"]).strftime("%Y-%m-%d"),  # Use the date column instead of index
                    forecasted_value=round(row["forecast"], 2) if pd.notna(row["forecast"]) else None,
                    lower_bound=round(row["lower_bound"], 2) if pd.notna(row["lower_bound"]) else None,
                    upper_bound=round(row["upper_bound"], 2) if pd.notna(row["upper_bound"]) else None,
                    confidence_level=round(row["confidence_level"], 2) if pd.notna(row["confidence_level"]) else None,
                )
            )
        return forecast

    def _get_periods_for_grain(self, grain: Granularity) -> list[PeriodType]:
        """
        Get the periods for the given grain.

        Args:
            grain: The grain to get the periods for.

        Returns:
            list[PeriodType]: The periods for the given grain.
        """
        if grain == Granularity.DAY:
            return [PeriodType.END_OF_WEEK, PeriodType.END_OF_MONTH, PeriodType.END_OF_QUARTER, PeriodType.END_OF_YEAR]
        elif grain == Granularity.WEEK:
            return [PeriodType.END_OF_MONTH, PeriodType.END_OF_QUARTER]
        elif grain == Granularity.MONTH:
            return [PeriodType.END_OF_QUARTER]
        elif grain == Granularity.QUARTER:
            return [PeriodType.END_OF_YEAR]
        elif grain == Granularity.YEAR:
            return [PeriodType.END_OF_YEAR]
        else:
            raise ValueError(f"Invalid grain: {grain}")

    def _get_pacing_grain_for_period(self, period: PeriodType) -> Granularity:
        """
        Get the grain for the given period.

        Args:
            period: The period to get the grain for.

        Returns:
            Granularity: The grain for the given period.
        """
        if period == PeriodType.END_OF_WEEK:
            return Granularity.WEEK
        elif period == PeriodType.END_OF_MONTH:
            return Granularity.MONTH
        elif period == PeriodType.END_OF_QUARTER:
            return Granularity.QUARTER
        elif period == PeriodType.END_OF_YEAR:
            return Granularity.YEAR
        # TODO: "endOfNextMonth" is tricky for current period pacing; skip pacing for it or define specific logic,
        #  confirm with abhi
        else:
            raise ValueError(f"Invalid period: {period}")

    def _get_forecast_window(self, forecast_df: Any) -> ForecastWindow:
        """
        Get the forecast window.

        Args:
            forecast_df: DataFrame containing the forecast data.
            period_grain: Period grain.
        """
        return ForecastWindow(
            start_date=forecast_df["date"].min().strftime("%Y-%m-%d"),
            end_date=forecast_df["date"].max().strftime("%Y-%m-%d"),
            num_periods=len(forecast_df),
        )

    def _get_forecast_periods_count(self, analysis_date: pd.Timestamp) -> int:
        """
        Get the forecast periods.

        Check the current date and get the quarter end date and return the remaining periods count to be forecasted.

        Args:
            analysis_date: The analysis date.

        Returns:
            int: The forecast periods count.
        """
        quarter_end_date = get_period_end_date(analysis_date, PeriodType.END_OF_QUARTER)
        quarter_end_date = pd.to_datetime(quarter_end_date)
        remaining_periods = calculate_remaining_periods(analysis_date, quarter_end_date, Granularity.DAY)
        return remaining_periods

    def _handle_min_data(self, metric_id: str, analysis_window: AnalysisWindow, **kwargs) -> Forecasting:
        """
        Create a standardized output for minimum data required for analysis.

        Args:
            metric_id: The metric ID
            analysis_window: AnalysisWindow object

        Returns:
            Forecasting: Forecasting object with an error message
        """
        forecast_df = kwargs.get("forecast_data", pd.DataFrame())
        forecast_data = self._prepare_forecast_data(forecast_df)
        forecast_window = self._get_forecast_window(forecast_df)
        return Forecasting(
            pattern=self.name,
            version=self.version,
            metric_id=metric_id,
            analysis_window=analysis_window,
            num_periods=len(forecast_df),  # type: ignore
            forecast=forecast_data,
            forecast_window=forecast_window,
            forecast_vs_target_stats=[],
            pacing=[],
            required_performance=[],
            error=dict(
                message="Invalid target data for analysis",
                type="data_error",
            ),
        )

    def _get_target_value(self, target_data: pd.DataFrame, date: pd.Timestamp, grain: Granularity) -> float | None:
        """
        Get the target value for the given date and grain.

        Args:
            target_data: DataFrame containing the target data.
            date: The date to get the target value for.
            grain: The grain to get the target value for.

        Returns:
            float | None: The target value for the given date and grain.
        """
        target_value = None

        # Find target for the period start date (e.g., July 1st for July targets)
        target_row = target_data[
            (pd.to_datetime(target_data["date"]) == date)
            & (target_data["grain"] == grain)
            & (target_data["target_value"].notna())
            & (target_data["target_value"] != 0)
        ]
        if not target_row.empty:
            target_value = float(target_row["target_value"].iloc[0])
        return target_value
