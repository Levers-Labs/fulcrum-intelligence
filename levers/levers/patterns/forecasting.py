"""
Forecasting Pattern

This module implements the Forecasting pattern which generates forecasts for a given metric,
including projections for specific future period end-dates and a detailed daily forecast.
"""

import logging
from datetime import date, datetime

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
    calculate_pop_growth,
    calculate_remaining_periods,
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
        "calculate_remaining_periods",
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
                    source_type=DataSourceType.METRIC_TIME_SERIES, is_required=True, data_key="data", lookahead=False
                ),
                DataSource(
                    source_type=DataSourceType.METRIC_WITH_TARGETS, is_required=True, data_key="target", lookahead=True
                ),
            ],
            analysis_window=AnalysisWindowConfig(
                strategy=WindowStrategy.FIXED_TIME, days=365, min_days=90, max_days=730, include_today=False
            ),
            settings={
                "confidence_level": 0.95,
                "pacing_status_threshold_pct": 5.0,
                "num_past_periods_for_growth": 4,
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
        num_past_periods_for_growth: int = 4,
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
            num_past_periods_for_growth: Number of past periods for historical growth calculation

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

            # Validate input data
            required_columns = ["date", "value"]
            self.validate_data(data, required_columns)

            # Process data
            df = self.preprocess_data(data, analysis_window)

            # Handle empty data
            if df.empty:
                return self.handle_empty_data(metric_id, analysis_window)

            # Process single forecast period
            period = self._get_period_for_grain(grain)
            # Get the grain for the period
            period_grain = self._get_grain_for_period(period)

            forecast_df = forecast_with_confidence_intervals(
                df=df,
                value_col="value",
                confidence_level=confidence_level,
                date_col="date",
                grain=grain,
                method=ForecastMethod.PROPHET,
            )
            if forecast_df.empty:
                return self.handle_empty_data(metric_id, analysis_window)

            # Get latest actual value for required growth calculations
            latest_actual_value = float(df["value"].iloc[-1])
            period_target_date = get_period_end_date(analysis_dt, period)
            # Get target value if available
            target_value = None

            # Check if data has target information and it's not all NaN/zero
            if not target.empty and "target_value" in target.columns and "date" in target.columns:
                # First try to find target for the specific period target date
                target_row = target[
                    (pd.to_datetime(target["date"]) == period_target_date)
                    & (target["target_value"].notna())
                    & (target["target_value"] != 0)
                ]

                if not target_row.empty:
                    target_value = float(target_row["target_value"].iloc[0])

            # Forecast vs Target Stats - only calculate if we have targets
            forecast_vs_target_stats = (
                None
                if target_value is None
                else self._get_forecast_vs_target_stats(forecast_df, period_target_date, target_value, confidence_level)
            )

            # Pacing Projection - only calculate if we have targets
            pacing = (
                None
                if target_value is None
                else self._calculate_pacing_projection(
                    df, analysis_dt, period_grain, target_value, pacing_status_threshold_pct
                )
            )

            # Required Performance - only calculate if we have targets
            required_performance = (
                None
                if target_value is None
                else self._calculate_required_performance(
                    df=df,
                    analysis_dt=analysis_dt,
                    grain=grain,
                    latest_actual_value=latest_actual_value,
                    target_date=period_target_date,
                    target_value=target_value,
                    num_past_periods_for_growth=num_past_periods_for_growth,
                )
            )

            period_forecast = self._prepare_period_forecast_data(forecast_df, confidence_level)

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
                forecast_period_grain=period_grain,
                num_periods=len(df),
                forecast_vs_target_stats=forecast_vs_target_stats,
                pacing=pacing,
                required_performance=required_performance,
                period_forecast=period_forecast,
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
        target_date: pd.Timestamp,
        target_value: float | None,
        confidence_level: float,
    ) -> ForecastVsTargetStats:
        """
        Get the forecast vs target stats.

        This method calculates the forecast vs target stats for a given forecast dataframe and target date.
        This is a simple comparison of the forecasted value vs the target value.

        Args:
            forecast_df: DataFrame containing forecasted values and confidence intervals.
            target_date: Target date for the forecast.
            confidence_level: Confidence level for the forecast.
            target_value: Target value.

        Returns:
            ForecastVsTargetStats: Forecast vs target stats.
            The forecast vs target stats include:
                - Forecasted value
                - Confidence level
                - Target date
                - Target value
                - gap percent
                - status
        """
        # Initialize sections
        stats = ForecastVsTargetStats()

        # Statistical Forecast Section
        target_date_str = target_date.strftime("%Y-%m-%d")

        # Create a copy to avoid modifying the original DataFrame
        forecast_df_copy = forecast_df.copy()

        # Convert date column to string format for comparison
        forecast_df_copy["date_str"] = pd.to_datetime(forecast_df_copy["date"]).dt.strftime("%Y-%m-%d")

        # Find the row matching the target date
        target_rows = forecast_df_copy[forecast_df_copy["date_str"] == target_date_str]

        if not target_rows.empty:
            fc_row = target_rows.iloc[0]
            stats.forecasted_value = round(fc_row.get("forecast", 0), 2) if pd.notna(fc_row.get("forecast")) else None

        stats.target_date = target_date_str
        stats.target_value = target_value

        # Calculate gap and status only if we have both forecast and target
        if stats.forecasted_value is not None and target_value is not None and target_value != 0:
            gap_pct = (target_value - stats.forecasted_value) / target_value * 100
            stats.gap_percent = abs(round(gap_pct, 2))

            stats.status = classify_metric_status(stats.forecasted_value, target_value)

        return stats

    def _calculate_pacing_projection(
        self,
        df: pd.DataFrame,
        analysis_dt: pd.Timestamp,
        period_grain: Granularity,
        target_value: float | None,
        pacing_status_threshold_pct: float,
    ) -> PacingProjection:
        """
        Calculate pacing projection for the period. This method calculates the pacing projection for the period based
        on the actual values in the period. Here we are calculating the projected value for the period based on the
        actual values in the period. The projected value is calculated by taking the cumulative value of the actual
        values in the period and dividing it by the percent of the period elapsed. The gap percent is calculated by
        taking the projected value and dividing it by the target value and subtracting 1. The status is calculated by
        taking the gap percent and comparing it to the pacing status threshold percentage.

        Args:
            df: DataFrame containing the data.
            analysis_dt: Analysis date.
            period_grain: Period grain.
            target_value: Target value.
            pacing_status_threshold_pct: Pacing status threshold percentage.

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
        result = PacingProjection(target_value=target_value)

        # Get the start and end dates for the pacing period
        # Use include_today=True to get the current active period (not the previous completed period)
        pacing_period_start, pacing_period_end = get_period_range_for_grain(
            analysis_dt, period_grain, include_today=True
        )

        # Check if analysis date is within the pacing period
        if not analysis_dt >= pacing_period_start:
            return result
        elapsed_days = (analysis_dt - pacing_period_start).days + 1
        total_days = (pacing_period_end - pacing_period_start).days + 1

        # Percent of period elapsed
        result.period_elapsed_percent = round((elapsed_days / total_days) * 100.0, 2) if total_days > 0 else 0.0

        # Cumulative value from df
        pacing_period_actuals = df[
            (pd.to_datetime(df["date"]) >= pacing_period_start) & (pd.to_datetime(df["date"]) <= analysis_dt)
        ]
        pacing_period_cumulative_value = pacing_period_actuals["value"].sum()
        result.cumulative_value = round(pacing_period_cumulative_value, 2)

        # Projected value
        if 0 < result.period_elapsed_percent < 100:
            result.projected_value = round((pacing_period_cumulative_value / result.period_elapsed_percent) * 100.0, 2)
        elif result.period_elapsed_percent >= 100:
            result.projected_value = round(pacing_period_cumulative_value, 2)

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
                - latest_actual_value: Latest actual value.
                - target_date: Target date.
                - target_value: Target value.
                - num_past_periods_for_growth: Number of past periods for growth calculation.

        Returns:
            RequiredPerformance: Required performance.
            The required performance includes:
                - Remaining periods count
                - Required pop growth percent
                - Previous pop growth percent
                - Growth difference
        """

        # Get the forecast target date
        target_date = kwargs.get("target_date")
        # Get the current target value
        target_value = kwargs.get("target_value", 0)
        # Get the number of past periods for growth calculation
        num_past_periods_for_growth = kwargs.get("num_past_periods_for_growth", 4)
        # Get the latest actual value
        latest_actual_value = kwargs.get("latest_actual_value", 0)

        # For forecasting, calculate remaining periods based on target date
        remaining_periods_count = calculate_remaining_periods(analysis_dt, target_date, grain)  # type: ignore

        required_performance = RequiredPerformance(remaining_periods=remaining_periods_count)

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
        if len(df) >= num_past_periods_for_growth + 1:
            past_df = df.tail(num_past_periods_for_growth + 1).copy()
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
        required_performance.num_periods = len(df)
        required_performance.previous_num_periods = num_past_periods_for_growth

        return required_performance

    def _prepare_period_forecast_data(self, forecast_df: pd.DataFrame, confidence_level: float) -> list[Forecast]:
        """
            Prepare period forecast data.
            This method prepares the period forecast data for the period.
            The period forecast data is a list of Forecast objects.

        Args:
            forecast_df: DataFrame containing the forecast data.
            confidence_level: Confidence level for the forecast.

        Returns:
            list[Forecast]: List of Forecast objects.
            The Forecast object includes:
                - Date
                - Forecasted value
                - Lower bound
                - Upper bound
                - Confidence level
        """
        period_forecast = []
        for _, row in forecast_df.iterrows():
            period_forecast.append(
                Forecast(
                    date=pd.to_datetime(row["date"]).strftime("%Y-%m-%d"),  # Use the date column instead of index
                    forecasted_value=round(row["forecast"], 2) if pd.notna(row["forecast"]) else None,
                    lower_bound=round(row["lower_bound"], 2) if pd.notna(row["lower_bound"]) else None,
                    upper_bound=round(row["upper_bound"], 2) if pd.notna(row["upper_bound"]) else None,
                )
            )
        return period_forecast

    def _get_period_for_grain(self, grain: Granularity) -> PeriodType:
        """
        Get the period for the given grain.

        Args:
            grain: The grain to get the period for.

        Returns:
            PeriodType: The period for the given grain.
        """
        if grain == Granularity.DAY:
            return PeriodType.END_OF_WEEK
        elif grain == Granularity.WEEK:
            return PeriodType.END_OF_MONTH
        elif grain == Granularity.MONTH:
            return PeriodType.END_OF_QUARTER
        elif grain == Granularity.QUARTER:
            return PeriodType.END_OF_YEAR
        elif grain == Granularity.YEAR:
            return PeriodType.END_OF_YEAR
        else:
            raise ValueError(f"Invalid grain: {grain}")

    def _get_grain_for_period(self, period: PeriodType) -> Granularity:
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

    def _extract_period_targets(
        self, targets_data: pd.DataFrame, analysis_window: AnalysisWindow, analysis_date: date | None = None
    ) -> pd.DataFrame:
        """
        Extract targets for specific period end dates from the combined targets_data.

        This method looks for targets that fall exactly on period boundaries (end of week, month, quarter, etc.)
        and returns them in a format expected by the rest of the forecasting logic.

        Args:
            targets_data: Combined DataFrame with date, value, and target columns
            analysis_window: Analysis window containing grain information
            analysis_date: Date from which to calculate period end dates

        Returns:
            DataFrame with columns: date, target_value for exact period end dates only
        """
        if analysis_date is None:
            analysis_date = date.today()

        analysis_dt = pd.to_datetime(analysis_date)

        # Define the period types we want to extract targets for
        period_types = [
            PeriodType.END_OF_WEEK,
            PeriodType.END_OF_MONTH,
            PeriodType.END_OF_QUARTER,
            # Could add PeriodType.END_OF_NEXT_MONTH if needed
        ]

        targets_list = []

        for period_type in period_types:
            try:
                # Get the exact period end date
                period_end_date = get_period_end_date(analysis_dt, period_type)

                # Find exact match for this period end date in targets_data
                target_rows = targets_data[
                    (pd.to_datetime(targets_data["date"]) == period_end_date)
                    & (targets_data.get("target", pd.Series()).notna())
                    & (targets_data.get("target", pd.Series()) != 0)
                ]

                if not target_rows.empty:
                    targets_list.append({"date": period_end_date, "target_value": float(target_rows["target"].iloc[0])})
                    logger.debug(
                        "Found target for %s: %s = %s",
                        period_type.value,
                        period_end_date,
                        target_rows["target"].iloc[0],
                    )
                else:
                    logger.debug("No target found for %s: %s", period_type.value, period_end_date)

            except Exception as e:
                logger.warning("Error extracting target for period %s: %s", period_type.value, str(e))
                continue

        # Return DataFrame in expected format
        if targets_list:
            result_df = pd.DataFrame(targets_list)
            logger.info("Extracted %d period-specific targets", len(result_df))
            return result_df
        else:
            logger.warning("No period-specific targets found in targets_data")
            return pd.DataFrame(columns=["date", "target_value"])

    def _get_forecast_window(self, forecast_df: pd.DataFrame) -> ForecastWindow:
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
