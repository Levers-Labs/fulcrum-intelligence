from datetime import date, timedelta
from typing import Any

import pandas as pd
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)

from levers.exceptions import InvalidPatternConfigError
from levers.models import DataSourceType, Granularity, WindowStrategy

GRAIN_META: dict[str, Any] = {
    Granularity.DAY: {"delta": {"days": 1}},
    Granularity.WEEK: {"delta": {"weeks": 1}},
    Granularity.MONTH: {"delta": {"months": 1}},
    Granularity.QUARTER: {"delta": {"months": 3}},
    Granularity.YEAR: {"delta": {"years": 1}},
}


class DataSource(BaseModel):
    """Configuration for a data source required by a pattern."""

    source_type: DataSourceType
    is_required: bool = True
    data_key: str = Field(..., description="Standardized name for accessing the data in the pattern")
    look_forward: bool = Field(
        default=False,
        description="Whether this data source needs future period end dates " "instead of historical range",
    )
    meta: dict[str, Any] = Field(default_factory=dict)


class AnalysisWindowConfig(BaseModel):
    """
    Unified configuration for pattern analysis windows supporting multiple strategies.
    """

    strategy: WindowStrategy

    # For FIXED_TIME strategy
    days: int | None = Field(default=None, ge=1)

    # For GRAIN_SPECIFIC_TIME strategy
    grain_days: dict[str, int] | None = Field(default=None)

    # For FIXED_DATAPOINTS strategy
    datapoints: int | None = Field(default=None, ge=1, le=1000)

    # Common configuration
    min_days: int = Field(default=7, ge=1)
    max_days: int = Field(default=730)

    def validate_strategy_params(self, pattern_name=None):
        strategy = self.strategy
        days = self.days
        grain_days = self.grain_days
        datapoints = self.datapoints

        if strategy == WindowStrategy.FIXED_TIME and not days:
            raise InvalidPatternConfigError(
                "days parameter is required for FIXED_TIME strategy",
                pattern_name=pattern_name,
                config=self,
                invalid_fields={"days": "days is required for FIXED_TIME strategy"},
            )

        if strategy == WindowStrategy.GRAIN_SPECIFIC_TIME and not grain_days:
            raise InvalidPatternConfigError(
                "grain_days parameter is required for GRAIN_SPECIFIC_TIME strategy",
                pattern_name=pattern_name,
                config=self,
                invalid_fields={"grain_days": "grain_days is required for GRAIN_SPECIFIC_TIME strategy"},
            )

        if strategy == WindowStrategy.FIXED_DATAPOINTS and not datapoints:
            raise InvalidPatternConfigError(
                "datapoints parameter is required for FIXED_DATAPOINTS strategy",
                pattern_name=pattern_name,
                config=self,
                invalid_fields={"datapoints": "datapoints is required for FIXED_DATAPOINTS strategy"},
            )

        return self

    @staticmethod
    def get_future_period_end_date(grain: Granularity, period_count: int, start_date: date) -> date:
        """
        Calculate the end date of a period that is a specified number of periods after the start date.

        This method determines the end date by going forward a specified number of periods from the
        start date, based on the given granularity.

        :param grain: The granularity of the period (e.g., day, week, month, etc.).
        :param period_count: The number of periods to go forward from the start date.
        :param start_date: The start date to calculate forward from.
        :return: The end date that is `period_count` periods after the `start_date`.
        """
        # Retrieve the delta for the specified grain from the GRAIN_META dictionary.
        delta_eq = GRAIN_META[grain]["delta"]

        # Convert the delta dictionary into a pandas DateOffset object.
        delta = pd.DateOffset(**delta_eq)

        # Calculate the end date by going forward period_count periods from the start_date.
        end_date = (start_date + period_count * delta).date()

        return end_date

    @staticmethod
    def get_prev_period_start_date(grain: Granularity, period_count: int, latest_start_date: date) -> date:
        """
        Calculate the start date of a period that is a specified number of periods before the latest start date.

        This method determines the start date of a period that is a specified number of periods before the
        latest start date, based on the given granularity. It uses the granularity metadata to determine the delta
        (e.g., weeks: 1) and then applies this delta to the latest start date to calculate the start date of the
        previous period.

        :param grain: The granularity of the period (e.g., day, week, month, etc.).
        :param period_count: The number of periods to go back from the latest start date.
        :param latest_start_date: The start date of the latest period.
        :return: The start date of the period that is `period_count` periods before the `latest_start_date`.
        """
        # Retrieve the delta for the specified grain from the GRAIN_META dictionary.
        delta_eq = GRAIN_META[grain]["delta"]

        # Convert the delta dictionary into a pandas DateOffset object.
        delta = pd.DateOffset(**delta_eq)

        # Calculate the start date of the period that is `period_count` periods before the `latest_start_date`.
        start_date = (latest_start_date - period_count * delta).date()

        return start_date

    def get_date_range(
        self, grain: Granularity, analysis_date: date | None = None, look_forward: bool = False
    ) -> tuple[date, date]:
        """
        Get the date range for analysis based on the configured strategy.

        Args:
            grain: The data granularity (day, week, month, quarter, year)
            analysis_date: The date to use for analysis
            look_forward: If True, returns future dates starting from analysis date

        Returns:
            Tuple of (start_date, end_date)
        """

        analysis_date = analysis_date or date.today()

        if look_forward:
            # For forward-looking data, we need to include current period start date and future periods
            # This ensures we capture targets for the current period
            start_date = date(analysis_date.year, analysis_date.month, 1)

            # Calculate the start of the current period based on grain
            if grain == Granularity.DAY:
                start_date = start_date
            elif grain == Granularity.WEEK:
                # Start of current week (Monday)
                start_date = analysis_date - timedelta(days=analysis_date.weekday())
            elif grain == Granularity.MONTH:
                # Start of current month
                start_date = date(analysis_date.year, analysis_date.month, 1)
            elif grain == Granularity.QUARTER:
                # Start of current quarter
                quarter_start_month = ((analysis_date.month - 1) // 3) * 3 + 1
                start_date = date(analysis_date.year, quarter_start_month, 1)
            elif grain == Granularity.YEAR:
                # Start of current year
                start_date = date(analysis_date.year, 1, 1)

            if self.strategy == WindowStrategy.FIXED_TIME:
                # Same time window for all grains
                end_date = start_date + timedelta(days=self.days or 90)
            elif self.strategy == WindowStrategy.GRAIN_SPECIFIC_TIME:
                # Different time windows for different grains
                days = self.grain_days.get(grain) if self.grain_days and grain in self.grain_days else self.days
                end_date = start_date + timedelta(days=days or 180)
            elif self.strategy == WindowStrategy.FIXED_DATAPOINTS:
                # For fixed datapoints, calculate forward periods from today
                datapoints = self.datapoints or 30
                # Calculate end date by going forward datapoints periods from analysis_date
                end_date = self.get_future_period_end_date(grain, datapoints, analysis_date)
        else:
            # end date will be the previous period end date
            if grain == Granularity.DAY:
                end_date = analysis_date - timedelta(days=1)  # Yesterday
            elif grain == Granularity.WEEK:
                end_date = analysis_date - timedelta(days=analysis_date.weekday() + 1)  # Sunday
            elif grain == Granularity.MONTH:
                end_date = date(analysis_date.year, analysis_date.month, 1) - timedelta(
                    days=1
                )  # Last day of previous month

            if self.strategy == WindowStrategy.FIXED_TIME:
                # Same time window for all grains
                start_date = end_date - timedelta(days=self.days or 90)
            elif self.strategy == WindowStrategy.GRAIN_SPECIFIC_TIME:
                # Different time windows for different grains
                days = self.grain_days.get(grain) if self.grain_days and grain in self.grain_days else self.days
                start_date = end_date - timedelta(days=days or 180)
            elif self.strategy == WindowStrategy.FIXED_DATAPOINTS:
                datapoints = self.datapoints or 30
                # Calculate start date by going back datapoints periods from the end date
                start_date = self.get_prev_period_start_date(
                    grain=grain,  # type: ignore
                    period_count=datapoints,
                    latest_start_date=end_date,
                )
            else:
                # Default fallback
                start_date = end_date - timedelta(days=180)  # type: ignore

            # Enforce min/max constraints for historical data
            min_start_date = end_date - timedelta(days=self.max_days)
            max_start_date = end_date - timedelta(days=self.min_days)

            if start_date < min_start_date:
                start_date = min_start_date

            if start_date > max_start_date:
                start_date = max_start_date

        return start_date, end_date


class PatternConfig(BaseModel):
    """Configuration for a pattern."""

    pattern_name: str
    version: str = "1.0"
    description: str | None = None

    # Data sources required for analysis
    data_sources: list[DataSource]

    # Analysis window configuration
    analysis_window: AnalysisWindowConfig

    # Pattern-specific settings
    settings: dict[str, Any] = Field(default_factory=dict)

    # Additional metadata
    meta: dict[str, Any] = Field(default_factory=dict)

    # Indicates if pattern needs dimension-based analysis
    needs_dimension_analysis: bool = Field(
        default=False, description="Whether this pattern should be run for each dimension"
    )

    @model_validator(mode="after")
    def validate_nested_models(self):
        """Validate nested models with context."""
        if self.analysis_window:
            # Pass pattern_name when validating
            self.analysis_window.validate_strategy_params(pattern_name=self.pattern_name)
        return self

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "pattern_name": "performance_status",
                "version": "1.0",
                "description": "Analyzes a metric's performance status against its target",
                "data_sources": [
                    {
                        "source_type": "metric_with_targets",
                        "is_required": True,
                        "data_key": "data",
                        "look_forward": False,
                    }
                ],
                "analysis_window": {"strategy": "fixed_time", "days": 180, "min_days": 30, "max_days": 365},
                "settings": {"threshold_ratio": 0.05},
                "needs_dimension_analysis": False,
            }
        }
    )
