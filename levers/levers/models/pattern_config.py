from datetime import date, timedelta
from enum import Enum
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field, model_validator

from levers.exceptions import InvalidPatternConfigError
from levers.models.common import Granularity

GRAIN_META: dict[str, Any] = {
    Granularity.DAY: {"delta": {"days": 1}},
    Granularity.WEEK: {"delta": {"weeks": 1}},
    Granularity.MONTH: {"delta": {"months": 1}},
    Granularity.QUARTER: {"delta": {"months": 3}},
    Granularity.YEAR: {"delta": {"years": 1}},
}


class DataSourceType(str, Enum):
    """Types of data sources that patterns can use."""

    METRIC_TIME_SERIES = "metric_time_series"
    METRIC_WITH_TARGETS = "metric_with_targets"
    DIMENSIONAL_TIME_SERIES = "dimensional_time_series"
    MULTI_METRIC = "multi_metric"


class WindowStrategy(str, Enum):
    """Strategies for determining the analysis window."""

    FIXED_TIME = "fixed_time"  # Same time window for all grains
    GRAIN_SPECIFIC_TIME = "grain_specific_time"  # Different time windows for different grains
    FIXED_DATAPOINTS = "fixed_datapoints"  # Fixed number of data points


class DataSource(BaseModel):
    """Configuration for a data source required by a pattern."""

    source_type: DataSourceType
    is_required: bool = True
    data_key: str = Field(..., description="Standardized name for accessing the data in the pattern")
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
    include_today: bool = Field(default=False)

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

    def get_date_range(self, grain: Granularity) -> tuple[date, date]:
        """
        Get the date range for analysis based on the configured strategy.

        Args:
            grain: The data granularity (day, week, month, quarter, year)

        Returns:
            Tuple of (start_date, end_date)
        """
        end_date = date.today() if self.include_today else date.today() - timedelta(days=1)

        if self.strategy == WindowStrategy.FIXED_TIME:
            # Same time window for all grains
            start_date = end_date - timedelta(days=self.days or 90)

        elif self.strategy == WindowStrategy.GRAIN_SPECIFIC_TIME:
            # Different time windows for different grains
            days = self.grain_days.get(grain) if self.grain_days and grain in self.grain_days else self.days
            start_date = end_date - timedelta(days=days or 180)

        elif self.strategy == WindowStrategy.FIXED_DATAPOINTS:
            datapoints = self.datapoints or 30
            # Fixed number of data points
            # Calculate start date by going back (datapoints-1) periods from the end date
            # We don't need available_dates since we can calculate based on grain and datapoints
            start_date = self.get_prev_period_start_date(
                grain=grain,  # type: ignore
                period_count=datapoints - 1,  # -1 because we include the end date
                latest_start_date=end_date,
            )
        else:
            # Default fallback
            start_date = end_date - timedelta(days=180)  # type: ignore

        # Enforce min/max constraints
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

    @model_validator(mode="after")
    def validate_nested_models(self):
        """Validate nested models with context."""
        if self.analysis_window:
            # Pass pattern_name when validating
            self.analysis_window.validate_strategy_params(pattern_name=self.pattern_name)
        return self

    class Config:
        schema_extra = {
            "example": {
                "pattern_name": "performance_status",
                "version": "1.0",
                "description": "Analyzes a metric's performance status against its target",
                "data_sources": [{"source_type": "metric_with_targets", "is_required": True, "data_key": "data"}],
                "analysis_window": {
                    "strategy": "fixed_time",
                    "days": 180,
                    "min_days": 30,
                    "max_days": 365,
                    "include_today": False,
                },
                "settings": {"threshold_ratio": 0.05},
            }
        }
