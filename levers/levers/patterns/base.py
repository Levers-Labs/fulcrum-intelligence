from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

import pandas as pd
from pydantic import ValidationError as PydanticValidationError

from levers.exceptions import (
    InvalidDataError,
    MissingDataError,
    PatternError,
    TimeRangeError,
    ValidationError as LeversValidationError,
)
from levers.models import (
    AnalysisWindow,
    AnalysisWindowConfig,
    BasePattern,
    DataSource,
    DataSourceType,
    Granularity,
    PatternConfig,
    WindowStrategy,
)

T = TypeVar("T", bound=BasePattern)


class Pattern(ABC, Generic[T]):
    """Base class for all analytics patterns."""

    # Class attributes to be defined by subclasses
    name: str = ""
    description: str = ""
    version: str = "1.0"
    required_primitives: list[str] = []
    output_model: type[T]  # Will be defined by subclasses

    # New attribute for pattern configuration
    config: PatternConfig | None = None

    def __init__(self, config: PatternConfig | None = None) -> None:
        """
        Initialize the pattern.

        Args:
            config: Optional pattern configuration. If not provided, default config will be used.
        """
        if not self.name:
            self.name = self.__class__.__name__
        if not self.description and self.__doc__:
            self.description = self.__doc__.strip().split("\n")[0]

        # Use provided config or default config
        self.config = config or self.get_default_config()

    @classmethod
    def get_default_config(cls) -> PatternConfig:
        """
        Get the default configuration for this pattern.
        Subclasses should override this method to provide a specific configuration.

        Returns:
            Default PatternConfig for this pattern
        """
        # Generic fallback configuration
        return PatternConfig(
            pattern_name=cls.name or cls.__name__,
            description=cls.description,
            version=cls.version,
            data_sources=[DataSource(source_type=DataSourceType.METRIC_TIME_SERIES, is_required=True, data_key="data")],
            analysis_window=AnalysisWindowConfig(
                strategy=WindowStrategy.FIXED_TIME, days=180, min_days=30, max_days=365, include_today=False
            ),
        )

    def get_data_requirements(self) -> list[str]:
        """
        Get the data requirements for this pattern.

        Returns:
            List of data keys required by the pattern
        """
        if self.config:
            return [ds.data_key for ds in self.config.data_sources if ds.is_required]
        return []

    @abstractmethod
    def analyze(self, metric_id: str, data: pd.DataFrame, analysis_window: AnalysisWindow, **kwargs) -> T:
        """
        Execute the analysis pattern and return a standardized output.

        Args:
            metric_id: The ID of the metric being analyzed
            data: DataFrame containing the metric data
            analysis_window: AnalysisWindow object specifying the analysis time window
            **kwargs: Additional pattern-specific parameters

        Returns:
            Structured output using the pattern's Pydantic model
        """
        pass

    def validate_output(self, output: dict[str, Any] | T) -> T:
        """
        Validate the pattern output against its output model.

        Args:
            output: Dictionary with output data or Pydantic model instance

        Returns:
            Validated Pydantic model instance

        Raises:
            PatternError: If no output model is defined
            LeversValidationError: If output validation fails
        """
        if not self.output_model:
            raise PatternError(
                "No output model defined for pattern",
                self.name,
                {"pattern_class": self.__class__.__name__},
            )

        try:
            if isinstance(output, self.output_model):
                return output
            return self.output_model.model_validate(output)
        except PydanticValidationError as e:
            raise LeversValidationError(
                f"Invalid output structure for {self.name}", {"validation_errors": e.errors()}
            ) from e

    @staticmethod
    def validate_time_window(df: pd.DataFrame, start_date: Any, end_date: Any, date_col: str = "date") -> pd.DataFrame:
        """
        Validate and filter data to the specified time window.

        Args:
            df: DataFrame with time series data
            start_date: Start date for analysis
            end_date: End date for analysis
            date_col: Name of date column

        Returns:
            Filtered DataFrame

        Raises:
            LeversValidationError: If date column is missing
            TimeRangeError: If no data in specified date range
        """
        if date_col not in df.columns:
            raise LeversValidationError(f"DataFrame must contain a '{date_col}' column", {"field": date_col})

        # Filter to date range
        filtered_df = df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]

        if filtered_df.empty:
            raise TimeRangeError("No data in specified date range", start_date=start_date, end_date=end_date)

        return filtered_df

    def validate_data(self, data: pd.DataFrame, required_columns: list[str]) -> bool:
        """
        Validate that the input DataFrame contains all required columns.

        Args:
            data: DataFrame to validate
            required_columns: List of column names that must be present

        Returns:
            True if all required columns are present

        Raises:
            MissingDataError: If any required column is missing
        """
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise MissingDataError(f"Missing required columns: {missing_columns}", missing_columns)
        return True

    def validate_analysis_window(self, analysis_window: AnalysisWindow) -> AnalysisWindow:
        """
        Validate the analysis window.

        Args:
            analysis_window: AnalysisWindow object

        Returns:
            Validated AnalysisWindow object

        Raises:
            InvalidDataError: If analysis window is invalid
        """
        # Validate that start_date is before end_date
        start_date = pd.to_datetime(analysis_window.start_date)
        end_date = pd.to_datetime(analysis_window.end_date)

        if start_date > end_date:
            raise InvalidDataError(
                "Start date must be before end date",
                {"start_date": analysis_window.start_date, "end_date": analysis_window.end_date},
            )

        return analysis_window

    def handle_empty_data(self, metric_id: str, analysis_window: AnalysisWindow) -> T:
        """
        Create a standardized output for empty or insufficient data.

        Args:
            metric_id: The metric ID
            analysis_window: AnalysisWindow object

        Returns:
            Empty output with an error message
        """
        result = {
            "pattern_name": self.name,
            "version": self.version,
            "metric_id": metric_id,
            "analysis_window": analysis_window,
            # todo: standardize type and schema in future
            "error": dict(
                message="Insufficient data for analysis",
                type="data_error",
            ),
        }
        return self.validate_output(result)

    @staticmethod
    def extract_date_range(data: pd.DataFrame, date_col: str = "date") -> dict[str, str]:
        """
        Extract the minimum and maximum dates from the data.

        Args:
            data: DataFrame containing the data
            date_col: Column name containing dates

        Returns:
            Dictionary with 'start_date' and 'end_date' in 'YYYY-MM-DD' format

        Raises:
            MissingDataError: If date column is missing
        """
        if date_col not in data.columns:
            raise MissingDataError(f"Date column '{date_col}' not found in data", [date_col])

        dates = pd.to_datetime(data[date_col])
        return {"start_date": dates.min().strftime("%Y-%m-%d"), "end_date": dates.max().strftime("%Y-%m-%d")}

    @classmethod
    def create_analysis_window(
        cls, data: pd.DataFrame, grain: Granularity = Granularity.DAY, date_col: str = "date"
    ) -> AnalysisWindow:
        """
        Create an AnalysisWindow from a DataFrame.

        Args:
            data: DataFrame containing the data
            grain: Time grain for analysis
            date_col: Column name containing dates

        Returns:
            AnalysisWindow object
        """
        date_range = cls.extract_date_range(data, date_col)
        return AnalysisWindow(start_date=date_range["start_date"], end_date=date_range["end_date"], grain=grain)

    def preprocess_data(
        self, data: pd.DataFrame, analysis_window: AnalysisWindow, date_col: str = "date"
    ) -> pd.DataFrame:
        """
        Preprocess data by validating, converting dates, and filtering to the analysis window.

        Args:
            data: DataFrame containing the data
            analysis_window: AnalysisWindow object
            date_col: Column name containing dates

        Returns:
            Processed DataFrame
        """
        # Validate analysis window
        validated_window = self.validate_analysis_window(analysis_window)

        # Process data
        processed_data = data.copy()

        # Convert date column to datetime
        processed_data[date_col] = pd.to_datetime(processed_data[date_col])

        # Sort by date
        processed_data.sort_values(date_col, inplace=True)

        # Filter to analysis window
        start_date = pd.to_datetime(validated_window.start_date)
        end_date = pd.to_datetime(validated_window.end_date)

        filtered_data = self.validate_time_window(processed_data, start_date, end_date, date_col)

        return filtered_data

    @classmethod
    def get_info(cls) -> dict[str, Any]:
        """
        Get pattern information.

        Returns:
            Dictionary with pattern metadata including full output schema
        """
        # Get basic pattern info
        info: dict[str, Any] = {
            "name": cls.name,
            "description": cls.__doc__.strip().split("\n")[0] if cls.__doc__ else "",
            "required_primitives": cls.required_primitives,
        }

        # Add output schema if available
        if hasattr(cls, "output_model"):
            try:
                # Get the schema from the Pydantic model
                schema = cls.output_model.model_json_schema()
                info["output"] = schema
            except Exception as e:
                # Fallback if schema generation fails
                info["output"] = {"error": f"Could not generate schema: {str(e)}"}
        else:
            info["output"] = None

        return info
