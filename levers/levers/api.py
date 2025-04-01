"""
Main API for the Levers package.
"""

from typing import Any, Generic, TypeVar

import pandas as pd

from levers.exceptions import LeversError, PatternError, PrimitiveError
from levers.models.common import AnalysisWindow, BasePattern, Granularity
from levers.models.patterns import MetricPerformance
from levers.patterns.base import Pattern
from levers.primitives import get_primitive_metadata, list_primitives_by_family
from levers.registry import PatternRegistry, autodiscover_patterns

T = TypeVar("T", bound=BasePattern)


class Levers(Generic[T]):
    """Main API class for accessing analytics primitives and patterns."""

    # Map of pattern names to their respective pattern classes
    _pattern_model_registry: dict[str, type[BasePattern]] = {
        "performance_status": MetricPerformance,
        # Add other patterns here as they are implemented
    }

    def __init__(self) -> None:
        """Initialize the Levers API."""
        # Auto-discover and register patterns
        autodiscover_patterns()

        # Initialize pattern registry
        self._pattern_registry = PatternRegistry[T]()

    @property
    def patterns(self) -> dict[str, type[Pattern[T]]]:
        """Get all registered patterns."""
        return self._pattern_registry._patterns

    @classmethod
    def get_pattern_model_class(cls, pattern_name: str) -> type[BasePattern]:
        """
        Get a pattern model class by name.

        Args:
            pattern_name: Name of the pattern model

        Returns:
            Pattern model class

        Raises:
            PatternError: If pattern model not found
        """
        pattern_class = cls._pattern_model_registry.get(pattern_name)
        if not pattern_class:
            raise PatternError(f"Unknown pattern type: {pattern_name}", pattern_name)
        return pattern_class

    @classmethod
    def load_pattern_model(cls, pattern_data: dict[str, Any]) -> BasePattern:
        """
        Dynamically load a pattern model based on the 'pattern' field in the input dictionary.

        This method examines the 'pattern' field in the input dictionary and creates an instance
        of the appropriate pattern model class based on that value. For example, if pattern='performance_status',
        it will create a MetricPerformance object.

        Usage example:
        ```python
        # Load pattern data from a dictionary
        pattern_run = {"pattern": "performance_status", "metric_id": "123", ...}
        pattern_model = Levers.load_pattern_model(pattern_run)

        # Now pattern_model is an instance of MetricPerformance
        ```

        Args:
            pattern_data: Dictionary containing pattern data with a 'pattern' key

        Returns:
            Appropriate pattern model instance (e.g., MetricPerformance for pattern='performance_status')

        Raises:
            PatternError: If pattern type is not found or validation fails
        """
        pattern_type = pattern_data.get("pattern")
        if not pattern_type:
            raise PatternError("No pattern type specified in data", "unknown")

        pattern_class = cls.get_pattern_model_class(pattern_type)

        try:
            return pattern_class(**pattern_data)
        except Exception as e:
            raise PatternError(
                f"Failed to load pattern data: {str(e)}", pattern_type, {"validation_error": str(e)}
            ) from e

    def get_pattern(self, pattern_name: str) -> type[Pattern[T]]:
        """
        Get a specific pattern by name.

        Args:
            pattern_name: Name of the pattern to retrieve

        Returns:
            Pattern class

        Raises:
            PatternError: If pattern not found
        """
        pattern = self._pattern_registry.get(pattern_name)
        if not pattern:
            raise PatternError("Pattern not found", pattern_name)
        return pattern

    def list_patterns(self) -> list[str]:
        """
        List all available pattern names.

        Returns:
            List of pattern names
        """
        return self._pattern_registry.list_all()

    def list_primitives(self) -> list[str]:
        """
        List all available primitive names.

        Returns:
            List of primitive names
        """
        # Get all primitives from all families
        all_primitives = []
        for primitives in list_primitives_by_family().values():
            all_primitives.extend(primitives)
        return all_primitives

    def get_primitive(self, primitive_name: str) -> Any:
        """
        Get a specific primitive by name.

        Args:
            primitive_name: Name of the primitive to retrieve

        Returns:
            Primitive function

        Raises:
            PrimitiveError: If primitive not found
        """
        try:
            return get_primitive_metadata(primitive_name)
        except ValueError as e:
            raise PrimitiveError("Primitive not found", primitive_name) from e

    def get_pattern_info(self, pattern_name: str) -> dict[str, Any]:
        """
        Get detailed information about a pattern.

        Args:
            pattern_name: Name of the pattern

        Returns:
            Dictionary with pattern metadata
        """
        pattern = self.get_pattern(pattern_name)
        return pattern.get_info()

    def get_primitive_info(self, primitive_name: str) -> dict[str, Any]:
        """
        Get detailed information about a primitive.

        Args:
            primitive_name: Name of the primitive

        Returns:
            Dictionary with primitive metadata
        """
        return get_primitive_metadata(primitive_name)

    def list_primitives_by_family(self) -> dict[str, list[str]]:
        """
        List all primitives organized by family.

        Returns:
            Dictionary with family names as keys and lists of primitive names as values
        """
        return list_primitives_by_family()

    def execute_pattern(
        self, pattern_name: str, metric_id: str, data: pd.DataFrame, analysis_window: AnalysisWindow, **kwargs
    ) -> Any:
        """
        Execute an analysis pattern.

        Args:
            pattern_name: Name of the pattern to execute
            metric_id: ID of the metric being analyzed
            data: DataFrame containing the metric data
            analysis_window: AnalysisWindow object specifying the analysis time window
            **kwargs: Additional pattern-specific parameters

        Returns:
            Analysis results as a Pydantic model

        Raises:
            PatternError: If pattern execution fails
        """
        try:
            pattern_class = self.get_pattern(pattern_name)
            pattern = pattern_class()
            result = pattern.analyze(metric_id=metric_id, data=data, analysis_window=analysis_window, **kwargs)

            # Return the model directly
            return result
        except Exception as e:
            if isinstance(e, LeversError):
                raise PatternError(
                    f"Error executing pattern: {str(e)}", pattern_name, {"original_error": type(e), **e.details}
                ) from e
            raise PatternError(f"Error executing pattern: {str(e)}", pattern_name, {"original_error": type(e)}) from e

    # Convenience methods for common patterns
    def analyze_performance_status(
        self,
        metric_id: str,
        data: pd.DataFrame,
        start_date: str,
        end_date: str,
        grain: Granularity = Granularity.DAY,
        threshold_ratio: float = 0.05,
    ) -> MetricPerformance:
        """
        Analyze performance status (on track/off track).

        Args:
            metric_id: ID of the metric
            data: DataFrame with time series data
            start_date: Analysis start date (YYYY-MM-DD)
            end_date: Analysis end date (YYYY-MM-DD)
            grain: Time grain for analysis
            threshold_ratio: Tolerance ratio for status classification

        Returns:
            Performance status analysis
        """
        # Create an analysis window
        analysis_window = AnalysisWindow(start_date=start_date, end_date=end_date, grain=grain)

        # Execute the pattern
        return self.execute_pattern(
            pattern_name="performance_status",
            metric_id=metric_id,
            data=data,
            analysis_window=analysis_window,
            threshold_ratio=threshold_ratio,
        )
