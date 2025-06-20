"""
Base class for mock pattern generators.

This provides common functionality for generating mock pattern results
that can be evaluated by story evaluators.
"""

import random
from abc import ABC, abstractmethod
from datetime import date
from typing import Any

from commons.models.enums import Granularity


class MockPatternGeneratorBase(ABC):
    """
    Base class for mock pattern generators.

    Each pattern generator creates realistic mock pattern results that simulate
    what the actual pattern analysis would produce.
    """

    pattern_name: str

    @abstractmethod
    def generate_pattern_results(self, metric: dict[str, Any], grain: Granularity, story_date: date) -> list[Any]:
        """
        Generate mock pattern results for the given parameters.

        Args:
            metric: Metric dictionary containing metric details
            grain: Time granularity for analysis
            story_date: Date for the analysis

        Returns:
            List of pattern result objects
        """
        pass

    def _get_base_pattern_data(self, metric: dict[str, Any], grain: Granularity, story_date: date) -> dict[str, Any]:
        """
        Get base data that's common to all pattern results.

        Args:
            metric: Metric dictionary
            grain: Time granularity
            story_date: Analysis date

        Returns:
            Dictionary with base pattern data
        """
        from datetime import timedelta

        # Calculate window start date (30 days before story date)
        start_date = story_date - timedelta(days=30)

        return {
            "pattern": self.pattern_name,
            "metric_id": metric["metric_id"],
            "analysis_window": {
                "start_date": start_date.isoformat(),
                "end_date": story_date.isoformat(),
                "grain": grain.value,
            },
            "analysis_date": story_date,
            "num_periods": 30,  # Reasonable default for mock data
        }

    def _generate_mock_value(self, base_value: float = 1000, variation: float = 0.3) -> float:
        """
        Generate a mock metric value with some randomization.

        Args:
            base_value: Base value to vary from
            variation: Percentage variation (0.3 = 30% variation)

        Returns:
            Randomized value
        """
        min_val = base_value * (1 - variation)
        max_val = base_value * (1 + variation)
        return round(random.uniform(min_val, max_val), 2)  # noqa

    def _generate_percentage(self, min_percent: float = -50, max_percent: float = 50) -> float:
        """
        Generate a random percentage value.

        Args:
            min_percent: Minimum percentage
            max_percent: Maximum percentage

        Returns:
            Random percentage value
        """
        return round(random.uniform(min_percent, max_percent), 2)  # noqa

    def _choose_random_target_status(self) -> str:
        """Choose a random target status."""
        return random.choice(["on_track", "off_track"])  # noqa

    def _generate_mock_dimensions(self) -> list[str]:
        """Generate mock dimension names."""
        dimensions = [
            "Region",
            "Channel",
            "Product_Type",
            "Customer_Segment",
            "Device_Type",
            "Campaign_Type",
            "Sales_Rep",
            "Geography",
        ]
        return random.sample(dimensions, k=random.randint(1, 3))  # noqa

    def _generate_mock_dimension_values(self, dimension: str) -> list[str]:
        """Generate mock values for a dimension."""
        dimension_values = {
            "Region": [
                "US-West",
                "US-East",
                "US-Central",
                "EMEA",
                "APAC",
                "LATAM",
                "ASIA",
                "Africa",
                "Middle East",
                "Canada",
                "Australia",
                "New Zealand",
            ],
            "Channel": ["Direct", "Partner", "Online", "Retail", "Mobile"],
            "Product_Type": ["Basic", "Premium", "Enterprise", "Starter"],
            "Customer_Segment": ["Small Business", "Mid-Market", "Enterprise", "Consumer"],
            "Device_Type": ["Desktop", "Mobile", "Tablet"],
            "Campaign_Type": ["Email", "Social", "Search", "Display"],
            "Sales_Rep": ["John Smith", "Jane Doe", "Mike Johnson", "Sarah Wilson"],
            "Geography": ["North", "South", "East", "West", "Central"],
        }
        return dimension_values.get(dimension, ["Value A", "Value B", "Value C", "Value D"])
