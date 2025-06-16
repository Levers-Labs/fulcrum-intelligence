"""
Base class for pattern result generators.
"""

import random
from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Any

from levers.models.common import AnalysisWindow, BasePattern


class PatternResultGeneratorBase(ABC):
    """Base class for pattern result generators"""

    def __init__(self, metric_id: str, analysis_window: AnalysisWindow):
        self.metric_id = metric_id
        self.analysis_window = analysis_window
        self.analysis_date = date.today()

    @abstractmethod
    def generate(self) -> BasePattern:
        """Generate a realistic pattern result"""
        pass

    def _generate_random_percentage(self, min_val: float = -50.0, max_val: float = 50.0) -> float:
        """Generate a random percentage value"""
        return round(random.uniform(min_val, max_val), 2)

    def _generate_random_value(self, min_val: float = 100.0, max_val: float = 10000.0) -> float:
        """Generate a random metric value"""
        return round(random.uniform(min_val, max_val), 2)

    def _generate_date_string(self, days_ago: int = 0) -> str:
        """Generate a date string N days ago"""
        target_date = date.today() - timedelta(days=days_ago)
        return target_date.strftime("%Y-%m-%d")

    def _generate_realistic_variation(self, base_value: float, variation_percent: float = 15.0) -> float:
        """Generate a realistic variation around a base value"""
        variation = base_value * (variation_percent / 100)
        return base_value + random.uniform(-variation, variation)

    def _ensure_positive(self, value: float, minimum: float = 1.0) -> float:
        """Ensure a value is positive"""
        return max(value, minimum)
