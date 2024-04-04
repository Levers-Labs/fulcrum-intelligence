from datetime import date

import pandas as pd

from .describe import describe
from .correlate import correlate
from .process_control import process_control


class AnalysisManager:
    """
    Core class for implementing all major functions for analysis manager
    """

    def describe(
        self,
        data: list[dict],
        dimensions: list[str],
        metric_id: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        aggregation_function: str,
    ) -> list[dict]:
        result = describe(data, dimensions, metric_id, start_date, end_date, aggregation_function)
        return result

    def correlate(self, data: pd.DataFrame, start_date: date, end_date: date) -> list[dict]:
        result = correlate(data, start_date, end_date)
        return result

    def process_control(
        self,
        data: pd.DataFrame,
        metric_id: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        grain: str,
        debug: bool = False,
    ) -> dict:
        result = process_control(data, metric_id, start_date, end_date, grain, debug)
        return result
