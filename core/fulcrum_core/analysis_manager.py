import os
from datetime import date
import uuid

import pandas as pd

from .correlate import correlate
from .describe import describe
from .process_control import process_control
from .segment_drift import segment_drift


class AnalysisManager:
    """
    Core class for implementing all major functions for analysis manager
    """

    def describe(self, data: pd.DataFrame) -> list[dict]:
        result = describe(data)
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

    def segment_drift(
        self,
        data: dict

    ) -> list[dict]:
        """
            Before calling the segment_drift(), convert the df to CSV file and save it. This conversion is necessary
            as the dsensei author has written custom logic to parse data from CSV.
        """
        file_path = os.path.join("/tmp", f"{uuid.uuid4()}.csv")
        try:
            df = pd.json_normalize(data["data"])
            df.to_csv(file_path)
            data["data_file_path"] = file_path
            result = segment_drift(
                data
            )
        except Exception as err:
            raise Exception("Possibility of incorrect or incomplete data") from err
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)

        return result
