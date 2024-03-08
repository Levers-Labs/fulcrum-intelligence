import pandas as pd


class AnalysisManager:
    """
    Core class for implementing
    all major functions for analysis manager
    """

    def describe(self, data: pd.DataFrame) -> dict:
        """
        Describe the data
        """
        return data.describe().to_dict()
