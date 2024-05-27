from itertools import combinations
from typing import Any

import numpy as np
import pandas as pd

from fulcrum_core.execptions import AnalysisError
from fulcrum_core.modules import BaseAnalyzer


class CorrelationAnalyzer(BaseAnalyzer):
    """Correlate Analyzer class."""

    def validate_input(self, df: pd.DataFrame, **kwargs):
        """
        Validate the input DataFrame for correlation analysis.

        Args:
            df (pd.DataFrame): The input DataFrame to validate.
            **kwargs: Additional keyword arguments for validation.

        Raises:
            ValueError: If the input DataFrame is missing required columns.
        """
        required_columns = ["date", "metric_id", "value"]
        missing_columns = set(required_columns) - set(df.columns)
        for column in required_columns:
            if column not in df.columns:
                missing_columns.add(column)

        if missing_columns:
            raise AnalysisError(f"Missing required columns: {missing_columns}")

    def preprocess_data(self, df: pd.DataFrame, **kwargs) -> Any:
        """
        Preprocess the input DataFrame for correlation analysis.

        Args:
            df (pd.DataFrame): The input DataFrame for preprocessing.
            **kwargs: Additional keyword arguments for preprocessing.

        Returns:
            pd.DataFrame: The preprocessed DataFrame.
        """
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        df = df.drop_duplicates(subset=["date", "metric_id"]).reset_index(drop=True)
        return df

    def analyze(self, df: pd.DataFrame, *args, **kwargs):
        # prepare the mapping of metric_id to its values
        metric_values = {}
        for metric_id, group in df.groupby("metric_id"):
            metric_values[metric_id] = group[["value", "date"]]

        response = []
        # calculate the correlation coefficient for each pair of metrics
        for metric_id1, metric_id2 in combinations(metric_values.keys(), 2):
            df1 = metric_values[metric_id1]
            df2 = metric_values[metric_id2]

            # drop duplicates and missing values
            df1.drop_duplicates(subset=["date"]).reset_index(drop=True)
            df2.drop_duplicates(subset=["date"]).reset_index(drop=True)

            # drop rows with missing values
            df1.dropna(inplace=True)
            df2.dropna(inplace=True)

            # get the common days
            common_days = set(df1["date"]).intersection(df2["date"])
            # filter the dataframes to have only common days
            df1 = df1[df1["date"].isin(common_days)]
            df2 = df2[df2["date"].isin(common_days)]

            # get the values of the metrics
            metric1_series = df1["value"]
            metric2_series = df2["value"]

            # calculate the correlation coefficient
            correlation = np.corrcoef(metric1_series, metric2_series)[0, 1]
            # append the result to the response
            response.append(
                {
                    "metric_id_1": metric_id1,
                    "metric_id_2": metric_id2,
                    "correlation_coefficient": correlation if not np.isnan(correlation) else 0.0,
                }
            )
        return response
