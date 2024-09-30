from typing import Any

import numpy as np
import pandas as pd

from fulcrum_core.execptions import AnalysisError
from fulcrum_core.modules import BaseAnalyzer


class DescribeAnalyzer(BaseAnalyzer):
    """Describe Analyzer class."""

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
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df

    def analyze(  # noqa  # type: ignore
        self,
        df: pd.DataFrame,
        dimensions: list[str],
        metric_id: str,
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        aggregation_function: str,
        **kwargs,
    ) -> dict[str, Any] | pd.DataFrame:

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        response: list[dict[str, Any]] = []
        aggregation_function = "mean" if aggregation_function.lower() == "avg" else aggregation_function

        # for each dimension member, calculate the descriptive statistics
        for dimension in dimensions:
            # group by on that dimension, date, metric_id
            grouped_df = df.groupby([dimension, "date", "metric_id"]).agg({"value": aggregation_function}).reset_index()
            # find unique slices for the dimension
            unique_members = set(grouped_df[dimension])
            # for each member, calculate the descriptive statistics
            for dimension_member in unique_members:
                # filter the data for the member and date range
                filtered_df = grouped_df.loc[
                    (grouped_df[dimension] == dimension_member)
                    & (grouped_df["date"] >= start_date)
                    & (grouped_df["date"] < end_date)
                ]
                #  calculate the percentiles
                all_percentiles = filtered_df["value"].quantile([0.25, 0.50, 0.75, 0.90, 0.95, 0.99])
                # create the result dictionary
                result = {
                    "metric_id": str(metric_id),
                    "dimension": str(dimension),
                    "member": str(dimension_member),
                    "mean": float(filtered_df["value"].mean()),
                    "median": float(all_percentiles.loc[0.50]),
                    "percentile_25": float(all_percentiles.loc[0.25]),
                    "percentile_50": float(all_percentiles.loc[0.50]),
                    "percentile_75": float(all_percentiles.loc[0.75]),
                    "percentile_90": float(all_percentiles.loc[0.90]),
                    "percentile_95": float(all_percentiles.loc[0.95]),
                    "percentile_99": float(all_percentiles.loc[0.99]),
                    "min": float(filtered_df["value"].min()),
                    "max": float(filtered_df["value"].max()),
                    "variance": float(filtered_df["value"].var()),
                    "count": int(filtered_df["value"].count()),
                    "sum": float(filtered_df["value"].sum()),
                    "unique": int(len(filtered_df["value"].unique())),
                }
                response.append(result)

        result_df = pd.DataFrame(response)
        return result_df

    def post_process_result(self, result: Any) -> Any:
        df = result
        # replace np.nan with None
        df.replace(np.nan, None, inplace=True)
        return result
