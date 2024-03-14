import math
from itertools import combinations
from typing import List

import numpy as np
import pandas as pd


class AnalysisManager:
    """
    Core class for implementing all major functions for analysis manager
    """

    def describe(self, data: pd.DataFrame) -> List[dict]:
        """
        Describe the data

        param:
            data: pd.DataFrame.

        return:
            List[dict]: Statistics of metric values for each (dimension, slice) for a given metric ID, and
            a list of dimensions.

        example usage:

            analysis_manager = AnalysisManager()

            start_date = "2024-01-01"
            end_date = "2024-04-30"
            metric_id = "5"
            dimensions = ["Proabability to Close Tier", "Owning Org", "Creating Org"]

            start_date = pd.to_datetime(start_date, format='%Y-%m-%d')
            end_date = pd.to_datetime(end_date, format='%Y-%m-%d')
            data['DAY'] = pd.to_datetime(data['DAY'], format='%Y-%m-%d')
            data['METRIC_ID'] = data['METRIC_ID'].astype(str)

            # Boolean indexing to filter rows
            data = data[(data['METRIC_ID'] == metric_id) &
                        (data['DAY'] >= start_date) &
                        (data['DAY'] <= end_date) &
                        (data['DIMENSION_NAME'].isin(dimensions))]

            response = analysis_manager.describe(data)

        sample response:
            [{'DIMENSION_NAME': 'Creating Org', 'SLICE': 'NA Sales', 'mean': 4.0, 'std': nan, 'p25': 4.0, 'p50': 4.0,
             'p75': 4.0, 'p90': 4.0, 'p95': 4.0, 'p99': 4.0, 'min': 4.0, 'max': 4.0, 'variance': nan, 'count': 1.0, 'sum': 4.0, 'unique': 1},

            {'DIMENSION_NAME': 'Creating Org', 'SLICE': 'Other', 'mean': 50.0, 'std': nan, 'p25': 50.0, 'p50': 50.0,
             'p75': 50.0, 'p90': 50.0, 'p95': 50.0, 'p99': 50.0, 'min': 50.0, 'max': 50.0, 'variance': nan, 'count': 1.0, 'sum': 50.0, 'unique': 1},

            {'DIMENSION_NAME': 'Owning Org', 'SLICE': 'NA CSM', 'mean': 56.0, 'std': nan, 'p25': 56.0, 'p50': 56.0,
             'p75': 56.0, 'p90': 56.0, 'p95': 56.0, 'p99': 56.0, 'min': 56.0, 'max': 56.0, 'variance': nan, 'count': 1.0, 'sum': 56.0, 'unique': 1}
            ]
        """

        grouped_data = data.groupby(["DIMENSION_NAME", "SLICE"])

        # Calculate additional percentiles and variance for each group
        grouped_stats = grouped_data["METRIC_VALUE"].describe(
            percentiles=[0.25, 0.50, 0.75, 0.90, 0.95, 0.99]
        )
        grouped_stats["variance"] = grouped_data["METRIC_VALUE"].var()
        grouped_stats["sum"] = grouped_data["METRIC_VALUE"].sum()
        grouped_stats["unique"] = grouped_data["METRIC_VALUE"].unique()

        grouped_stats.index.names = ["DIMENSION_NAME", "SLICE"]

        result = []

        # Iterate through the grouped DataFrame
        for index, row in grouped_stats.iterrows():
            stats_dict = {
                "DIMENSION_NAME": index[0],
                "SLICE": index[1],
                "mean": row["mean"],
                "std": row["std"],
                "p25": row["25%"],
                "p50": row["50%"],
                "p75": row["75%"],
                "p90": row["90%"],
                "p95": row["95%"],
                "p99": row["99%"],
                "min": row["min"],
                "max": row["max"],
                "variance": row["variance"],
                "count": row["count"],
                "sum": row["sum"],
                "unique": len(row["unique"]),
            }
            result.append(stats_dict)

        return result

    def correlate(
        self,
        data: pd.DataFrame,
        metric_ids: List[str],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
    ) -> List[dict]:
        """
        Compute the correlation between all the nC2 pairs generated from the given list metric_ids.

        param:
            data: pd.DataFrame. Already filtered on metric_ids, start_date, and end_date.
            metric_ids: given list metric_ids.
            start_date: pandas timestamp formatted in YYYY-MM-DD
            end_date: pandas timestamp formatted in YYYY-MM-DD

        return:
            dict having a list of correlation_coefficient, start_date, and end_date:
            [
              {
                "metric_id_1": "5",
                "metric_id_2": "6",
                "start_date": "2024-01-01",
                "end_date": "2024-04-30",
                "correlation_coefficient": 0.8
              },
              {
                "metric_id_1": "5",
                "metric_id_2": "7",
                "start_date": "2024-01-01",
                "end_date": "2024-04-30",
                "correlation_coefficient": 0.9
              }
            ]

        example usage:

            analysis_manager = AnalysisManager()

            start_date = "2020-01-01"
            end_date = "2025-04-30"

            start_date = pd.to_datetime(start_date, format="%Y-%m-%d")
            end_date = pd.to_datetime(end_date, format="%Y-%m-%d")
            data['DAY'] = pd.to_datetime(data['DAY'], format='%Y-%m-%d')
            data['METRIC_ID'] = data['METRIC_ID'].astype(str)


            data = data[(data["DAY"] >= start_date) & (data["DAY"] <= end_date)]

            response = analysis_manager.correlate(data, [], start_date=start_date, end_date=end_date)
        """
        data["METRIC_VALUE"] = pd.to_numeric(data["METRIC_VALUE"], errors="coerce")

        data = data.drop_duplicates(subset=["DAY", "METRIC_ID"]).reset_index(drop=True)

        precomputed_metric_values = {}
        for metric_id, group in data.groupby("METRIC_ID"):
            precomputed_metric_values[metric_id] = group["METRIC_VALUE"]

        response = []

        for metric_id1, metric_id2 in combinations(precomputed_metric_values.keys(), 2):
            series1 = precomputed_metric_values[metric_id1]
            series2 = precomputed_metric_values[metric_id2]

            len_series1 = len(series1)
            len_series2 = len(series2)
            if len_series1 < len_series2:
                series2 = series2[:len_series1]
            else:
                series1 = series1[:len_series2]
            correlation = np.corrcoef(series1, series2)[0, 1]
            response.append(
                {
                    "metric_id_1": metric_id1,
                    "metric_id_2": metric_id2,
                    "correlation_coefficient": correlation,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )

        return response
