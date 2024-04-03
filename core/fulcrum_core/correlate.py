from datetime import date
from itertools import combinations

import numpy as np
import pandas as pd


def correlate(data: pd.DataFrame, start_date: date, end_date: date) -> list[dict]:
    """
    Compute the correlation between all the nC2 pairs generated from the given list metric_ids.

    param:
        data: pd.DataFrame. Already filtered on metric_ids, start_date, and end_date.
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
        precomputed_metric_values[metric_id] = group[["METRIC_VALUE", "DAY"]]

    response = []

    for metric_id1, metric_id2 in combinations(precomputed_metric_values.keys(), 2):
        df1 = precomputed_metric_values[metric_id1]
        df2 = precomputed_metric_values[metric_id2]

        df1.drop_duplicates(subset=["DAY"]).reset_index(drop=True)
        df2.drop_duplicates(subset=["DAY"]).reset_index(drop=True)

        df1.dropna(inplace=True)
        df2.dropna(inplace=True)

        common_days = set(df1["DAY"]).intersection(df2["DAY"])

        df1 = df1[df1["DAY"].isin(common_days)]
        df2 = df2[df2["DAY"].isin(common_days)]

        series1 = df1["METRIC_VALUE"]
        series2 = df2["METRIC_VALUE"]

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
