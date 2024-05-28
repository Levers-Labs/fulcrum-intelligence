import random
from datetime import date

import pandas as pd
import pytest

start_date = date(2023, 4, 7)
number_of_data_points = 90


@pytest.fixture
def values_df():
    df = pd.DataFrame(
        {
            "date": pd.date_range(start=start_date, periods=number_of_data_points, freq="D"),
            "value": random.sample(range(100, 200), number_of_data_points),
        }
    )
    return df


@pytest.fixture
def metric_values(values_df):
    return values_df.to_dict(orient="records")


@pytest.fixture
def process_control_df(values_df):
    df = values_df.copy()
    df["central_line"] = df["value"].rolling(window=5).mean()
    df["ucl"] = df["central_line"].max()
    df["lcl"] = df["central_line"].min()
    df["slope"] = 1.5
    df["slope_change"] = 0
    df["trend_signal_detected"] = False
    return df


@pytest.fixture
def sorted_df():
    data = {
        "date": [
            "2024-01-01",
            "2024-01-08",
            "2024-01-15",
            "2024-01-22",
            "2024-01-29",
            "2024-02-05",
            "2024-02-12",
            "2024-02-19",
            "2024-02-26",
            "2024-03-04",
            "2024-03-11",
            "2024-03-18",
        ],
        "value": [6, 15, 16, 25, 65, 9, 14, 7, 18, 12, 14, 22],
    }
    df = pd.DataFrame(data)
    sorted_df = df.sort_values(by="value", ascending=False).reset_index(drop=True)
    sorted_df.index += 1
    sorted_df.index.name = "rank"
    return sorted_df
