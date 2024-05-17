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
