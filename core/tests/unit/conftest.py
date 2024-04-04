import pandas as pd
import pytest
import json


@pytest.fixture
def metric_values():
    return [
        {"date": "2022-09-01", "value": 363, "metric_id": "NewMRR"},
        {"date": "2024-10-20", "value": 1914, "metric_id": "ToMRR"},
        {"date": "2024-11-19", "value": 144, "metric_id": "NewMRR"},
        {"date": "2024-12-19", "value": 113, "metric_id": "NewMRR"},
        {"date": "2025-01-18", "value": 176, "metric_id": "NewMRR"},
        {"date": "2025-02-17", "value": 1094, "metric_id": "ToMRR"},
        {"date": "2025-03-19", "value": 712, "metric_id": "NewMRR"},
        {"date": "2025-04-18", "value": 1056, "metric_id": "ToMRR"},
        {"date": "2025-05-18", "value": 3028, "metric_id": "ToMRR"},
        {"date": "2025-06-17", "value": 3357, "metric_id": "ToMRR"},
    ]


@pytest.fixture
def correlate_df(metric_values):
    df = pd.DataFrame(metric_values)
    df["date"] = pd.to_datetime(df["date"])
    # rename columns
    df = df.rename(columns={"date": "DAY", "value": "METRIC_VALUE", "metric_id": "METRIC_ID"})
    return df


@pytest.fixture
def process_control_df():
    df = pd.read_csv("tests/data/process_control.csv")
    return df


@pytest.fixture
def process_control_output():
    with open("tests/data/process_control_output.txt", "r") as fr:  # noqa: UP015
        process_control_output = json.loads(fr.read())
    return process_control_output


@pytest.fixture
def describe_data():
    with open("tests/data/describe_data.json", "r") as fr: # noqa: UP015
        describe_data = json.load(fr)
    return describe_data


@pytest.fixture
def describe_output():
    with open("tests/data/describe_output.txt", "r") as fr:  # noqa: UP015
        describe_output = json.loads(fr.read())
    return describe_output
