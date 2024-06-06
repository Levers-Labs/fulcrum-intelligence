import json
import pathlib
from datetime import date

import pandas as pd
import pytest
from config import Paths

from fulcrum_core.enums import AggregationMethod, AggregationOption, MetricAim


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
    return df


@pytest.fixture
def process_control_df():
    df = pd.read_csv(pathlib.Path.joinpath(Paths.BASE_DIR, "tests/data/process_control.csv"))
    return df


@pytest.fixture
def process_control_output():
    with open(pathlib.Path.joinpath(Paths.BASE_DIR, "tests/data/process_control_output.json")) as fr:  # noqa: UP015
        process_control_output = json.loads(fr.read())
    return process_control_output


@pytest.fixture
def describe_data():
    with open(pathlib.Path.joinpath(Paths.BASE_DIR, "tests/data/describe_data.json")) as fr:  # noqa: UP015
        describe_data = json.load(fr)
    return describe_data


@pytest.fixture
def metric_expression():
    return {
        "type": "expression",
        "operator": "/",
        "operands": [
            {
                "type": "expression",
                "operator": "+",
                "operands": [
                    {"type": "metric", "metric_id": "SalesDevSpend"},
                    {"type": "metric", "metric_id": "SalesSpend"},
                ],
            },
            {
                "type": "expression",
                "operator": "+",
                "operands": [
                    {"type": "metric", "metric_id": "NewCust"},
                    {"type": "metric", "metric_id": "OldCust"},
                ],
            },
        ],
    }


@pytest.fixture
def component_drift_response():
    with open(pathlib.Path.joinpath(Paths.BASE_DIR, "tests/data/component_drift_response.json")) as fr:
        output = json.loads(fr.read())
    return output


@pytest.fixture
def describe_output():
    with open(pathlib.Path.joinpath(Paths.BASE_DIR, "tests/data/describe_output.json")) as fr:  # noqa: UP015
        describe_output = json.loads(fr.read())
    return describe_output


@pytest.fixture
def segment_drift_data():
    data = [
        {"date": "2024-03-01", "metric_id": "ToMRR", "value": 500, "region": "Asia", "stage_name": "Won"},
        {"date": "2024-03-02", "metric_id": "ToMRR", "value": 600, "region": "Asia", "stage_name": "Won"},
        {"date": "2024-03-02", "metric_id": "ToMRR", "value": 600, "region": "Asia", "stage_name": "Won"},
        {"date": "2024-03-02", "metric_id": "ToMRR", "value": 400, "region": "Asia", "stage_name": "Procurement"},
        {"date": "2024-03-03", "metric_id": "ToMRR", "value": 300, "region": "EMEA", "stage_name": "Lost"},
        {"date": "2024-03-04", "metric_id": "ToMRR", "value": 200, "region": "EMEA", "stage_name": "Sale"},
        {"date": "2024-03-05", "metric_id": "ToMRR", "value": 800, "region": "EMEA", "stage_name": "Sale"},
        {"date": "2025-03-01", "metric_id": "ToMRR", "value": 100, "region": "Asia", "stage_name": "Won"},
        {"date": "2025-03-02", "metric_id": "ToMRR", "value": 200, "region": "Asia", "stage_name": "Won"},
        {"date": "2025-03-02", "metric_id": "ToMRR", "value": 300, "region": "Asia", "stage_name": "Won"},
        {"date": "2025-03-02", "metric_id": "ToMRR", "value": 400, "region": "Asia", "stage_name": "Procurement"},
        {"date": "2025-03-03", "metric_id": "ToMRR", "value": 500, "region": "EMEA", "stage_name": "Lost"},
        {"date": "2025-03-04", "metric_id": "ToMRR", "value": 600, "region": "EMEA", "stage_name": "Sale"},
        {"date": "2025-03-05", "metric_id": "ToMRR", "value": 700, "region": "EMEA", "stage_name": "Sale"},
    ]

    date_column = "date"
    metric_value_column = "value"
    dimensions = ["region", "stage_name"]

    segment_drift_data = {
        "data": data,
        "evaluation_start_date": date(2025, 3, 1),
        "evaluation_end_date": date(2025, 3, 30),
        "comparison_start_date": date(2024, 3, 1),
        "comparison_end_date": date(2024, 3, 30),
        "date_column": date_column,
        "metric_column": metric_value_column,
        "dimensions": dimensions,
        "aggregation_option": AggregationOption.SUM.value,
        "aggregation_method": AggregationMethod.SUM.value,
        "target_metric_direction": MetricAim.INCREASING,
    }

    return segment_drift_data


@pytest.fixture
def dsensei_csv_file_id():
    return "ac60684ce86261be1227a43f75ecef96"


@pytest.fixture
def insight_api_response():
    return {
        "value_SUM": {
            "name": "SUM value",
            "totalSegments": 10,
            "expectedChangePercentage": 0,
            "aggregationMethod": "SUM",
            "baselineNumRows": 7,
            "comparisonNumRows": 7,
            "baselineValue": 3400,
            "comparisonValue": 2800,
            "baselineValueByDate": [
                {"date": "2024-03-01", "value": 500},
                {"date": "2024-03-02", "value": 1600},
                {"date": "2024-03-03", "value": 300},
                {"date": "2024-03-04", "value": 200},
                {"date": "2024-03-05", "value": 800},
            ],
            "comparisonValueByDate": [
                {"date": "2025-03-01", "value": 100},
                {"date": "2025-03-02", "value": 900},
                {"date": "2025-03-03", "value": 500},
                {"date": "2025-03-04", "value": 600},
                {"date": "2025-03-05", "value": 700},
            ],
            "baselineDateRange": ["2024-03-01", "2024-03-30"],
            "comparisonDateRange": ["2025-03-01", "2025-03-30"],
            "topDriverSliceKeys": [
                "region:Asia",
                "stage_name:Won",
                "region:Asia|stage_name:Won",
                "region:EMEA",
                "stage_name:Sale",
                "region:EMEA|stage_name:Sale",
                "stage_name:Lost",
                "region:EMEA|stage_name:Lost",
                "stage_name:Procurement",
                "region:Asia|stage_name:Procurement",
            ],
            "dimensions": {
                "stage_name": {"name": "stage_name", "score": 0.4941942122512673, "is_key_dimension": True},
                "region": {"name": "region", "score": 0.45421245421245426, "is_key_dimension": True},
            },
            "dimensionSliceInfo": {
                "region:Asia": {
                    "key": [{"dimension": "region", "value": "Asia"}],
                    "serializedKey": "region:Asia",
                    "baselineValue": {"sliceCount": 4, "sliceSize": 0.5714285714285714, "sliceValue": 2100},
                    "comparisonValue": {"sliceCount": 4, "sliceSize": 0.5714285714285714, "sliceValue": 1000},
                    "impact": -1100,
                    "changePercentage": -0.5238095238095238,
                    "changeDev": 0.7693956406369576,
                    "absoluteContribution": -0.5610859728506787,
                    "confidence": 0.49999999999999956,
                    "sortValue": 1100,
                },
                "stage_name:Won": {
                    "key": [{"dimension": "stage_name", "value": "Won"}],
                    "serializedKey": "stage_name:Won",
                    "baselineValue": {"sliceCount": 3, "sliceSize": 0.42857142857142855, "sliceValue": 1700},
                    "comparisonValue": {"sliceCount": 3, "sliceSize": 0.42857142857142855, "sliceValue": 600},
                    "impact": -1100,
                    "changePercentage": -0.6470588235294118,
                    "changeDev": 0.8186592357997562,
                    "absoluteContribution": -0.47058823529411764,
                    "confidence": None,
                    "sortValue": 1100,
                },
                "region:Asia|stage_name:Won": {
                    "key": [{"dimension": "region", "value": "Asia"}, {"dimension": "stage_name", "value": "Won"}],
                    "serializedKey": "region:Asia|stage_name:Won",
                    "baselineValue": {"sliceCount": 3, "sliceSize": 0.42857142857142855, "sliceValue": 1700},
                    "comparisonValue": {"sliceCount": 3, "sliceSize": 0.42857142857142855, "sliceValue": 600},
                    "impact": -1100,
                    "changePercentage": -0.6470588235294118,
                    "changeDev": 0.8186592357997562,
                    "absoluteContribution": -0.47058823529411764,
                    "confidence": None,
                    "sortValue": 1100,
                },
                "region:EMEA": {
                    "key": [{"dimension": "region", "value": "EMEA"}],
                    "serializedKey": "region:EMEA",
                    "baselineValue": {"sliceCount": 3, "sliceSize": 0.42857142857142855, "sliceValue": 1300},
                    "comparisonValue": {"sliceCount": 3, "sliceSize": 0.42857142857142855, "sliceValue": 1800},
                    "impact": 500,
                    "changePercentage": 0.38461538461538464,
                    "changeDev": 0.5649408550131507,
                    "absoluteContribution": 0.3473389355742297,
                    "confidence": 0.23080282980050915,
                    "sortValue": 500,
                },
                "stage_name:Sale": {
                    "key": [{"dimension": "stage_name", "value": "Sale"}],
                    "serializedKey": "stage_name:Sale",
                    "baselineValue": {"sliceCount": 2, "sliceSize": 0.2857142857142857, "sliceValue": 1000},
                    "comparisonValue": {"sliceCount": 2, "sliceSize": 0.2857142857142857, "sliceValue": 1300},
                    "impact": 300,
                    "changePercentage": 0.3,
                    "changeDev": 0.3795601911435233,
                    "absoluteContribution": 0.19852941176470587,
                    "confidence": None,
                    "sortValue": 300,
                },
                "region:EMEA|stage_name:Sale": {
                    "key": [{"dimension": "region", "value": "EMEA"}, {"dimension": "stage_name", "value": "Sale"}],
                    "serializedKey": "region:EMEA|stage_name:Sale",
                    "baselineValue": {"sliceCount": 2, "sliceSize": 0.2857142857142857, "sliceValue": 1000},
                    "comparisonValue": {"sliceCount": 2, "sliceSize": 0.2857142857142857, "sliceValue": 1300},
                    "impact": 300,
                    "changePercentage": 0.3,
                    "changeDev": 0.3795601911435233,
                    "absoluteContribution": 0.19852941176470587,
                    "confidence": None,
                    "sortValue": 300,
                },
                "stage_name:Lost": {
                    "key": [{"dimension": "stage_name", "value": "Lost"}],
                    "serializedKey": "stage_name:Lost",
                    "baselineValue": {"sliceCount": 1, "sliceSize": 0.14285714285714285, "sliceValue": 300},
                    "comparisonValue": {"sliceCount": 1, "sliceSize": 0.14285714285714285, "sliceValue": 500},
                    "impact": 200,
                    "changePercentage": 0.6666666666666666,
                    "changeDev": 0.49744975165091326,
                    "absoluteContribution": 0.08159392789373812,
                    "confidence": None,
                    "sortValue": 200,
                },
                "region:EMEA|stage_name:Lost": {
                    "key": [{"dimension": "region", "value": "EMEA"}, {"dimension": "stage_name", "value": "Lost"}],
                    "serializedKey": "region:EMEA|stage_name:Lost",
                    "baselineValue": {"sliceCount": 1, "sliceSize": 0.14285714285714285, "sliceValue": 300},
                    "comparisonValue": {"sliceCount": 1, "sliceSize": 0.14285714285714285, "sliceValue": 500},
                    "impact": 200,
                    "changePercentage": 0.6666666666666666,
                    "changeDev": 0.49744975165091326,
                    "absoluteContribution": 0.08159392789373812,
                    "confidence": None,
                    "sortValue": 200,
                },
                "stage_name:Procurement": {
                    "key": [{"dimension": "stage_name", "value": "Procurement"}],
                    "serializedKey": "stage_name:Procurement",
                    "baselineValue": {"sliceCount": 1, "sliceSize": 0.14285714285714285, "sliceValue": 400},
                    "comparisonValue": {"sliceCount": 1, "sliceSize": 0.14285714285714285, "sliceValue": 400},
                    "impact": 0,
                    "changePercentage": 0.0,
                    "changeDev": 0.0,
                    "absoluteContribution": 0.023529411764705882,
                    "confidence": None,
                    "sortValue": 0,
                },
                "region:Asia|stage_name:Procurement": {
                    "key": [
                        {"dimension": "region", "value": "Asia"},
                        {"dimension": "stage_name", "value": "Procurement"},
                    ],
                    "serializedKey": "region:Asia|stage_name:Procurement",
                    "baselineValue": {"sliceCount": 1, "sliceSize": 0.14285714285714285, "sliceValue": 400},
                    "comparisonValue": {"sliceCount": 1, "sliceSize": 0.14285714285714285, "sliceValue": 400},
                    "impact": 0,
                    "changePercentage": 0.0,
                    "changeDev": 0.0,
                    "absoluteContribution": 0.023529411764705882,
                    "confidence": None,
                    "sortValue": 0,
                },
            },
            "keyDimensions": ["stage_name", "region"],
            "filters": [],
            "id": "value_SUM",
        }
    }


@pytest.fixture
def get_insight_response():
    return {
        "name": "SUM value",
        "total_segments": 10,
        "expected_change_percentage": 0,
        "aggregation_method": "SUM",
        "comparison_num_rows": 7,
        "evaluation_num_rows": 7,
        "comparison_value": 3400,
        "evaluation_value": 2800,
        "comparison_value_by_date": [
            {"date": "2024-03-01", "value": 500},
            {"date": "2024-03-02", "value": 1600},
            {"date": "2024-03-03", "value": 300},
            {"date": "2024-03-04", "value": 200},
            {"date": "2024-03-05", "value": 800},
        ],
        "evaluation_value_by_date": [
            {"date": "2025-03-01", "value": 100},
            {"date": "2025-03-02", "value": 900},
            {"date": "2025-03-03", "value": 500},
            {"date": "2025-03-04", "value": 600},
            {"date": "2025-03-05", "value": 700},
        ],
        "comparison_date_range": ["2024-03-01", "2024-03-30"],
        "evaluation_date_range": ["2025-03-01", "2025-03-30"],
        "dimensions": [
            {"name": "stage_name", "score": 0.4941942122512673, "is_key_dimension": True},
            {"name": "region", "score": 0.45421245421245426, "is_key_dimension": True},
        ],
        "key_dimensions": ["stage_name", "region"],
        "filters": [],
        "id": "value_SUM",
        "dimension_slices": [
            {
                "key": [{"dimension": "region", "value": "Asia"}],
                "serialized_key": "region:Asia",
                "comparison_value": {
                    "slice_count": 4,
                    "slice_size": 0.5714285714285714,
                    "slice_value": 2100,
                    "slice_share": 61.76470588235294,
                },
                "evaluation_value": {
                    "slice_count": 4,
                    "slice_size": 0.5714285714285714,
                    "slice_value": 1000,
                    "slice_share": 35.714285714285715,
                },
                "impact": -1100,
                "change_percentage": -0.5238095238095238,
                "change_dev": 0.7693956406369576,
                "absolute_contribution": -0.5610859728506787,
                "confidence": 0.49999999999999956,
                "sort_value": 1100,
                "slice_share_change_percentage": -26.050420168067227,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Won"}],
                "serialized_key": "stage_name:Won",
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1700,
                    "slice_share": 50.0,
                },
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 600,
                    "slice_share": 21.428571428571427,
                },
                "impact": -1100,
                "change_percentage": -0.6470588235294118,
                "change_dev": 0.8186592357997562,
                "absolute_contribution": -0.47058823529411764,
                "confidence": None,
                "sort_value": 1100,
                "slice_share_change_percentage": -28.571428571428573,
            },
            {
                "key": [{"dimension": "region", "value": "Asia"}, {"dimension": "stage_name", "value": "Won"}],
                "serialized_key": "region:Asia|stage_name:Won",
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1700,
                    "slice_share": 50.0,
                },
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 600,
                    "slice_share": 21.428571428571427,
                },
                "impact": -1100,
                "change_percentage": -0.6470588235294118,
                "change_dev": 0.8186592357997562,
                "absolute_contribution": -0.47058823529411764,
                "confidence": None,
                "sort_value": 1100,
                "slice_share_change_percentage": -28.571428571428573,
            },
            {
                "key": [{"dimension": "region", "value": "EMEA"}],
                "serialized_key": "region:EMEA",
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1300,
                    "slice_share": 38.23529411764706,
                },
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1800,
                    "slice_share": 64.28571428571429,
                },
                "impact": 500,
                "change_percentage": 0.38461538461538464,
                "change_dev": 0.5649408550131507,
                "absolute_contribution": 0.3473389355742297,
                "confidence": 0.23080282980050915,
                "sort_value": 500,
                "slice_share_change_percentage": 26.050420168067234,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Sale"}],
                "serialized_key": "stage_name:Sale",
                "comparison_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1000,
                    "slice_share": 29.411764705882355,
                },
                "evaluation_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1300,
                    "slice_share": 46.42857142857143,
                },
                "impact": 300,
                "change_percentage": 0.3,
                "change_dev": 0.3795601911435233,
                "absolute_contribution": 0.19852941176470587,
                "confidence": None,
                "sort_value": 300,
                "slice_share_change_percentage": 17.016806722689076,
            },
            {
                "key": [{"dimension": "region", "value": "EMEA"}, {"dimension": "stage_name", "value": "Sale"}],
                "serialized_key": "region:EMEA|stage_name:Sale",
                "comparison_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1000,
                    "slice_share": 29.411764705882355,
                },
                "evaluation_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1300,
                    "slice_share": 46.42857142857143,
                },
                "impact": 300,
                "change_percentage": 0.3,
                "change_dev": 0.3795601911435233,
                "absolute_contribution": 0.19852941176470587,
                "confidence": None,
                "sort_value": 300,
                "slice_share_change_percentage": 17.016806722689076,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Lost"}],
                "serialized_key": "stage_name:Lost",
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 300,
                    "slice_share": 8.823529411764707,
                },
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 500,
                    "slice_share": 17.857142857142858,
                },
                "impact": 200,
                "change_percentage": 0.6666666666666666,
                "change_dev": 0.49744975165091326,
                "absolute_contribution": 0.08159392789373812,
                "confidence": None,
                "sort_value": 200,
                "slice_share_change_percentage": 9.033613445378151,
            },
            {
                "key": [{"dimension": "region", "value": "EMEA"}, {"dimension": "stage_name", "value": "Lost"}],
                "serialized_key": "region:EMEA|stage_name:Lost",
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 300,
                    "slice_share": 8.823529411764707,
                },
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 500,
                    "slice_share": 17.857142857142858,
                },
                "impact": 200,
                "change_percentage": 0.6666666666666666,
                "change_dev": 0.49744975165091326,
                "absolute_contribution": 0.08159392789373812,
                "confidence": None,
                "sort_value": 200,
                "slice_share_change_percentage": 9.033613445378151,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Procurement"}],
                "serialized_key": "stage_name:Procurement",
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                    "slice_share": 11.76470588235294,
                },
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                    "slice_share": 14.285714285714285,
                },
                "impact": 0,
                "change_percentage": 0.0,
                "change_dev": 0.0,
                "absolute_contribution": 0.023529411764705882,
                "confidence": None,
                "sort_value": 0,
                "slice_share_change_percentage": 2.5210084033613445,
            },
            {
                "key": [{"dimension": "region", "value": "Asia"}, {"dimension": "stage_name", "value": "Procurement"}],
                "serialized_key": "region:Asia|stage_name:Procurement",
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                    "slice_share": 11.76470588235294,
                },
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                    "slice_share": 14.285714285714285,
                },
                "impact": 0,
                "change_percentage": 0.0,
                "change_dev": 0.0,
                "absolute_contribution": 0.023529411764705882,
                "confidence": None,
                "sort_value": 0,
                "slice_share_change_percentage": 2.5210084033613445,
            },
        ],
        "dimension_slices_permutation_keys": [
            "region:Asia",
            "stage_name:Won",
            "region:Asia|stage_name:Won",
            "region:EMEA",
            "stage_name:Sale",
            "region:EMEA|stage_name:Sale",
            "stage_name:Lost",
            "region:EMEA|stage_name:Lost",
            "stage_name:Procurement",
            "region:Asia|stage_name:Procurement",
        ],
    }


@pytest.fixture
def segment_drift_output():
    return {
        "id": "value_SUM",
        "name": "SUM value",
        "total_segments": 10,
        "expected_change_percentage": 0,
        "aggregation_method": "SUM",
        "evaluation_num_rows": 7,
        "comparison_num_rows": 7,
        "evaluation_value": 2800,
        "comparison_value": 3400,
        "evaluation_value_by_date": [
            {"date": "2025-03-01", "value": 100},
            {"date": "2025-03-02", "value": 900},
            {"date": "2025-03-03", "value": 500},
            {"date": "2025-03-04", "value": 600},
            {"date": "2025-03-05", "value": 700},
        ],
        "comparison_value_by_date": [
            {"date": "2024-03-01", "value": 500},
            {"date": "2024-03-02", "value": 1600},
            {"date": "2024-03-03", "value": 300},
            {"date": "2024-03-04", "value": 200},
            {"date": "2024-03-05", "value": 800},
        ],
        "evaluation_date_range": ["2025-03-01", "2025-03-05"],
        "comparison_date_range": ["2024-03-01", "2024-03-05"],
        "dimensions": [
            {"name": "stage_name", "score": 0.4941942122512673, "is_key_dimension": True},
            {"name": "region", "score": 0.45421245421245426, "is_key_dimension": True},
        ],
        "key_dimensions": ["stage_name", "region"],
        "filters": [],
        "dimension_slices": [
            {
                "key": [{"dimension": "region", "value": "Asia"}],
                "serialized_key": "region:Asia",
                "evaluation_value": {
                    "slice_count": 4,
                    "slice_size": 0.5714285714285714,
                    "slice_value": 1000,
                    "slice_share": 35.714285714285715,
                },
                "comparison_value": {
                    "slice_count": 4,
                    "slice_size": 0.5714285714285714,
                    "slice_value": 2100,
                    "slice_share": 61.76470588235294,
                },
                "impact": -1100,
                "change_percentage": -0.5238095238095238,
                "change_dev": 0.7693956406369576,
                "absolute_contribution": -0.5610859728506787,
                "confidence": 0.49999999999999956,
                "sort_value": 1100,
                "relative_change": -34.73389355742297,
                "pressure": "DOWNWARD",
                "slice_share_change_percentage": -26.050420168067227,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Won"}],
                "serialized_key": "stage_name:Won",
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 600,
                    "slice_share": 21.428571428571427,
                },
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1700,
                    "slice_share": 50,
                },
                "impact": -1100,
                "change_percentage": -0.6470588235294118,
                "change_dev": 0.8186592357997562,
                "absolute_contribution": -0.47058823529411764,
                "confidence": None,
                "sort_value": 1100,
                "relative_change": -47.05882352941176,
                "pressure": "DOWNWARD",
                "slice_share_change_percentage": -28.571428571428573,
            },
            {
                "key": [{"dimension": "region", "value": "Asia"}, {"dimension": "stage_name", "value": "Won"}],
                "serialized_key": "region:Asia|stage_name:Won",
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 600,
                    "slice_share": 21.428571428571427,
                },
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1700,
                    "slice_share": 50,
                },
                "impact": -1100,
                "change_percentage": -0.6470588235294118,
                "change_dev": 0.8186592357997562,
                "absolute_contribution": -0.47058823529411764,
                "confidence": None,
                "sort_value": 1100,
                "relative_change": -47.05882352941176,
                "pressure": "DOWNWARD",
                "slice_share_change_percentage": -28.571428571428573,
            },
            {
                "key": [{"dimension": "region", "value": "EMEA"}],
                "serialized_key": "region:EMEA",
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1800,
                    "slice_share": 64.28571428571429,
                },
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1300,
                    "slice_share": 38.23529411764706,
                },
                "impact": 500,
                "change_percentage": 0.38461538461538464,
                "change_dev": 0.5649408550131507,
                "absolute_contribution": 0.3473389355742297,
                "confidence": 0.23080282980050915,
                "sort_value": 500,
                "relative_change": 56.10859728506787,
                "pressure": "UPWARD",
                "slice_share_change_percentage": 26.050420168067234,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Sale"}],
                "serialized_key": "stage_name:Sale",
                "evaluation_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1300,
                    "slice_share": 46.42857142857143,
                },
                "comparison_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1000,
                    "slice_share": 29.411764705882355,
                },
                "impact": 300,
                "change_percentage": 0.3,
                "change_dev": 0.3795601911435233,
                "absolute_contribution": 0.19852941176470587,
                "confidence": None,
                "sort_value": 300,
                "relative_change": 47.647058823529406,
                "pressure": "UPWARD",
                "slice_share_change_percentage": 17.016806722689076,
            },
            {
                "key": [{"dimension": "region", "value": "EMEA"}, {"dimension": "stage_name", "value": "Sale"}],
                "serialized_key": "region:EMEA|stage_name:Sale",
                "evaluation_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1300,
                    "slice_share": 46.42857142857143,
                },
                "comparison_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1000,
                    "slice_share": 29.411764705882355,
                },
                "impact": 300,
                "change_percentage": 0.3,
                "change_dev": 0.3795601911435233,
                "absolute_contribution": 0.19852941176470587,
                "confidence": None,
                "sort_value": 300,
                "relative_change": 47.647058823529406,
                "pressure": "UPWARD",
                "slice_share_change_percentage": 17.016806722689076,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Lost"}],
                "serialized_key": "stage_name:Lost",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 500,
                    "slice_share": 17.857142857142858,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 300,
                    "slice_share": 8.823529411764707,
                },
                "impact": 200,
                "change_percentage": 0.6666666666666666,
                "change_dev": 0.49744975165091326,
                "absolute_contribution": 0.08159392789373812,
                "confidence": None,
                "sort_value": 200,
                "relative_change": 84.31372549019608,
                "pressure": "UPWARD",
                "slice_share_change_percentage": 9.033613445378151,
            },
            {
                "key": [{"dimension": "region", "value": "EMEA"}, {"dimension": "stage_name", "value": "Lost"}],
                "serialized_key": "region:EMEA|stage_name:Lost",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 500,
                    "slice_share": 17.857142857142858,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 300,
                    "slice_share": 8.823529411764707,
                },
                "impact": 200,
                "change_percentage": 0.6666666666666666,
                "change_dev": 0.49744975165091326,
                "absolute_contribution": 0.08159392789373812,
                "confidence": None,
                "sort_value": 200,
                "relative_change": 84.31372549019608,
                "pressure": "UPWARD",
                "slice_share_change_percentage": 9.033613445378151,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Procurement"}],
                "serialized_key": "stage_name:Procurement",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                    "slice_share": 14.285714285714285,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                    "slice_share": 11.76470588235294,
                },
                "impact": 0,
                "change_percentage": 0,
                "change_dev": 0,
                "absolute_contribution": 0.023529411764705882,
                "confidence": None,
                "sort_value": 0,
                "relative_change": 17.647058823529413,
                "pressure": "UNCHANGED",
                "slice_share_change_percentage": 2.5210084033613445,
            },
            {
                "key": [{"dimension": "region", "value": "Asia"}, {"dimension": "stage_name", "value": "Procurement"}],
                "serialized_key": "region:Asia|stage_name:Procurement",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                    "slice_share": 14.285714285714285,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                    "slice_share": 11.76470588235294,
                },
                "impact": 0,
                "change_percentage": 0,
                "change_dev": 0,
                "absolute_contribution": 0.023529411764705882,
                "confidence": None,
                "sort_value": 0,
                "relative_change": 17.647058823529413,
                "pressure": "UNCHANGED",
                "slice_share_change_percentage": 2.5210084033613445,
            },
        ],
        "dimension_slices_permutation_keys": [
            "region:Asia",
            "stage_name:Won",
            "region:Asia|stage_name:Won",
            "region:EMEA",
            "stage_name:Sale",
            "region:EMEA|stage_name:Sale",
            "stage_name:Lost",
            "region:EMEA|stage_name:Lost",
            "stage_name:Procurement",
            "region:Asia|stage_name:Procurement",
        ],
    }
