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
            "baselineValue": 2800,
            "comparisonValue": 3400,
            "baselineValueByDate": [
                {"date": "2025-03-01", "value": 100},
                {"date": "2025-03-02", "value": 900},
                {"date": "2025-03-03", "value": 500},
                {"date": "2025-03-04", "value": 600},
                {"date": "2025-03-05", "value": 700},
            ],
            "comparisonValueByDate": [
                {"date": "2024-03-01", "value": 500},
                {"date": "2024-03-02", "value": 1600},
                {"date": "2024-03-03", "value": 300},
                {"date": "2024-03-04", "value": 200},
                {"date": "2024-03-05", "value": 800},
            ],
            "baselineDateRange": ["2025-03-01", "2025-03-30"],
            "comparisonDateRange": ["2024-03-01", "2024-03-30"],
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
                "region": {
                    "name": "region",
                    "score": 0.6888888888888889,
                    "is_key_dimension": True,
                },
                "stage_name": {
                    "name": "stage_name",
                    "score": 0.9962623591866414,
                    "is_key_dimension": True,
                },
            },
            "dimensionSliceInfo": {
                "region:Asia": {
                    "key": [{"dimension": "region", "value": "Asia"}],
                    "serializedKey": "region:Asia",
                    "baselineValue": {
                        "sliceCount": 4,
                        "sliceSize": 0.5714285714285714,
                        "sliceValue": 1000,
                    },
                    "comparisonValue": {
                        "sliceCount": 4,
                        "sliceSize": 0.5714285714285714,
                        "sliceValue": 2100,
                    },
                    "impact": 1100,
                    "changePercentage": 1.1,
                    "changeDev": 0.8570032134790982,
                    "absoluteContribution": 0.4920634920634921,
                    "confidence": 0.49999999999999956,
                    "sortValue": 1100,
                },
                "stage_name:Won": {
                    "key": [{"dimension": "stage_name", "value": "Won"}],
                    "serializedKey": "stage_name:Won",
                    "baselineValue": {
                        "sliceCount": 3,
                        "sliceSize": 0.42857142857142855,
                        "sliceValue": 600,
                    },
                    "comparisonValue": {
                        "sliceCount": 3,
                        "sliceSize": 0.42857142857142855,
                        "sliceValue": 1700,
                    },
                    "impact": 1100,
                    "changePercentage": 1.8333333333333333,
                    "changeDev": 1.2303091986141086,
                    "absoluteContribution": 0.44155844155844154,
                    "confidence": None,
                    "sortValue": 1100,
                },
                "region:Asia|stage_name:Won": {
                    "key": [
                        {"dimension": "region", "value": "Asia"},
                        {"dimension": "stage_name", "value": "Won"},
                    ],
                    "serializedKey": "region:Asia|stage_name:Won",
                    "baselineValue": {
                        "sliceCount": 3,
                        "sliceSize": 0.42857142857142855,
                        "sliceValue": 600,
                    },
                    "comparisonValue": {
                        "sliceCount": 3,
                        "sliceSize": 0.42857142857142855,
                        "sliceValue": 1700,
                    },
                    "impact": 1100,
                    "changePercentage": 1.8333333333333333,
                    "changeDev": 1.2303091986141086,
                    "absoluteContribution": 0.44155844155844154,
                    "confidence": None,
                    "sortValue": 1100,
                },
                "region:EMEA": {
                    "key": [{"dimension": "region", "value": "EMEA"}],
                    "serializedKey": "region:EMEA",
                    "baselineValue": {
                        "sliceCount": 3,
                        "sliceSize": 0.42857142857142855,
                        "sliceValue": 1800,
                    },
                    "comparisonValue": {
                        "sliceCount": 3,
                        "sliceSize": 0.42857142857142855,
                        "sliceValue": 1300,
                    },
                    "impact": -500,
                    "changePercentage": -0.2777777777777778,
                    "changeDev": 0.21641495289876214,
                    "absoluteContribution": -0.8857142857142858,
                    "confidence": 0.16687067367945163,
                    "sortValue": 500,
                },
                "stage_name:Sale": {
                    "key": [{"dimension": "stage_name", "value": "Sale"}],
                    "serializedKey": "stage_name:Sale",
                    "baselineValue": {
                        "sliceCount": 2,
                        "sliceSize": 0.2857142857142857,
                        "sliceValue": 1300,
                    },
                    "comparisonValue": {
                        "sliceCount": 2,
                        "sliceSize": 0.2857142857142857,
                        "sliceValue": 1000,
                    },
                    "impact": -300,
                    "changePercentage": -0.23076923076923078,
                    "changeDev": 0.15486409493044723,
                    "absoluteContribution": -0.3857142857142857,
                    "confidence": None,
                    "sortValue": 300,
                },
                "region:EMEA|stage_name:Sale": {
                    "key": [
                        {"dimension": "region", "value": "EMEA"},
                        {"dimension": "stage_name", "value": "Sale"},
                    ],
                    "serializedKey": "region:EMEA|stage_name:Sale",
                    "baselineValue": {
                        "sliceCount": 2,
                        "sliceSize": 0.2857142857142857,
                        "sliceValue": 1300,
                    },
                    "comparisonValue": {
                        "sliceCount": 2,
                        "sliceSize": 0.2857142857142857,
                        "sliceValue": 1000,
                    },
                    "impact": -300,
                    "changePercentage": -0.23076923076923078,
                    "changeDev": 0.15486409493044723,
                    "absoluteContribution": -0.3857142857142857,
                    "confidence": None,
                    "sortValue": 300,
                },
                "stage_name:Lost": {
                    "key": [{"dimension": "stage_name", "value": "Lost"}],
                    "serializedKey": "stage_name:Lost",
                    "baselineValue": {
                        "sliceCount": 1,
                        "sliceSize": 0.14285714285714285,
                        "sliceValue": 500,
                    },
                    "comparisonValue": {
                        "sliceCount": 1,
                        "sliceSize": 0.14285714285714285,
                        "sliceValue": 300,
                    },
                    "impact": -200,
                    "changePercentage": -0.4,
                    "changeDev": 0.15831202465660632,
                    "absoluteContribution": -0.13354037267080746,
                    "confidence": None,
                    "sortValue": 200,
                },
                "region:EMEA|stage_name:Lost": {
                    "key": [
                        {"dimension": "region", "value": "EMEA"},
                        {"dimension": "stage_name", "value": "Lost"},
                    ],
                    "serializedKey": "region:EMEA|stage_name:Lost",
                    "baselineValue": {
                        "sliceCount": 1,
                        "sliceSize": 0.14285714285714285,
                        "sliceValue": 500,
                    },
                    "comparisonValue": {
                        "sliceCount": 1,
                        "sliceSize": 0.14285714285714285,
                        "sliceValue": 300,
                    },
                    "impact": -200,
                    "changePercentage": -0.4,
                    "changeDev": 0.15831202465660632,
                    "absoluteContribution": -0.13354037267080746,
                    "confidence": None,
                    "sortValue": 200,
                },
                "stage_name:Procurement": {
                    "key": [{"dimension": "stage_name", "value": "Procurement"}],
                    "serializedKey": "stage_name:Procurement",
                    "baselineValue": {
                        "sliceCount": 1,
                        "sliceSize": 0.14285714285714285,
                        "sliceValue": 400,
                    },
                    "comparisonValue": {
                        "sliceCount": 1,
                        "sliceSize": 0.14285714285714285,
                        "sliceValue": 400,
                    },
                    "impact": 0,
                    "changePercentage": 0.0,
                    "changeDev": 0.0,
                    "absoluteContribution": -0.035714285714285726,
                    "confidence": None,
                    "sortValue": 0,
                },
                "region:Asia|stage_name:Procurement": {
                    "key": [
                        {"dimension": "region", "value": "Asia"},
                        {"dimension": "stage_name", "value": "Procurement"},
                    ],
                    "serializedKey": "region:Asia|stage_name:Procurement",
                    "baselineValue": {
                        "sliceCount": 1,
                        "sliceSize": 0.14285714285714285,
                        "sliceValue": 400,
                    },
                    "comparisonValue": {
                        "sliceCount": 1,
                        "sliceSize": 0.14285714285714285,
                        "sliceValue": 400,
                    },
                    "impact": 0,
                    "changePercentage": 0.0,
                    "changeDev": 0.0,
                    "absoluteContribution": -0.035714285714285726,
                    "confidence": None,
                    "sortValue": 0,
                },
            },
            "keyDimensions": ["region", "stage_name"],
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
        "evaluation_date_range": ["2025-03-01", "2025-03-30"],
        "comparison_date_range": ["2024-03-01", "2024-03-30"],
        "dimensions": [
            {"name": "region", "score": 0.6888888888888889, "is_key_dimension": True},
            {"name": "stage_name", "score": 0.9962623591866414, "is_key_dimension": True},
        ],
        "key_dimensions": ["region", "stage_name"],
        "filters": [],
        "id": "value_SUM",
        "dimension_slices": [
            {
                "key": [{"dimension": "region", "value": "Asia"}],
                "serialized_key": "region:Asia",
                "evaluation_value": {
                    "slice_count": 4,
                    "slice_size": 0.5714285714285714,
                    "slice_value": 1000,
                },
                "comparison_value": {
                    "slice_count": 4,
                    "slice_size": 0.5714285714285714,
                    "slice_value": 2100,
                },
                "impact": 1100,
                "change_percentage": 1.1,
                "change_dev": 0.8570032134790982,
                "absolute_contribution": 0.4920634920634921,
                "confidence": 0.49999999999999956,
                "sort_value": 1100,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Won"}],
                "serialized_key": "stage_name:Won",
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 600,
                },
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1700,
                },
                "impact": 1100,
                "change_percentage": 1.8333333333333333,
                "change_dev": 1.2303091986141086,
                "absolute_contribution": 0.44155844155844154,
                "confidence": None,
                "sort_value": 1100,
            },
            {
                "key": [
                    {"dimension": "region", "value": "Asia"},
                    {"dimension": "stage_name", "value": "Won"},
                ],
                "serialized_key": "region:Asia|stage_name:Won",
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 600,
                },
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1700,
                },
                "impact": 1100,
                "change_percentage": 1.8333333333333333,
                "change_dev": 1.2303091986141086,
                "absolute_contribution": 0.44155844155844154,
                "confidence": None,
                "sort_value": 1100,
            },
            {
                "key": [{"dimension": "region", "value": "EMEA"}],
                "serialized_key": "region:EMEA",
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1800,
                },
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1300,
                },
                "impact": -500,
                "change_percentage": -0.2777777777777778,
                "change_dev": 0.21641495289876214,
                "absolute_contribution": -0.8857142857142858,
                "confidence": 0.16687067367945163,
                "sort_value": 500,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Sale"}],
                "serialized_key": "stage_name:Sale",
                "evaluation_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1300,
                },
                "comparison_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1000,
                },
                "impact": -300,
                "change_percentage": -0.23076923076923078,
                "change_dev": 0.15486409493044723,
                "absolute_contribution": -0.3857142857142857,
                "confidence": None,
                "sort_value": 300,
            },
            {
                "key": [
                    {"dimension": "region", "value": "EMEA"},
                    {"dimension": "stage_name", "value": "Sale"},
                ],
                "serialized_key": "region:EMEA|stage_name:Sale",
                "evaluation_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1300,
                },
                "comparison_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1000,
                },
                "impact": -300,
                "change_percentage": -0.23076923076923078,
                "change_dev": 0.15486409493044723,
                "absolute_contribution": -0.3857142857142857,
                "confidence": None,
                "sort_value": 300,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Lost"}],
                "serialized_key": "stage_name:Lost",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 500,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 300,
                },
                "impact": -200,
                "change_percentage": -0.4,
                "change_dev": 0.15831202465660632,
                "absolute_contribution": -0.13354037267080746,
                "confidence": None,
                "sort_value": 200,
            },
            {
                "key": [
                    {"dimension": "region", "value": "EMEA"},
                    {"dimension": "stage_name", "value": "Lost"},
                ],
                "serialized_key": "region:EMEA|stage_name:Lost",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 500,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 300,
                },
                "impact": -200,
                "change_percentage": -0.4,
                "change_dev": 0.15831202465660632,
                "absolute_contribution": -0.13354037267080746,
                "confidence": None,
                "sort_value": 200,
            },
            {
                "key": [{"dimension": "stage_name", "value": "Procurement"}],
                "serialized_key": "stage_name:Procurement",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                },
                "impact": 0,
                "change_percentage": 0.0,
                "change_dev": 0.0,
                "absolute_contribution": -0.035714285714285726,
                "confidence": None,
                "sort_value": 0,
            },
            {
                "key": [
                    {"dimension": "region", "value": "Asia"},
                    {"dimension": "stage_name", "value": "Procurement"},
                ],
                "serialized_key": "region:Asia|stage_name:Procurement",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                },
                "impact": 0,
                "change_percentage": 0.0,
                "change_dev": 0.0,
                "absolute_contribution": -0.035714285714285726,
                "confidence": None,
                "sort_value": 0,
            },
        ],
        "dimension_slices_permutation_Keys": [
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
        "evaluation_date_range": ["2025-03-01", "2025-03-30"],
        "comparison_date_range": ["2024-03-01", "2024-03-30"],
        "dimensions": [
            {"name": "region", "score": 0.6888888888888889, "is_key_dimension": True},
            {"name": "stage_name", "score": 0.9962623591866413, "is_key_dimension": True},
        ],
        "key_dimensions": ["region", "stage_name"],
        "filters": [],
        "dimension_slices": [
            {
                "key": [{"dimension": "region", "value": "Asia"}],
                "serialized_key": "region:Asia",
                "evaluation_value": {
                    "slice_count": 4,
                    "slice_size": 0.5714285714285714,
                    "slice_value": 1000,
                },
                "comparison_value": {
                    "slice_count": 4,
                    "slice_size": 0.5714285714285714,
                    "slice_value": 2100,
                },
                "impact": 1100,
                "change_percentage": 1.1,
                "change_dev": 0.8570032134790982,
                "absolute_contribution": 0.4920634920634921,
                "confidence": 0.49999999999999956,
                "sort_value": 1100,
                "relative_change": 88.57142857142858,
                "pressure": "UPWARD",
            },
            {
                "key": [{"dimension": "stage_name", "value": "Won"}],
                "serialized_key": "stage_name:Won",
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 600,
                },
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1700,
                },
                "impact": 1100,
                "change_percentage": 1.8333333333333333,
                "change_dev": 1.2303091986141086,
                "absolute_contribution": 0.44155844155844154,
                "confidence": None,
                "sort_value": 1100,
                "relative_change": 161.9047619047619,
                "pressure": "UPWARD",
            },
            {
                "key": [
                    {"dimension": "region", "value": "Asia"},
                    {"dimension": "stage_name", "value": "Won"},
                ],
                "serialized_key": "region:Asia|stage_name:Won",
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 600,
                },
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1700,
                },
                "impact": 1100,
                "change_percentage": 1.8333333333333333,
                "change_dev": 1.2303091986141086,
                "absolute_contribution": 0.44155844155844154,
                "confidence": None,
                "sort_value": 1100,
                "relative_change": 161.9047619047619,
                "pressure": "UPWARD",
            },
            {
                "key": [{"dimension": "region", "value": "EMEA"}],
                "serialized_key": "region:EMEA",
                "evaluation_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1800,
                },
                "comparison_value": {
                    "slice_count": 3,
                    "slice_size": 0.42857142857142855,
                    "slice_value": 1300,
                },
                "impact": -500,
                "change_percentage": -0.2777777777777778,
                "change_dev": 0.21641495289876214,
                "absolute_contribution": -0.8857142857142858,
                "confidence": 0.16687067367945163,
                "sort_value": 500,
                "relative_change": -49.20634920634921,
                "pressure": "DOWNWARD",
            },
            {
                "key": [{"dimension": "stage_name", "value": "Sale"}],
                "serialized_key": "stage_name:Sale",
                "evaluation_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1300,
                },
                "comparison_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1000,
                },
                "impact": -300,
                "change_percentage": -0.23076923076923078,
                "change_dev": 0.15486409493044723,
                "absolute_contribution": -0.3857142857142857,
                "confidence": None,
                "sort_value": 300,
                "relative_change": -44.505494505494504,
                "pressure": "DOWNWARD",
            },
            {
                "key": [
                    {"dimension": "region", "value": "EMEA"},
                    {"dimension": "stage_name", "value": "Sale"},
                ],
                "serialized_key": "region:EMEA|stage_name:Sale",
                "evaluation_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1300,
                },
                "comparison_value": {
                    "slice_count": 2,
                    "slice_size": 0.2857142857142857,
                    "slice_value": 1000,
                },
                "impact": -300,
                "change_percentage": -0.23076923076923078,
                "change_dev": 0.15486409493044723,
                "absolute_contribution": -0.3857142857142857,
                "confidence": None,
                "sort_value": 300,
                "relative_change": -44.505494505494504,
                "pressure": "DOWNWARD",
            },
            {
                "key": [{"dimension": "stage_name", "value": "Lost"}],
                "serialized_key": "stage_name:Lost",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 500,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 300,
                },
                "impact": -200,
                "change_percentage": -0.4,
                "change_dev": 0.15831202465660632,
                "absolute_contribution": -0.13354037267080746,
                "confidence": None,
                "sort_value": 200,
                "relative_change": -61.42857142857143,
                "pressure": "DOWNWARD",
            },
            {
                "key": [
                    {"dimension": "region", "value": "EMEA"},
                    {"dimension": "stage_name", "value": "Lost"},
                ],
                "serialized_key": "region:EMEA|stage_name:Lost",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 500,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 300,
                },
                "impact": -200,
                "change_percentage": -0.4,
                "change_dev": 0.15831202465660632,
                "absolute_contribution": -0.13354037267080746,
                "confidence": None,
                "sort_value": 200,
                "relative_change": -61.42857142857143,
                "pressure": "DOWNWARD",
            },
            {
                "key": [{"dimension": "stage_name", "value": "Procurement"}],
                "serialized_key": "stage_name:Procurement",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                },
                "impact": 0,
                "change_percentage": 0.0,
                "change_dev": 0.0,
                "absolute_contribution": -0.035714285714285726,
                "confidence": None,
                "sort_value": 0,
                "relative_change": -21.428571428571427,
                "pressure": "DOWNWARD",
            },
            {
                "key": [
                    {"dimension": "region", "value": "Asia"},
                    {"dimension": "stage_name", "value": "Procurement"},
                ],
                "serialized_key": "region:Asia|stage_name:Procurement",
                "evaluation_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                },
                "comparison_value": {
                    "slice_count": 1,
                    "slice_size": 0.14285714285714285,
                    "slice_value": 400,
                },
                "impact": 0,
                "change_percentage": 0.0,
                "change_dev": 0.0,
                "absolute_contribution": -0.035714285714285726,
                "confidence": None,
                "sort_value": 0,
                "relative_change": -21.428571428571427,
                "pressure": "DOWNWARD",
            },
        ],
        "dimension_slices_permutation_Keys": [
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
