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
    df["date"] = pd.to_datetime(df["date"])
    sorted_df = df.sort_values(by="value", ascending=False).reset_index(drop=True)
    sorted_df.index += 1
    sorted_df.index.name = "rank"
    return sorted_df


@pytest.fixture
def targets_df():
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
        "target": [10, 15, 20, 10, 15, 20, 10, 15, 20, 10, 20, 40],
    }
    df = pd.DataFrame(data)
    return df


@pytest.fixture
def metric_details():
    return {
        "id": "NewBizDeals",
        "label": "New Business Deals",
        "abbreviation": "NewBizDeals",
        "definition": "The count of New Business Opportunities that were generated in the period related to closing "
        "deals.",
        "unit_of_measure": "Quantity",
        "unit": "n",
        "complexity": "Complex",
        "components": [],
        "terms": ["New Business Opportunities"],
        "metric_expression": {
            "type": "metric",
            "metric_id": "NewBizDeals",
            "period": 0,
            "expression_str": "{AcceptOpps} * {SQOToWinRate}",
            "expression": {
                "type": "expression",
                "operator": "*",
                "operands": [
                    {
                        "type": "metric",
                        "metric_id": "AcceptOpps",
                        "period": 0,
                        "expression_str": None,
                        "expression": None,
                    },
                    {
                        "type": "metric",
                        "metric_id": "SQOToWinRate",
                        "period": 0,
                        "expression_str": None,
                        "expression": None,
                    },
                ],
            },
        },
        "grain_aggregation": "Week",
        "metadata": {
            "semantic_meta": {
                "cube": "dim_opportunity",
                "member": "new_biz_deals",
                "member_type": "measure",
                "time_dimension": {"cube": "dim_opportunity", "member": "close_date"},
            }
        },
        "output_of": [],
        "input_to": [],
        "influences": [],
        "influenced_by": [],
        "periods": ["Week", "Month", "Quarter"],
        "aggregations": ["Sum"],
        "owned_by_team": ["Sales"],
        "dimensions": [
            {
                "dimension_id": "account_region",
                "label": "Account Region",
                "metadata": {
                    "semantic_meta": {"cube": "dim_opportunity", "member": "account_region", "member_type": "dimension"}
                },
            },
            {
                "dimension_id": "account_segment",
                "label": "Account Segment",
                "metadata": {
                    "semantic_meta": {
                        "cube": "dim_opportunity",
                        "member": "account_segment",
                        "member_type": "dimension",
                    }
                },
            },
            {
                "dimension_id": "account_territory",
                "label": "Account Territory",
                "metadata": {
                    "semantic_meta": {
                        "cube": "dim_opportunity",
                        "member": "account_territory",
                        "member_type": "dimension",
                    }
                },
            },
            {
                "dimension_id": "billing_plan",
                "label": "Billing Plan",
                "metadata": {
                    "semantic_meta": {"cube": "dim_opportunity", "member": "billing_plan", "member_type": "dimension"}
                },
            },
            {
                "dimension_id": "billing_term",
                "label": "Billing Term",
                "metadata": {
                    "semantic_meta": {"cube": "dim_opportunity", "member": "billing_term", "member_type": "dimension"}
                },
            },
            {
                "dimension_id": "contract_duration_months",
                "label": "Contract Duration Months",
                "metadata": {
                    "semantic_meta": {
                        "cube": "dim_opportunity",
                        "member": "contract_duration_months",
                        "member_type": "dimension",
                    }
                },
            },
            {
                "dimension_id": "customer_success_manager_name",
                "label": "Customer Success Manager Name",
                "metadata": {
                    "semantic_meta": {
                        "cube": "dim_opportunity",
                        "member": "customer_success_manager_name",
                        "member_type": "dimension",
                    }
                },
            },
            {
                "dimension_id": "forecast_category",
                "label": "Forecast Category",
                "metadata": {
                    "semantic_meta": {
                        "cube": "dim_opportunity",
                        "member": "forecast_category",
                        "member_type": "dimension",
                    }
                },
            },
            {
                "dimension_id": "lead_source",
                "label": "Lead Source",
                "metadata": {
                    "semantic_meta": {"cube": "dim_opportunity", "member": "lead_source", "member_type": "dimension"}
                },
            },
            {
                "dimension_id": "loss_reason",
                "label": "Loss Reason",
                "metadata": {
                    "semantic_meta": {"cube": "dim_opportunity", "member": "loss_reason", "member_type": "dimension"}
                },
            },
            {
                "dimension_id": "opportunity_source",
                "label": "Opportunity Source",
                "metadata": {
                    "semantic_meta": {
                        "cube": "dim_opportunity",
                        "member": "opportunity_source",
                        "member_type": "dimension",
                    }
                },
            },
        ],
    }
