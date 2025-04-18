from datetime import date, datetime
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from commons.models.enums import Granularity
from fulcrum_core import AnalysisManager
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType


@pytest.fixture
def mock_query_service():
    query_service = AsyncMock()
    query_service.get_metric_values.return_value = [
        {"date": "2023-01-01", "value": 100},
        {"date": "2023-01-02", "value": 200},
        {"date": "2023-01-03", "value": 300},
    ]
    query_service.get_metric_time_series.return_value = [
        {"date": "2023-01-01", "value": 100},
        {"date": "2023-01-02", "value": 200},
        {"date": "2023-01-03", "value": 300},
    ]
    query_service.list_metrics.return_value = [
        {"id": 1, "name": "metric1", "metric_id": "metric1"},
        {"id": 2, "name": "metric2", "metric_id": "metric2"},
        {"id": 3, "name": "metric3", "metric_id": "metric3"},
    ]
    return query_service


@pytest.fixture
def mock_analysis_service():
    return AsyncMock()


@pytest.fixture
def mock_analysis_manager():
    return AnalysisManager()


@pytest.fixture
def mock_db_session():
    return AsyncMock()


@pytest.fixture
def mock_story_date():
    return date(2023, 4, 17)


@pytest.fixture
def mock_stories():
    stories = [
        {
            "metric_id": "metric_1",
            "genre": StoryGenre.GROWTH,
            "story_group": StoryGroup.GROWTH_RATES,
            "story_type": StoryType.ACCELERATING_GROWTH,
            "grain": Granularity.DAY,
            "series": [{"date": "2023-01-01", "value": 100}],
            "title": "Title 1",
            "detail": "Detail 1",
            "title_template": "Title Template 1",
            "detail_template": "Detail Template 1",
            "variables": {"key": "value"},
            "story_date": datetime(2023, 1, 1),
        },
        {
            "metric_id": "metric_2",
            "genre": StoryGenre.GROWTH,
            "story_group": StoryGroup.GROWTH_RATES,
            "story_type": StoryType.ACCELERATING_GROWTH,
            "grain": Granularity.DAY,
            "series": [{"date": "2023-01-02", "value": 200}],
            "title": "Title 2",
            "detail": "Detail 2",
            "title_template": "Title Template 2",
            "detail_template": "Detail Template 2",
            "variables": {"key": "value"},
            "story_date": datetime(2023, 1, 2),
        },
    ]
    return stories


@pytest.fixture
def segment_drift_data():
    return {
        "name": "SUM value",
        "total_segments": 3268,
        "expected_change_percentage": 0,
        "aggregation_method": "SUM",
        "evaluation_num_rows": 284,
        "comparison_num_rows": 311,
        "evaluation_value": 30,
        "comparison_value": 106,
        "evaluation_date_range": ["2024-02-05", "2024-02-25"],
        "comparison_date_range": ["2024-01-15", "2024-02-04"],
        "dimensions": [
            {"name": "account_region", "score": 3.1070523976023563, "is_key_dimension": True},
            {"name": "loss_reason", "score": 0.2105206684519219, "is_key_dimension": True},
            {"name": "opportunity_source", "score": 1.1869760275565782, "is_key_dimension": True},
            {"name": "billing_term", "score": 1.7353564295891097, "is_key_dimension": True},
            {"name": "forecast_category", "score": 0.0, "is_key_dimension": False},
            {"name": "account_territory", "score": 0.5296295848198944, "is_key_dimension": True},
            {"name": "billing_plan", "score": 0.3122740121942086, "is_key_dimension": True},
            {"name": "contract_duration_months", "score": 1.4302242405706989, "is_key_dimension": True},
            {"name": "account_segment", "score": 0.3985314427755724, "is_key_dimension": True},
            {"name": "customer_success_manager_name", "score": 1.6068499546007227, "is_key_dimension": True},
            {"name": "lead_source", "score": 0.2571842350857151, "is_key_dimension": True},
        ],
        "key_dimensions": [
            "account_region",
            "loss_reason",
            "opportunity_source",
            "billing_term",
            "account_territory",
            "billing_plan",
            "contract_duration_months",
            "account_segment",
            "customer_success_manager_name",
            "lead_source",
        ],
        "filters": [],
        "id": "value_SUM",
        "dimension_slices": [
            {
                "key": [{"dimension": "billing_plan", "value": "Enterprise"}],
                "serialized_key": "billing_plan:Enterprise",
                "evaluation_value": {
                    "slice_count": 259,
                    "slice_size": 0.9119718309859155,
                    "slice_value": 29,
                    "slice_share": 96.66666666666667,
                },
                "comparison_value": {
                    "slice_count": 287,
                    "slice_size": 0.9228295819935691,
                    "slice_value": 106,
                    "slice_share": 100.0,
                },
                "impact": 77,
                "change_percentage": 2.6551724137931036,
                "change_dev": 1.4728997933428938,
                "absolute_contribution": 3.533333333333333,
                "confidence": -1.0,
                "sort_value": 77,
                "slice_share_change_percentage": -3.3333333333333286,
                "relative_change": -12.183908045977043,
                "pressure": "UPWARD",
            },
            {
                "key": [{"dimension": "billing_plan", "value": "Partnership"}],
                "serialized_key": "billing_plan:Partnership",
                "evaluation_value": {
                    "slice_count": 25,
                    "slice_size": 0.0880281690140845,
                    "slice_value": 1,
                    "slice_share": 3.3333333333333335,
                },
                "comparison_value": {
                    "slice_count": 24,
                    "slice_size": 0.07717041800643087,
                    "slice_value": 0,
                    "slice_share": 0.0,
                },
                "impact": -1,
                "change_percentage": -1.0,
                "change_dev": 0.04774342705259216,
                "absolute_contribution": -0.12183908045977043,
                "confidence": -1.0,
                "sort_value": 1,
                "slice_share_change_percentage": 3.3333333333333335,
                "relative_change": 353.3333333333333,
                "pressure": "DOWNWARD",
            },
        ],
    }


@pytest.fixture
def segment_drift_stories():
    return [
        {
            "metric_id": "NewBizDeals",
            "genre": StoryGenre.ROOT_CAUSES,
            "story_group": StoryGroup.SEGMENT_DRIFT,
            "story_type": StoryType.SHRINKING_SEGMENT,
            "story_date": date(2024, 3, 1),
            "grain": Granularity.WEEK,
            "series": [
                {
                    "previous_share": 100,
                    "current_share": 97,
                    "dimension": "Billing Plan",
                    "slice": "Enterprise",
                    "slice_share_change_percentage": -3,
                    "pressure_direction": "upward",
                    "previous_value": 106,
                    "current_value": 29,
                    "deviation": -73,
                    "pressure_change": 3,
                    "impact": 77,
                    "sort_value": 77,
                    "serialized_key": "billing_plan:Enterprise",
                }
            ],
            "title": "Key Driver: Falling Enterprise share of Billing Plan",
            "detail": "For New Business Deals, the share of Billing Plan that is Enterprise has "
            "decreased from 100% to 97% over the past week. This decrease contributed -3% upward"
            " pressure on New Business Deals.",
            "title_template": "Key Driver: Falling {{slice | default('null')}} share of " "{{dimension}}",
            "detail_template": "For {{metric.label}}, the share of {{dimension}} that is "
            "{{slice | default('null')}} has decreased from {{previous_share}}% to "
            "{{current_share}}% over the past {{grain}}. This decrease contributed "
            "{{slice_share_change_percentage}}% {{pressure_direction}} pressure on {{metric.label}}.",
            "variables": {
                "grain": "week",
                "eoi": "EOW",
                "metric": {"id": "NewBizDeals", "label": "New Business Deals"},
                "pop": "w/w",
                "interval": "weekly",
                "previous_share": 100,
                "current_share": 97,
                "dimension": "Billing Plan",
                "slice": "Enterprise",
                "slice_share_change_percentage": -3,
                "pressure_direction": "upward",
                "previous_value": 106,
                "current_value": 29,
                "deviation": -73,
                "pressure_change": 3,
                "impact": 77,
                "sort_value": 77,
                "serialized_key": "billing_plan:Enterprise",
            },
        },
        {
            "metric_id": "NewBizDeals",
            "genre": StoryGenre.ROOT_CAUSES,
            "story_group": StoryGroup.SEGMENT_DRIFT,
            "story_type": StoryType.IMPROVING_SEGMENT,
            "story_date": date(2024, 3, 1),
            "grain": Granularity.WEEK,
            "series": [
                {
                    "previous_share": 100,
                    "current_share": 97,
                    "dimension": "Billing Plan",
                    "slice": "Enterprise",
                    "slice_share_change_percentage": -3,
                    "pressure_direction": "upward",
                    "previous_value": 106,
                    "current_value": 29,
                    "deviation": -73,
                    "pressure_change": 3,
                    "impact": 77,
                    "sort_value": 77,
                    "serialized_key": "billing_plan:Enterprise",
                }
            ],
            "title": "Key Driver: Stronger Enterprise segment",
            "detail": "Over the past week, when Billing Plan is Enterprise, "
            "New Business Deals is 29. This is an increase of "
            "-73% relative to the prior week, and this increase contributed 3% upward pressure on New Business Deals.",
            "title_template": "Key Driver: Stronger {{slice | default('null')}} segment",
            "detail_template": "Over the past {{grain}}, when {{dimension}} is {{slice | "
            "default('null')}}, {{metric.label}} is {{current_value}}. This is an increase of "
            "{{deviation}}% relative to the prior {{grain}}, and this increase contributed "
            "{{pressure_change}}% {{pressure_direction}} pressure on {{metric.label}}.",
            "variables": {
                "grain": "week",
                "eoi": "EOW",
                "metric": {"id": "NewBizDeals", "label": "New Business Deals"},
                "pop": "w/w",
                "interval": "weekly",
                "previous_share": 100,
                "current_share": 97,
                "dimension": "Billing Plan",
                "slice": "Enterprise",
                "slice_share_change_percentage": -3,
                "pressure_direction": "upward",
                "previous_value": 106,
                "current_value": 29,
                "deviation": -73,
                "pressure_change": 3,
                "impact": 77,
                "sort_value": 77,
                "serialized_key": "billing_plan:Enterprise",
            },
        },
        {
            "metric_id": "NewBizDeals",
            "genre": StoryGenre.ROOT_CAUSES,
            "story_group": StoryGroup.SEGMENT_DRIFT,
            "story_type": StoryType.GROWING_SEGMENT,
            "story_date": date(2024, 3, 1),
            "grain": Granularity.WEEK,
            "series": [
                {
                    "previous_share": 0,
                    "current_share": 3,
                    "dimension": "Billing Plan",
                    "slice": "Partnership",
                    "slice_share_change_percentage": 3,
                    "pressure_direction": "downward",
                    "previous_value": 0,
                    "current_value": 1,
                    "deviation": 0,
                    "pressure_change": -1,
                    "impact": -1,
                    "sort_value": 1,
                    "serialized_key": "billing_plan:Partnership",
                }
            ],
            "title": "Key Driver: Growing Partnership share of Billing Plan",
            "detail": "The share of Billing Plan that is Partnership increased from 0% to 3% over the "
            "past week. This increase contributed 3% downward pressure on New Business Deals.",
            "title_template": "Key Driver: Growing {{slice | default('null')}} share of {{dimension}}",
            "detail_template": "The share of {{dimension}} that is {{slice | default('null')}} "
            "increased from {{previous_share}}% to {{current_share}}% over the past {{grain}}. "
            "This increase contributed {{slice_share_change_percentage}}% {{pressure_direction}} "
            "pressure on {{metric.label}}.",
            "variables": {
                "grain": "week",
                "eoi": "EOW",
                "metric": {"id": "NewBizDeals", "label": "New Business Deals"},
                "pop": "w/w",
                "interval": "weekly",
                "previous_share": 0,
                "current_share": 3,
                "dimension": "Billing Plan",
                "slice": "Partnership",
                "slice_share_change_percentage": 3,
                "pressure_direction": "downward",
                "previous_value": 0,
                "current_value": 1,
                "deviation": 0,
                "pressure_change": -1,
                "impact": -1,
                "sort_value": 1,
                "serialized_key": "billing_plan:Partnership",
            },
        },
        {
            "metric_id": "NewBizDeals",
            "genre": StoryGenre.ROOT_CAUSES,
            "story_group": StoryGroup.SEGMENT_DRIFT,
            "story_type": StoryType.WORSENING_SEGMENT,
            "story_date": date(2024, 3, 1),
            "grain": Granularity.WEEK,
            "series": [
                {
                    "previous_share": 0,
                    "current_share": 3,
                    "dimension": "Billing Plan",
                    "slice": "Partnership",
                    "slice_share_change_percentage": 3,
                    "pressure_direction": "downward",
                    "previous_value": 0,
                    "current_value": 1,
                    "deviation": 0,
                    "pressure_change": -1,
                    "impact": -1,
                    "sort_value": 1,
                    "serialized_key": "billing_plan:Partnership",
                }
            ],
            "title": "Key Driver: Weaker Partnership segment",
            "detail": "Over the past week, when Billing Plan is Partnership, "
            "New Business Deals is 1. This is a decrease of 0% relative to the prior week, and this decrease "
            "contributed -1% downward pressure on New Business Deals.",
            "title_template": "Key Driver: Weaker {{slice}} segment",
            "detail_template": "Over the past {{grain}}, when {{dimension}} is {{slice | "
            "default('null')}}, {{metric.label}} is {{current_value}}. This is a decrease of {{deviation}}%"
            " relative to the prior {{grain}}, and this decrease contributed {{pressure_change}}% "
            "{{pressure_direction}} pressure on {{metric.label}}.",
            "variables": {
                "grain": "week",
                "eoi": "EOW",
                "metric": {"id": "NewBizDeals", "label": "New Business Deals"},
                "pop": "w/w",
                "interval": "weekly",
                "previous_share": 0,
                "current_share": 3,
                "dimension": "Billing Plan",
                "slice": "Partnership",
                "slice_share_change_percentage": 3,
                "pressure_direction": "downward",
                "previous_value": 0,
                "current_value": 1,
                "deviation": 0,
                "pressure_change": -1,
                "impact": -1,
                "sort_value": 1,
                "serialized_key": "billing_plan:Partnership",
            },
        },
    ]


@pytest.fixture
def mock_segment_drift_query_service(metric_details):
    query_service = AsyncMock()
    query_service.get_metric.return_value = metric_details
    return query_service


@pytest.fixture
def dimension_slice_data():
    return {
        "account_region": [
            {"metric_id": "NewBizDeals", "value": 4, "date": None, "account_region": None},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "account_region": "US-West"},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "account_region": "US-East"},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "account_region": "EMEA"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "account_region": "APAC"},
        ],
        "account_segment": [
            {"metric_id": "NewBizDeals", "value": 4, "date": None, "account_segment": "Mid Market"},
            {"metric_id": "NewBizDeals", "value": 3, "date": None, "account_segment": "Enterprise"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "account_segment": None},
        ],
        "account_territory": [
            {"metric_id": "NewBizDeals", "value": 4, "date": None, "account_territory": "EMEA"},
            {"metric_id": "NewBizDeals", "value": 3, "date": None, "account_territory": "AMER"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "account_territory": "APAC"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "account_territory": None},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "account_territory": "LATAM"},
        ],
        "billing_plan": [
            {"metric_id": "NewBizDeals", "value": 6, "date": None, "billing_plan": "Enterprise"},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "billing_plan": "Partnership"},
        ],
        "billing_term": [
            {"metric_id": "NewBizDeals", "value": 5, "date": None, "billing_term": "Annual"},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "billing_term": "Semiannual"},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "billing_term": "Monthly"},
        ],
        "contract_duration_months": [
            {"metric_id": "NewBizDeals", "value": 3, "date": None, "contract_duration_months": "12"},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "contract_duration_months": "24"},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "contract_duration_months": None},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "contract_duration_months": "36"},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "contract_duration_months": "39"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "contract_duration_months": "29"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "contract_duration_months": "6"},
        ],
        "customer_success_manager_name": [
            {"metric_id": "NewBizDeals", "value": 2, "date": None, "customer_success_manager_name": None},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "customer_success_manager_name": "Jon Kay"},
            {
                "metric_id": "NewBizDeals",
                "value": 1,
                "date": None,
                "customer_success_manager_name": "Christina Kopecky",
            },
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "customer_success_manager_name": "Brian Shanley"},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "customer_success_manager_name": "Matt Cleghorn"},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "customer_success_manager_name": "Paul Staelin"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "customer_success_manager_name": "Nathan Kuhn"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "customer_success_manager_name": "Christine Durham"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "customer_success_manager_name": "Nicolas Ishihara"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "customer_success_manager_name": "Mathias Wetzel"},
        ],
        "forecast_category": [
            {"metric_id": "NewBizDeals", "value": 7, "date": None, "forecast_category": "Closed"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "forecast_category": "Omitted"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "forecast_category": "Commit"},
        ],
        "lead_source": [
            {"metric_id": "NewBizDeals", "value": 7, "date": None, "lead_source": None},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "lead_source": "Inbound Request"},
        ],
        "loss_reason": [
            {"metric_id": "NewBizDeals", "value": 7, "date": None, "loss_reason": None},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "loss_reason": "No Decision / Non-Responsive"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "loss_reason": "Price"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "loss_reason": "No Budget / Lost Funding"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "loss_reason": "Churn"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "loss_reason": "Staying on Pro"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "loss_reason": "Needs Nurturing"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "loss_reason": "Lost to Competitor"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "loss_reason": "Contract terms"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "loss_reason": "Other"},
        ],
        "opportunity_source": [
            {"metric_id": "NewBizDeals", "value": 3, "date": None, "opportunity_source": "Self-prospected"},
            {"metric_id": "NewBizDeals", "value": 2, "date": None, "opportunity_source": None},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "opportunity_source": "Renewal"},
            {"metric_id": "NewBizDeals", "value": 1, "date": None, "opportunity_source": "Partner"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "opportunity_source": "Inbound Request"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "opportunity_source": "SDR"},
            {"metric_id": "NewBizDeals", "value": 0, "date": None, "opportunity_source": "CSM Identified"},
        ],
    }


@pytest.fixture
def significant_segment_stories():
    return [
        {
            "metric_id": "NewBizDeals",
            "genre": StoryGenre.PERFORMANCE,
            "story_group": StoryGroup.SIGNIFICANT_SEGMENTS,
            "story_type": StoryType.TOP_4_SEGMENTS,
            "story_date": date(2024, 3, 1),
            "grain": Granularity.WEEK,
            "series": [
                {"value": 7, "member": None, "dimension_label": "Lead Source", "dimension_id": "lead_source"},
                {
                    "value": 7,
                    "member": "Closed",
                    "dimension_label": "Forecast Category",
                    "dimension_id": "forecast_category",
                },
                {"value": 7, "member": None, "dimension_label": "Loss Reason", "dimension_id": "loss_reason"},
                {"value": 6, "member": "Enterprise", "dimension_label": "Billing Plan", "dimension_id": "billing_plan"},
            ],
            "title": "Prior week best performing segments",
            "detail": "The segments below had the highest average values for New Business Deals over the past week",
            "title_template": "Prior {{grain}} best performing segments",
            "detail_template": "The segments below had the highest average values for {{metric.label}} over the past "
            "{{grain}}",
            "variables": {
                "grain": "week",
                "eoi": "EOW",
                "metric": {"id": "NewBizDeals", "label": "New Business Deals"},
                "pop": "w/w",
                "interval": "weekly",
                "average": 1,
            },
        },
        {
            "metric_id": "NewBizDeals",
            "genre": StoryGenre.PERFORMANCE,
            "story_group": StoryGroup.SIGNIFICANT_SEGMENTS,
            "story_type": StoryType.BOTTOM_4_SEGMENTS,
            "story_date": date(2024, 3, 1),
            "grain": Granularity.WEEK,
            "series": [
                {
                    "value": 0,
                    "member": "Nicolas Ishihara",
                    "dimension_label": "Customer Success Manager Name",
                    "dimension_id": "customer_success_manager_name",
                },
                {
                    "value": 0,
                    "member": "Christine Durham",
                    "dimension_label": "Customer Success Manager Name",
                    "dimension_id": "customer_success_manager_name",
                },
                {
                    "value": 0,
                    "member": "Nathan Kuhn",
                    "dimension_label": "Customer Success Manager Name",
                    "dimension_id": "customer_success_manager_name",
                },
                {
                    "value": 0,
                    "member": "CSM Identified",
                    "dimension_label": "Opportunity Source",
                    "dimension_id": "opportunity_source",
                },
            ],
            "title": "Prior week worst performing segments",
            "detail": "The segments below had the lowest average values for New Business Deals over the past week",
            "title_template": "Prior {{grain}} worst performing segments",
            "detail_template": "The segments below had the lowest average values for {{metric.label}} over the past "
            "{{grain}}",
            "variables": {
                "grain": "week",
                "eoi": "EOW",
                "metric": {"id": "NewBizDeals", "label": "New Business Deals"},
                "pop": "w/w",
                "interval": "weekly",
                "average": 1,
            },
        },
    ]


@pytest.fixture
def mock_get_dimension_slice_data(significant_segment_story_builder, mocker, dimension_slice_data):
    async def get_dimension_slice_data(metric_id, start_date, end_date, dimensions):
        return pd.DataFrame(dimension_slice_data[dimensions[0]])

    mocker.patch.object(
        significant_segment_story_builder.query_service,
        "get_metric_values_df",
        AsyncMock(side_effect=get_dimension_slice_data),
    )
