from datetime import date, datetime
from unittest.mock import AsyncMock

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
        {"id": 1, "name": "metric1"},
        {"id": 2, "name": "metric2"},
        {"id": 3, "name": "metric3"},
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
            "genre": "ROOT_CAUSES",
            "story_group": "SEGMENT_DRIFT",
            "story_type": "SHRINKING_SEGMENT",
            "story_date": date(2024, 6, 17),
            "grain": "week",
            "series": [
                {
                    "previous_share": 100.0,
                    "current_share": 96.67,
                    "dimension": "Billing Plan",
                    "slice_name": "Enterprise",
                    "slice_share_change_percentage": -3.33,
                    "pressure_direction": "upward",
                    "previous_value": 106,
                    "current_value": 29,
                    "deviation": -72.64,
                    "pressure_change": 2.66,
                    "impact": 77,
                    "sort_value": 77,
                    "serialized_key": "billing_plan:Enterprise",
                }
            ],
            "title": "Key Driver: Falling null share of Billing Plan",
            "detail": "For New Business Deals, the share of Billing Plan that is null has decreased from 100.0% to "
            "96.67% over the past week. This decrease contributed -3.33% upward pressure on New Business "
            "Deals.",
            "title_template": "Key Driver: Falling {{slice | default('null')}} share of {{dimension}}",
            "detail_template": "For {{metric.label}}, the share of {{dimension}} that is {{slice | default('null')}} "
            "has decreased from {{previous_share}}% to {{current_share}}% over the past {{grain}}. "
            "This decrease contributed {{slice_share_change_percentage}}% {{pressure_direction}} "
            "pressure on {{metric.label}}.",
            "variables": {
                "grain": "week",
                "eoi": "EOW",
                "metric": {"id": "NewBizDeals", "label": "New Business Deals"},
                "pop": "w/w",
                "interval": "weekly",
                "previous_share": 100.0,
                "current_share": 96.67,
                "dimension": "Billing Plan",
                "slice_name": "Enterprise",
                "slice_share_change_percentage": -3.33,
                "pressure_direction": "upward",
                "previous_value": 106,
                "current_value": 29,
                "deviation": -72.64,
                "pressure_change": 2.66,
                "impact": 77,
                "sort_value": 77,
                "serialized_key": "billing_plan:Enterprise",
            },
        },
        {
            "metric_id": "NewBizDeals",
            "genre": "ROOT_CAUSES",
            "story_group": "SEGMENT_DRIFT",
            "story_type": "IMPROVING_SEGMENT",
            "story_date": date(2024, 6, 17),
            "grain": "week",
            "series": [
                {
                    "previous_share": 100.0,
                    "current_share": 96.67,
                    "dimension": "Billing Plan",
                    "slice_name": "Enterprise",
                    "slice_share_change_percentage": -3.33,
                    "pressure_direction": "upward",
                    "previous_value": 106,
                    "current_value": 29,
                    "deviation": -72.64,
                    "pressure_change": 2.66,
                    "impact": 77,
                    "sort_value": 77,
                    "serialized_key": "billing_plan:Enterprise",
                }
            ],
            "title": "Key Driver: Stronger null segment",
            "detail": "Over the past week, when Billing Plan is null, New Business Deals is 29. This is an increase of "
            "-72.64% relative to the prior week, and this increase contributed 2.66% upward pressure on New "
            "Business Deals.",
            "title_template": "Key Driver: Stronger {{slice | default('null')}} segment",
            "detail_template": "Over the past {{grain}}, when {{dimension}} is {{slice | default('null')}}, "
            "{{metric.label}} is {{current_value}}. This is an increase of {{deviation}}% relative "
            "to the prior {{grain}}, and this increase contributed {{pressure_change}}% "
            "{{pressure_direction}} pressure on {{metric.label}}.",
            "variables": {
                "grain": "week",
                "eoi": "EOW",
                "metric": {"id": "NewBizDeals", "label": "New Business Deals"},
                "pop": "w/w",
                "interval": "weekly",
                "previous_share": 100.0,
                "current_share": 96.67,
                "dimension": "Billing Plan",
                "slice_name": "Enterprise",
                "slice_share_change_percentage": -3.33,
                "pressure_direction": "upward",
                "previous_value": 106,
                "current_value": 29,
                "deviation": -72.64,
                "pressure_change": 2.66,
                "impact": 77,
                "sort_value": 77,
                "serialized_key": "billing_plan:Enterprise",
            },
        },
        {
            "metric_id": "NewBizDeals",
            "genre": "ROOT_CAUSES",
            "story_group": "SEGMENT_DRIFT",
            "story_type": "GROWING_SEGMENT",
            "story_date": date(2024, 6, 17),
            "grain": "week",
            "series": [
                {
                    "previous_share": 0.0,
                    "current_share": 3.33,
                    "dimension": "Billing Plan",
                    "slice_name": "Partnership",
                    "slice_share_change_percentage": 3.33,
                    "pressure_direction": "downward",
                    "previous_value": 0,
                    "current_value": 1,
                    "deviation": 0.0,
                    "pressure_change": -1.0,
                    "impact": -1,
                    "sort_value": 1,
                    "serialized_key": "billing_plan:Partnership",
                }
            ],
            "title": "Key Driver: Growing null share of Billing Plan",
            "detail": "The share of Billing Plan that is null increased from 0.0% to 3.33% over the past week. This "
            "increase contributed 3.33% downward pressure on New Business Deals.",
            "title_template": "Key Driver: Growing {{slice | default('null')}} share of {{dimension}}",
            "detail_template": "The share of {{dimension}} that is {{slice | default('null')}} increased from "
            "{previous_share}}% to {{current_share}}% over the past {{grain}}. This increase "
            "contributed {{slice_share_change_percentage}}% {{pressure_direction}} pressure on "
            "{{metric.label}}.",
            "variables": {
                "grain": "week",
                "eoi": "EOW",
                "metric": {"id": "NewBizDeals", "label": "New Business Deals"},
                "pop": "w/w",
                "interval": "weekly",
                "previous_share": 0.0,
                "current_share": 3.33,
                "dimension": "Billing Plan",
                "slice_name": "Partnership",
                "slice_share_change_percentage": 3.33,
                "pressure_direction": "downward",
                "previous_value": 0,
                "current_value": 1,
                "deviation": 0.0,
                "pressure_change": -1.0,
                "impact": -1,
                "sort_value": 1,
                "serialized_key": "billing_plan:Partnership",
            },
        },
        {
            "metric_id": "NewBizDeals",
            "genre": "ROOT_CAUSES",
            "story_group": "SEGMENT_DRIFT",
            "story_type": "WORSENING_SEGMENT",
            "story_date": date(2024, 6, 17),
            "grain": "week",
            "series": [
                {
                    "previous_share": 0.0,
                    "current_share": 3.33,
                    "dimension": "Billing Plan",
                    "slice_name": "Partnership",
                    "slice_share_change_percentage": 3.33,
                    "pressure_direction": "downward",
                    "previous_value": 0,
                    "current_value": 1,
                    "deviation": 0.0,
                    "pressure_change": -1.0,
                    "impact": -1,
                    "sort_value": 1,
                    "serialized_key": "billing_plan:Partnership",
                }
            ],
            "title": "Key Driver: Weaker  segment",
            "detail": "Over the past week, when Billing Plan is null, New Business Deals is 1. This is a decrease of "
            "0.0% relative to the prior week, and this decrease contributed -1.0% downward pressure on New "
            "Business Deals.",
            "title_template": "Key Driver: Weaker {{slice}} segment",
            "detail_template": "Over the past {{grain}}, when {{dimension}} is {{slice | default('null')}}, "
            "{{metric.label}} is {{current_value}}. This is a decrease of {{deviation}}% relative "
            "to the prior {{grain}}, and this decrease contributed {{pressure_change}}% "
            "{{pressure_direction}} pressure on {{metric.label}}.",
            "variables": {
                "grain": "week",
                "eoi": "EOW",
                "metric": {"id": "NewBizDeals", "label": "New Business Deals"},
                "pop": "w/w",
                "interval": "weekly",
                "previous_share": 0.0,
                "current_share": 3.33,
                "dimension": "Billing Plan",
                "slice_name": "Partnership",
                "slice_share_change_percentage": 3.33,
                "pressure_direction": "downward",
                "previous_value": 0,
                "current_value": 1,
                "deviation": 0.0,
                "pressure_change": -1.0,
                "impact": -1,
                "sort_value": 1,
                "serialized_key": "billing_plan:Partnership",
            },
        },
    ]
