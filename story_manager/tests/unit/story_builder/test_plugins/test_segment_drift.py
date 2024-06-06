from datetime import date
from unittest.mock import AsyncMock

import pytest

from commons.models.enums import Granularity
from story_manager.story_builder.plugins.segment_drift import SegmentDriftStoryBuilder

start_date = date(2024, 3, 1)
# number_of_data_points = 90
metric_id = "NewBizDeals"
metric_details = {
    "id": "NewBizDeals",
    "label": "New Business Deals",
    "abbreviation": "NewBizDeals",
    "definition": "The count of New Business Opportunities that were generated in the period related to closing deals.",
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
                {"type": "metric", "metric_id": "AcceptOpps", "period": 0, "expression_str": None, "expression": None},
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
            "id": "account_region",
            "label": "Account Region",
            "metadata": {
                "semantic_meta": {"cube": "dim_opportunity", "member": "account_region", "member_type": "dimension"}
            },
        },
        {
            "id": "account_segment",
            "label": "Account Segment",
            "metadata": {
                "semantic_meta": {"cube": "dim_opportunity", "member": "account_segment", "member_type": "dimension"}
            },
        },
        {
            "id": "account_territory",
            "label": "Account Territory",
            "metadata": {
                "semantic_meta": {"cube": "dim_opportunity", "member": "account_territory", "member_type": "dimension"}
            },
        },
        {
            "id": "billing_plan",
            "label": "Billing Plan",
            "metadata": {
                "semantic_meta": {"cube": "dim_opportunity", "member": "billing_plan", "member_type": "dimension"}
            },
        },
        {
            "id": "billing_term",
            "label": "Billing Term",
            "metadata": {
                "semantic_meta": {"cube": "dim_opportunity", "member": "billing_term", "member_type": "dimension"}
            },
        },
        {
            "id": "contract_duration_months",
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
            "id": "customer_success_manager_name",
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
            "id": "forecast_category",
            "label": "Forecast Category",
            "metadata": {
                "semantic_meta": {"cube": "dim_opportunity", "member": "forecast_category", "member_type": "dimension"}
            },
        },
        {
            "id": "lead_source",
            "label": "Lead Source",
            "metadata": {
                "semantic_meta": {"cube": "dim_opportunity", "member": "lead_source", "member_type": "dimension"}
            },
        },
        {
            "id": "loss_reason",
            "label": "Loss Reason",
            "metadata": {
                "semantic_meta": {"cube": "dim_opportunity", "member": "loss_reason", "member_type": "dimension"}
            },
        },
        {
            "id": "opportunity_source",
            "label": "Opportunity Source",
            "metadata": {
                "semantic_meta": {"cube": "dim_opportunity", "member": "opportunity_source", "member_type": "dimension"}
            },
        },
    ],
}
grain = Granularity.WEEK


@pytest.fixture
def segment_drift_story_builder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session):
    return SegmentDriftStoryBuilder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.mark.asyncio
async def test_generate_stories_success(segment_drift_story_builder, segment_drift_stories, segment_drift_data):
    segment_drift_story_builder.query_service.get_metric = AsyncMock(return_value=metric_details)
    segment_drift_story_builder.analysis_service.get_segment_drift = AsyncMock(return_value=segment_drift_data)
    stories = await segment_drift_story_builder.generate_stories(metric_id, grain)
    assert stories == segment_drift_stories


@pytest.mark.asyncio
async def test_generate_stories_shrinking_segment(
    segment_drift_story_builder, segment_drift_stories, segment_drift_data
):
    segment_drift_story_builder.query_service.get_metric = AsyncMock(return_value=metric_details)

    # removing growing segment from dimension slices
    segment_drift_data["dimension_slices"] = segment_drift_data["dimension_slices"][:1]
    shrinking_stories = segment_drift_stories[:2]

    segment_drift_story_builder.analysis_service.get_segment_drift = AsyncMock(return_value=segment_drift_data)
    stories = await segment_drift_story_builder.generate_stories(metric_id, grain)
    assert stories == shrinking_stories


@pytest.mark.asyncio
async def test_generate_stories_improving_segment(
    segment_drift_story_builder, segment_drift_stories, segment_drift_data
):
    segment_drift_story_builder.query_service.get_metric = AsyncMock(return_value=metric_details)

    # removing growing segment from dimension slices
    segment_drift_data["dimension_slices"] = segment_drift_data["dimension_slices"][:1]
    improving_stories = segment_drift_stories[:2]

    segment_drift_story_builder.analysis_service.get_segment_drift = AsyncMock(return_value=segment_drift_data)
    stories = await segment_drift_story_builder.generate_stories(metric_id, grain)
    assert stories == improving_stories


# @pytest.mark.asyncio
# async def test_generate_stories_growing_segment(segment_drift_story_builder, segment_drift_stories,
# segment_drift_data):
#     segment_drift_story_builder.query_service.get_metric = AsyncMock(return_value=metric_details)
#
#     # removing shrinking segment from dimension slices
#     segment_drift_data["dimension_slices"] = segment_drift_data["dimension_slices"][1:]
#     growing_stories = segment_drift_stories[2:]
#
#     segment_drift_story_builder.analysis_service.get_segment_drift = AsyncMock(return_value=segment_drift_data)
#     stories = await segment_drift_story_builder.generate_stories(metric_id, grain)
#     print(stories)
#     assert stories == growing_stories


# @pytest.mark.asyncio
# async def test_generate_stories_worsening_segment(
#     segment_drift_story_builder, segment_drift_stories, segment_drift_data
# ):
#     segment_drift_story_builder.query_service.get_metric = AsyncMock(return_value=metric_details)
#
#     # removing shrinking segment from dimension slices
#     segment_drift_data["dimension_slices"] = segment_drift_data["dimension_slices"][1:]
#     worsening_stories = segment_drift_stories[2:]
#
#     segment_drift_story_builder.analysis_service.get_segment_drift = AsyncMock(return_value=segment_drift_data)
#     stories = await segment_drift_story_builder.generate_stories(metric_id, grain)
#     # print(stories)
#     assert stories == worsening_stories
