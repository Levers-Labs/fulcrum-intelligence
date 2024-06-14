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
            "genre": "PERFORMANCE",
            "story_group": "SIGNIFICANT_SEGMENTS",
            "story_type": "TOP_4_SEGMENTS",
            "grain": "week",
            "series": [
                {"dimension": "lead_source", "member": None, "value": 7},
                {"dimension": "forecast_category", "member": "Closed", "value": 7},
                {"dimension": "loss_reason", "member": None, "value": 7},
                {"dimension": "billing_plan", "member": "Enterprise", "value": 6},
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
                "average": 1.35,
            },
        },
        {
            "metric_id": "NewBizDeals",
            "genre": "PERFORMANCE",
            "story_group": "SIGNIFICANT_SEGMENTS",
            "story_type": "BOTTOM_4_SEGMENTS",
            "grain": "week",
            "series": [
                {"dimension": "customer_success_manager_name", "member": "Nicolas Ishihara", "value": 0},
                {"dimension": "customer_success_manager_name", "member": "Christine Durham", "value": 0},
                {"dimension": "customer_success_manager_name", "member": "Nathan Kuhn", "value": 0},
                {"dimension": "opportunity_source", "member": "CSM Identified", "value": 0},
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
                "average": 1.35,
            },
        },
    ]


@pytest.fixture
def mock_get_dimension_slice_data(significant_segment_story_builder, mocker, dimension_slice_data):
    async def get_dimension_slice_data(metric_id, start_date, end_date, dimensions):
        return dimension_slice_data[dimensions[0]]

    mocker.patch.object(significant_segment_story_builder.query_service, "get_metric_values", get_dimension_slice_data)
