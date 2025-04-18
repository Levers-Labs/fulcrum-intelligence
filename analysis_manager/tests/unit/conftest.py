import importlib
import logging
import os
from datetime import date, datetime, timedelta

import pytest
import pytest_asyncio
from _pytest.monkeypatch import MonkeyPatch
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from testing.postgresql import Postgresql

from analysis_manager.db.config import MODEL_PATHS

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def postgres():
    os.environ.setdefault("LC_CTYPE", "en_US.UTF-8")
    os.environ.setdefault("LC_ALL", "en_US.UTF-8")
    with Postgresql() as postgres:
        yield postgres


@pytest.fixture(scope="session")
def session_monkeypatch():
    m_patch = MonkeyPatch()
    yield m_patch
    m_patch.undo()


@pytest.fixture(autouse=True, scope="session")
def setup_env(session_monkeypatch, postgres):
    """
    Setup test environment
    """
    db_sync_uri = postgres.url()
    db_async_uri = db_sync_uri.replace("postgresql://", "postgresql+asyncpg://")

    logger.info("Setting up test environment")
    session_monkeypatch.setenv("SERVER_HOST", "http://localhost:8001")
    session_monkeypatch.setenv("DEBUG", "true")
    session_monkeypatch.setenv("ENV", "dev")
    session_monkeypatch.setenv("SECRET_KEY", "secret")
    session_monkeypatch.setenv("BACKEND_CORS_ORIGINS", '["http://localhost"]')
    session_monkeypatch.setenv("QUERY_MANAGER_SERVER_HOST", "http://localhost:8001/v1/")
    session_monkeypatch.setenv("DATABASE_URL", db_async_uri)
    session_monkeypatch.setenv("AUTH0_API_AUDIENCE", "https://some_auth0_audience")
    session_monkeypatch.setenv("AUTH0_ISSUER", "https://some_auth0_domain.com")
    session_monkeypatch.setenv("AUTH0_CLIENT_ID", "client_id")
    session_monkeypatch.setenv("AUTH0_CLIENT_SECRET", "client_secret")
    yield


@pytest.fixture(scope="session")
def client(setup_env):
    # Import only after setting up the environment
    from analysis_manager.main import app  # noqa

    with TestClient(app) as client:
        yield client


@pytest_asyncio.fixture()
async def db_session(postgres):
    db_sync_uri = postgres.url()
    db_async_uri = db_sync_uri.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(db_async_uri, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)  # type: ignore
    async with engine.begin() as conn:
        # recreate all models
        for model_path in MODEL_PATHS:
            importlib.import_module(model_path)
        # Create schema
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS analysis_store"))
        await conn.run_sync(SQLModel.metadata.drop_all)
        await conn.run_sync(SQLModel.metadata.create_all)

        async with async_session(bind=conn) as session:
            yield session
            await session.flush()
            await session.rollback()


@pytest.fixture(scope="session", name="jwt_payload")
def mock_jwt_payload():
    return {
        "sub": "PN0CtJASlMDm9TEivb3izsDnIf5dcFYA@clients",
        "permissions": [
            "user:write",
            "user:read",
            "admin:read",
            "admin:write",
            "alert_report:read",
            "alert_report" ":write",
        ],
        "iat": datetime.now(),
        "exp": datetime.now() + timedelta(hours=1),
        "scope": "user:write user:read admin:read admin:write alert_report:read alert_report:write",
        "tenant_id": 1,  # Include tenant_id if needed
        "external_id": "auth0_123",
    }


@pytest.fixture(scope="session")
def metric_values():
    return [
        {
            "metric_id": "CAC",
            "value": 2295,
            "date": "2024-01-24",
            "customer_segment": "Enterprise",
            "channel": "Online",
            "region": "Asia",
        },
        {
            "metric_id": "CAC",
            "value": 3674,
            "date": "2024-01-24",
            "customer_segment": "Enterprise",
            "channel": "Online",
            "region": "Europe",
        },
        {
            "metric_id": "CAC",
            "value": 4084,
            "date": "2024-01-24",
            "customer_segment": "Enterprise",
            "channel": "Online",
            "region": "America",
        },
        {
            "metric_id": "CAC",
            "value": 2995,
            "date": "2024-01-24",
            "customer_segment": "Enterprise",
            "channel": "Retail",
            "region": "Asia",
        },
        {
            "metric_id": "CAC",
            "value": 4847,
            "date": "2024-01-24",
            "customer_segment": "Enterprise",
            "channel": "Retail",
            "region": "Europe",
        },
        {
            "metric_id": "CAC",
            "value": 580,
            "date": "2024-01-24",
            "customer_segment": "Enterprise",
            "channel": "Retail",
            "region": "America",
        },
        {
            "metric_id": "CAC",
            "value": 3759,
            "date": "2024-01-24",
            "customer_segment": "Enterprise",
            "channel": "Wholesale",
            "region": "Asia",
        },
        {
            "metric_id": "CAC",
            "value": 1719,
            "date": "2024-01-24",
            "customer_segment": "Enterprise",
            "channel": "Wholesale",
            "region": "Europe",
        },
        {
            "metric_id": "CAC",
            "value": 792,
            "date": "2024-01-24",
            "customer_segment": "Enterprise",
            "channel": "Wholesale",
            "region": "America",
        },
    ]


@pytest.fixture(scope="session")
def metric_values_correlate():
    return [
        {"metric_id": "NewMRR", "value": 50927, "date": "2022-09-01"},
        {"metric_id": "NewMRR", "value": 40294, "date": "2022-10-01"},
        {"metric_id": "NewMRR", "value": 67557, "date": "2022-10-31"},
        {"metric_id": "NewMRR", "value": 74216, "date": "2022-11-30"},
        {"metric_id": "NewMRR", "value": 58084, "date": "2022-12-30"},
        {"metric_id": "NewMRR", "value": 39168, "date": "2023-01-29"},
        {"metric_id": "NewMRR", "value": 20033, "date": "2023-02-28"},
        {"metric_id": "NewMRR", "value": 61401, "date": "2023-03-30"},
        {"metric_id": "NewMRR", "value": 86673, "date": "2023-04-29"},
        {"metric_id": "NewMRR", "value": 26451, "date": "2023-05-29"},
        {"metric_id": "CAC", "value": 363, "date": "2022-09-01"},
        {"metric_id": "CAC", "value": 2146, "date": "2022-10-01"},
        {"metric_id": "CAC", "value": 1540, "date": "2022-10-31"},
        {"metric_id": "CAC", "value": 3908, "date": "2022-11-30"},
        {"metric_id": "CAC", "value": 4000, "date": "2022-12-30"},
        {"metric_id": "CAC", "value": 1169, "date": "2023-01-29"},
    ]


@pytest.fixture(scope="session")
def metric_values_netmrr():
    metric_values_net_mrr = [
        {"metric_id": "NewMRR", "value": 50927, "date": "2022-09-01"},
        {"metric_id": "NewMRR", "value": 40294, "date": "2022-10-01"},
        {"metric_id": "NewMRR", "value": 67557, "date": "2022-10-31"},
        {"metric_id": "NewMRR", "value": 74216, "date": "2022-11-30"},
        {"metric_id": "NewMRR", "value": 58084, "date": "2022-12-30"},
        {"metric_id": "NewMRR", "value": 39168, "date": "2023-01-29"},
        {"metric_id": "NewMRR", "value": 20033, "date": "2023-02-28"},
        {"metric_id": "NewMRR", "value": 61401, "date": "2023-03-30"},
        {"metric_id": "NewMRR", "value": 86673, "date": "2023-04-29"},
        {"metric_id": "NewMRR", "value": 26451, "date": "2023-05-29"},
        {"metric_id": "NewMRR", "value": 47445, "date": "2023-06-28"},
        {"metric_id": "NewMRR", "value": 63417, "date": "2023-07-28"},
        {"metric_id": "NewMRR", "value": 63514, "date": "2023-08-27"},
        {"metric_id": "NewMRR", "value": 64206, "date": "2023-09-26"},
        {"metric_id": "NewMRR", "value": 78596, "date": "2023-10-26"},
        {"metric_id": "NewMRR", "value": 21955, "date": "2023-11-25"},
        {"metric_id": "NewMRR", "value": 30849, "date": "2023-12-25"},
        {"metric_id": "NewMRR", "value": 77068, "date": "2024-01-24"},
        {"metric_id": "NewMRR", "value": 95685, "date": "2024-02-23"},
        {"metric_id": "NewMRR", "value": 62233, "date": "2024-03-24"},
        {"metric_id": "NewMRR", "value": 42173, "date": "2024-04-23"},
        {"metric_id": "NewMRR", "value": 18191, "date": "2024-05-23"},
        {"metric_id": "NewMRR", "value": 95453, "date": "2024-06-22"},
        {"metric_id": "NewMRR", "value": 46417, "date": "2024-07-22"},
        {"metric_id": "NewMRR", "value": 69306, "date": "2024-08-21"},
        {"metric_id": "NewMRR", "value": 76026, "date": "2024-09-20"},
        {"metric_id": "NewMRR", "value": 43884, "date": "2024-10-20"},
        {"metric_id": "NewMRR", "value": 11489, "date": "2024-11-19"},
        {"metric_id": "NewMRR", "value": 99466, "date": "2024-12-19"},
        {"metric_id": "NewMRR", "value": 78883, "date": "2025-01-18"},
        {"metric_id": "NewMRR", "value": 76167, "date": "2025-02-17"},
        {"metric_id": "NewMRR", "value": 95945, "date": "2025-03-19"},
        {"metric_id": "NewMRR", "value": 97288, "date": "2025-04-18"},
        {"metric_id": "NewMRR", "value": 79936, "date": "2025-05-18"},
        {"metric_id": "NewMRR", "value": 33299, "date": "2025-06-17"},
    ]
    return metric_values_net_mrr


@pytest.fixture(scope="session")
def metric_cac():
    return {
        "metric_id": "CAC",
        "label": "Metric 1",
        "abbreviation": "M1",
        "definition": "Definition 1",
        "unit_of_measure": "Units",
        "unit": "U",
        "complexity": "Simple",
        "metric_expression": {
            "expression_str": "{SalesMktSpend\u209c} / ({NewCust\u209c} - {OldCust\u209c})",
            "metric_id": "CAC",
            "type": "metric",
            "period": 0,
            "expression": {
                "type": "expression",
                "operator": "+",
                "operands": [
                    {"type": "metric", "metric_id": "SalesMktSpend", "period": 0},
                    {
                        "type": "expression",
                        "operator": "-",
                        "operands": [
                            {"type": "metric", "metric_id": "NewCust", "period": 0},
                            {"type": "metric", "metric_id": "OldCust", "period": 0},
                        ],
                    },
                ],
            },
        },
        "grain_aggregation": "aggregation",
        "components": ["component1", "component2"],
        "terms": ["term1", "term2"],
        "output_of": "output",
        "input_to": ["input1", "input2"],
        "influences": ["influence1", "influence2"],
        "influenced_by": ["influenced1", "influenced2"],
        "periods": ["period1", "period2"],
        "aggregations": ["aggregation1", "aggregation2"],
        "owned_by_team": ["team1", "team2"],
    }


@pytest.fixture(scope="session")
def metric_sms():
    return {
        "id": "SalesMktSpend",
        "label": "Sales and Marketing Spend",
        "abbreviation": "M1",
        "definition": "Definition 1",
        "unit_of_measure": "Units",
        "unit": "U",
        "complexity": "Simple",
        "metric_expression": {
            "expression_str": "{SalesSpend} + {MktSpend}",
            "metric_id": "CAC",
            "type": "metric",
            "period": 0,
            "expression": {
                "type": "expression",
                "operator": "+",
                "operands": [
                    {"type": "metric", "metric_id": "SalesSpend", "period": 0},
                    {"type": "metric", "metric_id": "MktSpend", "period": 0},
                ],
            },
        },
        "grain_aggregation": "sum",
        "components": ["component1", "component2"],
        "terms": ["term1", "term2"],
        "output_of": "output",
        "input_to": ["input1", "input2"],
        "influences": ["influence1", "influence2"],
        "influenced_by": ["influenced1", "influenced2"],
        "periods": ["period1", "period2"],
        "aggregations": ["aggregation1", "aggregation2"],
        "owned_by_team": ["team1", "team2"],
    }


@pytest.fixture(scope="session")
def metric_list(metric_cac, metric_sms):
    metric_ss = metric_sms.copy()
    metric_ss["metric_id"] = "SalesSpend"
    metric_ss["metric_expression"] = None
    metric_ms = metric_sms.copy()
    metric_ms["metric_id"] = "MktSpend"
    metric_ms["metric_expression"] = None

    metric_new_cust = metric_sms.copy()
    metric_new_cust["metric_id"] = "NewCust"

    metric_old_cust = metric_new_cust.copy()
    metric_old_cust["metric_id"] = "OldCust"
    return [metric_cac, metric_sms, metric_ss, metric_ms, metric_new_cust, metric_old_cust]


@pytest.fixture(scope="session")
def segment_drift_evaluation_data():
    return [
        {"date": "2025-03-01", "metric_id": "ToMRR", "value": 100, "region": "Asia", "stage_name": "Won"},
        {"date": "2025-03-02", "metric_id": "ToMRR", "value": 200, "region": "Asia", "stage_name": "Won"},
        {"date": "2025-03-02", "metric_id": "ToMRR", "value": 300, "region": "Asia", "stage_name": "Won"},
        {"date": "2025-03-02", "metric_id": "ToMRR", "value": 400, "region": "Asia", "stage_name": "Procurement"},
        {"date": "2025-03-03", "metric_id": "ToMRR", "value": 500, "region": "EMEA", "stage_name": "Lost"},
        {"date": "2025-03-04", "metric_id": "ToMRR", "value": 600, "region": "EMEA", "stage_name": "Sale"},
        {"date": "2025-03-05", "metric_id": "ToMRR", "value": 700, "region": "EMEA", "stage_name": "Sale"},
    ]


@pytest.fixture(scope="session")
def segment_drift_comparison_data():
    return [
        {"date": "2024-03-01", "metric_id": "ToMRR", "value": 500, "region": "Asia", "stage_name": "Won"},
        {"date": "2024-03-02", "metric_id": "ToMRR", "value": 600, "region": "Asia", "stage_name": "Won"},
        {"date": "2024-03-02", "metric_id": "ToMRR", "value": 600, "region": "Asia", "stage_name": "Won"},
        {"date": "2024-03-02", "metric_id": "ToMRR", "value": 400, "region": "Asia", "stage_name": "Procurement"},
        {"date": "2024-03-03", "metric_id": "ToMRR", "value": 300, "region": "EMEA", "stage_name": "Lost"},
        {"date": "2024-03-04", "metric_id": "ToMRR", "value": 200, "region": "EMEA", "stage_name": "Sale"},
        {"date": "2024-03-05", "metric_id": "ToMRR", "value": 800, "region": "EMEA", "stage_name": "Sale"},
    ]


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


@pytest.fixture
def dsensei_csv_file_id():
    return "ac60684ce86261be1227a43f75ecef96"


@pytest.fixture
def mock_get_metric_time_series(mocker, segment_drift_evaluation_data, segment_drift_comparison_data):
    def _mock_get_metric_time_series(metric_id: str, start_date: date, end_date: date, dimensions: list[str]):
        if start_date == date(2025, 3, 1):
            return segment_drift_evaluation_data
        else:
            return segment_drift_comparison_data

    mocker.patch(
        "commons.clients.query_manager.QueryManagerClient.get_metric_time_series",
        side_effect=_mock_get_metric_time_series,
    )


@pytest.fixture
def mock_drift_resp():
    return {
        "metric_id": "CAC",
        "evaluation_value": 3799.9996,
        "comparison_value": 5899.9933,
        "drift": {
            "absolute_drift": -2099.9937,
            "percentage_drift": -0.3559315397866638,
            "relative_impact": 1,
            "marginal_contribution": 1,
            "relative_impact_root": 1,
            "marginal_contribution_root": 1,
        },
        "components": [
            {
                "metric_id": "SalesMktSpend",
                "evaluation_value": 206,
                "comparison_value": 241,
                "drift": {
                    "absolute_drift": -0.15692076470107352,
                    "percentage_drift": -0.14522821576763487,
                    "relative_impact": 0.3566784274548074,
                    "marginal_contribution": -0.12695310189267545,
                    "relative_impact_root": 0.3566784274548074,
                    "marginal_contribution_root": -0.12695310189267545,
                },
                "components": [
                    {
                        "metric_id": "OpenNewBizOpps",
                        "evaluation_value": 512,
                        "comparison_value": 455,
                        "drift": {
                            "absolute_drift": 0.11802720608855743,
                            "percentage_drift": 0.12527472527472527,
                            "relative_impact": -0.8050695786735789,
                            "marginal_contribution": 0.10978325931323069,
                            "relative_impact_root": -0.28715095131299645,
                            "marginal_contribution_root": -0.013937325305702585,
                        },
                        "components": [],
                    },
                    {
                        "metric_id": "SQORate",
                        "evaluation_value": 33.3984,
                        "comparison_value": 43.5165,
                        "drift": {
                            "absolute_drift": -0.26463218187588744,
                            "percentage_drift": -0.23251180586674017,
                            "relative_impact": 1.8050695786735669,
                            "marginal_contribution": -0.24614819250833003,
                            "relative_impact_root": 0.6438293787677996,
                            "marginal_contribution_root": 0.031249276564207912,
                        },
                        "components": [],
                    },
                ],
            },
            {
                "metric_id": "SQOToWinRate",
                "evaluation_value": 18.4466,
                "comparison_value": 24.4813,
                "drift": {
                    "absolute_drift": -0.283029489147558,
                    "percentage_drift": -0.24650243246886402,
                    "relative_impact": 0.6433215725451906,
                    "marginal_contribution": -0.22897843789398764,
                    "relative_impact_root": 0.6433215725451906,
                    "marginal_contribution_root": -0.22897843789398764,
                },
            },
        ],
    }


@pytest.fixture(scope="session")
def metric_expression():
    return {
        "metric_id": "NewBizDeals",
        "type": "metric",
        "period": 0,
        "expression_str": "{AcceptOpps} * {SQOToWinRate}",
        "expression": {
            "type": "expression",
            "operator": "*",
            "operands": [
                {
                    "metric_id": "AcceptOpps",
                    "type": "metric",
                    "period": 0,
                    "expression_str": "{OpenNewBizOpps} * {SQORate}",
                    "expression": {
                        "type": "expression",
                        "operator": "*",
                        "operands": [
                            {"metric_id": "OpenNewBizOpps", "type": "metric", "period": 0},
                            {"metric_id": "SQORate", "type": "metric", "period": 0},
                        ],
                    },
                },
                {"metric_id": "SQOToWinRate", "type": "metric", "period": 0},
            ],
        },
    }


@pytest.fixture(scope="session")
def leverage_id_response():
    return {
        "metric_id": "NewBizDeals",
        "leverage": {"pct_diff": 0, "pct_diff_root": 0},
        "components": [
            {
                "metric_id": "AcceptOpps",
                "leverage": {"pct_diff": 18.865, "pct_diff_root": 18.865},
                "components": [
                    {
                        "metric_id": "OpenNewBizOpps",
                        "leverage": {"pct_diff": 51.942, "pct_diff_root": 51.942},
                        "components": [],
                    },
                    {
                        "metric_id": "SQORate",
                        "leverage": {"pct_diff": 43.868, "pct_diff_root": 43.868},
                        "components": [],
                    },
                ],
            },
            {
                "metric_id": "SQOToWinRate",
                "leverage": {"pct_diff": 169.357, "pct_diff_root": 169.357},
                "components": [],
            },
        ],
    }


@pytest.fixture()
def get_metric_response():
    return {
        "metric_id": "NewBizDeals",
        "label": "New Business Deals",
        "abbreviation": "NewBizDeals",
        "definition": "The count of New Business Opportunities that were generated in the period related to closing "
        "deals.",
        "unit_of_measure": "Quantity",
        "unit": "n",
        "terms": ["New Business Opportunities"],
        "complexity": "Complex",
        "metric_expression": {
            "type": "metric",
            "metric_id": "NewBizDeals",
            "coefficient": 1,
            "period": 0,
            "expression_str": "{AcceptOpps} * {SQOToWinRate}",
            "expression": {
                "type": "expression",
                "operator": "*",
                "operands": [
                    {
                        "type": "metric",
                        "metric_id": "AcceptOpps",
                        "coefficient": 1,
                        "period": 0,
                        "expression_str": None,
                        "expression": None,
                        "power": 1,
                    },
                    {
                        "type": "metric",
                        "metric_id": "SQOToWinRate",
                        "coefficient": 1,
                        "period": 0,
                        "expression_str": None,
                        "expression": None,
                        "power": 1,
                    },
                ],
            },
            "power": 1,
        },
        "periods": ["week", "month", "quarter"],
        "grain_aggregation": "sum",
        "aggregations": ["sum"],
        "owned_by_team": ["Sales"],
        "meta_data": {
            "semantic_meta": {
                "cube": "dim_opportunity",
                "member": "new_biz_deals",
                "member_type": "measure",
                "time_dimension": {"cube": "dim_opportunity", "member": "close_date"},
            }
        },
        "hypothetical_max": None,
        "id": 1,
        "outputs": [],
        "inputs": [],
        "influences": [],
        "influencers": ["SQLs"],
        "dimensions": [],
    }
