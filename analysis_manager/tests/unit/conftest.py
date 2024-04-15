import logging
import os

import pytest
from _pytest.monkeypatch import MonkeyPatch
from fastapi.testclient import TestClient
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor

logger = logging.getLogger(__name__)


# Temp postgres database for testing
test_db = factories.postgresql_proc(port=None, dbname="test_db")
os.environ.setdefault("LC_CTYPE", "en_US.UTF-8")


@pytest.fixture(scope="session")
def session_monkeypatch():
    m_patch = MonkeyPatch()
    yield m_patch
    m_patch.undo()


@pytest.fixture(autouse=True, scope="session")
def setup_env(session_monkeypatch, test_db):  # noqa
    """
    Setup test environment
    """
    pg_host = test_db.host
    pg_port = test_db.port
    pg_user = test_db.user
    pg_db = test_db.dbname
    db_uri = f"postgresql+asyncpg://{pg_user}:@{pg_host}:{pg_port}/{pg_db}"

    logger.info("Setting up test environment")
    session_monkeypatch.setenv("SERVER_HOST", "http://localhost:8001")
    session_monkeypatch.setenv("DEBUG", "true")
    session_monkeypatch.setenv("ENV", "dev")
    session_monkeypatch.setenv("SECRET_KEY", "secret")
    session_monkeypatch.setenv("BACKEND_CORS_ORIGINS", '["http://localhost"]')
    session_monkeypatch.setenv("QUERY_MANAGER_SERVER_HOST", "http://localhost:8001/v1/")
    session_monkeypatch.setenv("DATABASE_URL", db_uri)
    yield


@pytest.fixture(scope="session")
def client(setup_env, test_db, session_monkeypatch):  # noqa
    # Import only after setting up the environment
    from analysis_manager.main import app  # noqa

    pg_host = test_db.host
    pg_port = test_db.port
    pg_user = test_db.user
    pg_password = test_db.password
    pg_db = test_db.dbname
    with DatabaseJanitor(
        user=pg_user, host=pg_host, port=pg_port, dbname=pg_db, version=test_db.version, password=pg_password
    ):
        client = TestClient(app)
        yield client


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
        "id": "CAC",
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
def metric_list(metric_cac, metric_sms):
    metric_ss = metric_sms.copy()
    metric_ss["id"] = "SalesSpend"
    metric_ss["metric_expression"] = None
    metric_ms = metric_sms.copy()
    metric_ms["id"] = "MktSpend"
    metric_ms["metric_expression"] = None

    metric_new_cust = metric_sms.copy()
    metric_new_cust["id"] = "NewCust"

    metric_old_cust = metric_new_cust.copy()
    metric_old_cust["id"] = "OldCust"
    return [metric_cac, metric_sms, metric_ss, metric_ms, metric_new_cust, metric_old_cust]
