import logging

import pytest
from _pytest.monkeypatch import MonkeyPatch
from fastapi.testclient import TestClient

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def session_monkeypatch():
    m_patch = MonkeyPatch()
    yield m_patch
    m_patch.undo()


@pytest.fixture(autouse=True, scope="session")
def setup_env(session_monkeypatch):
    """
    Setup test environment
    """
    logger.info("Setting up test environment")
    session_monkeypatch.setenv("SERVER_HOST", "http://localhost:8001")
    session_monkeypatch.setenv("DEBUG", "true")
    session_monkeypatch.setenv("ENV", "dev")
    session_monkeypatch.setenv("SECRET_KEY", "secret")
    session_monkeypatch.setenv("BACKEND_CORS_ORIGINS", '["http://localhost"]')
    session_monkeypatch.setenv("AWS_BUCKET", "bucket")
    session_monkeypatch.setenv("AWS_REGION", "region")
    session_monkeypatch.setenv("CUBE_API_URL", "http://localhost:4000")
    session_monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://postgres:root@localhost:5432/qm_test")
    yield


@pytest.fixture(scope="session")
def client(setup_env):
    # Import only after setting up the environment
    from query_manager.main import app  # noqa

    client = TestClient(app)
    return client


@pytest.fixture(scope="session")
def dimension():
    return {
        "id": 1,
        "dimension_id": "billing_plan",
        "label": "Billing Plan",
        "reference": "billing_plan",
        "definition": "Billing Plan Definition",
        "meta_data": {
            "semantic_meta": {"cube": "cube1", "member": "billing_plan", "member_type": "dimension"},
        },
    }


@pytest.fixture(scope="session")
def metric(dimension):
    return {
        "id": 1,
        "metric_id": "metric1",
        "label": "Metric 1",
        "abbreviation": "M1",
        "definition": "Definition 1",
        "unit_of_measure": "Units",
        "unit": "U",
        "complexity": "Complex",
        "metric_expression": {
            "expression_str": "{SalesMktSpend\u209c} / {NewCust\u209c}",
            "metric_id": "CAC",
            "type": "metric",
            "period": 0,
            "expression": {
                "type": "expression",
                "operator": "/",
                "operands": [
                    {"type": "metric", "metric_id": "SalesMktSpend", "period": 0},
                    {"type": "metric", "metric_id": "NewCust", "period": 0},
                ],
            },
        },
        "grain_aggregation": "aggregation",
        "terms": ["term1", "term2"],
        "periods": ["day", "week"],
        "aggregations": ["aggregation1", "aggregation2"],
        "owned_by_team": ["team1", "team2"],
        "dimensions": [dimension],
        "meta_data": {
            "semantic_meta": {
                "cube": "cube1",
                "member": "member1",
                "member_type": "measure",
                "time_dimension": {
                    "cube": "cube1",
                    "member": "created_at",
                },
            },
        },
    }
