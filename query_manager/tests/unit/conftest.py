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
    yield


@pytest.fixture(scope="session")
def client(setup_env):
    # Import only after setting up the environment
    from query_manager.main import app  # noqa

    client = TestClient(app)
    return client


@pytest.fixture(scope="session")
def metric():
    return {
        "id": "metric1",
        "label": "Metric 1",
        "abbreviation": "M1",
        "definition": "Definition 1",
        "unit_of_measure": "Units",
        "unit": "U",
        "complexity": "Simple",
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
def dimension():
    return {
        "id": "dimension1",
        "label": "Dimension 1",
        "reference": "Reference 1",
        "definition": "Definition 1",
        "members": ["member1", "member2"],
    }
