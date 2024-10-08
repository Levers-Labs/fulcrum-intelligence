import importlib
import logging
import os

import pytest
from _pytest.monkeypatch import MonkeyPatch
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Session, SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession
from testing.postgresql import Postgresql

from query_manager.db.config import MODEL_PATHS

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


@pytest.fixture(scope="session", autouse=True)
def setup_db(postgres):
    db_sync_uri = postgres.url()
    # Ensure tables are created
    engine = create_engine(db_sync_uri)
    # create schema
    logger.info("Creating db schema and tables")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS query_store"))
        conn.commit()
    # create all models
    for model_path in MODEL_PATHS:
        importlib.import_module(model_path)
    SQLModel.metadata.create_all(engine)
    engine.dispose()
    yield db_sync_uri


@pytest.fixture(autouse=True, scope="session")
def setup_env(session_monkeypatch, postgres):
    """
    Setup test environment
    """
    db_sync_uri = postgres.url()
    db_async_uri = db_sync_uri.replace("postgresql://", "postgresql+asyncpg://")

    logger.info("Setting up test environment")
    session_monkeypatch.setenv("SERVER_HOST", "http://localhost:8001")
    session_monkeypatch.setenv("INSIGHTS_BACKEND_SERVER_HOST", "http://localhost:8004/insights/v1")
    session_monkeypatch.setenv("DEBUG", "true")
    session_monkeypatch.setenv("ENV", "dev")
    session_monkeypatch.setenv("SECRET_KEY", "secret")
    session_monkeypatch.setenv("BACKEND_CORS_ORIGINS", '["http://localhost"]')
    session_monkeypatch.setenv("AWS_BUCKET", "bucket")
    session_monkeypatch.setenv("AWS_REGION", "region")
    session_monkeypatch.setenv("CUBE_API_URL", "http://localhost:4000")
    session_monkeypatch.setenv("DATABASE_URL", db_async_uri)
    session_monkeypatch.setenv("AUTH0_ISSUER", "https://some_auth0_domain.com")
    session_monkeypatch.setenv("AUTH0_API_AUDIENCE", "https://some_auth0_audience")
    session_monkeypatch.setenv("AUTH0_CLIENT_ID", "client_id")
    session_monkeypatch.setenv("AUTH0_CLIENT_SECRET", "client_secret")
    yield


@pytest.fixture(scope="session")
def client(setup_env):
    # Import only after setting up the environment
    from query_manager.main import app  # noqa

    client = TestClient(app)
    return client


@pytest.fixture(scope="function")
def db_session(postgres):
    # Create an asynchronous engine
    engine = create_engine(postgres.url(), echo=True)

    # Create a new session
    with Session(engine) as session:
        # Start a transaction
        with session.begin():
            tenant_id = 1
            # Set tenant id context in the db session
            query = text(f"SET app.current_tenant={tenant_id};")
            session.execute(query)
            yield session  # Provide the session to the test

    # Close the engine
    engine.dispose()


@pytest.fixture(scope="function")
async def async_db_session(postgres):
    # Create an asynchronous engine
    engine = create_async_engine(postgres.url().replace("postgresql://", "postgresql+asyncpg://"), echo=True)

    # Create a new async session
    async with AsyncSession(engine) as session:
        tenant_id = 1
        # Set tenant id context in the db session
        query = text(f"SET app.current_tenant={tenant_id};")
        await session.execute(query)
        yield session

    # Close the engine
    await engine.dispose()


@pytest.fixture(scope="session")
def dimension():
    return {
        "id": 1,
        "tenant_id": 1,
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
        "tenant_id": 1,
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
        "hypothetical_max": 100,
    }
