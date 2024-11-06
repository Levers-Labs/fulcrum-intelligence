import asyncio
import importlib
import logging
import os
from collections.abc import Callable, Generator
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import jwt
import pytest
import pytest_asyncio
from _pytest.monkeypatch import MonkeyPatch
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession
from testing.postgresql import Postgresql

from commons.auth.auth import Oauth2Auth
from commons.clients.insight_backend import InsightBackendClient
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
def event_loop(request) -> Generator:
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


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
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS query_store"))
        await conn.run_sync(SQLModel.metadata.drop_all)
        await conn.run_sync(SQLModel.metadata.create_all)

        async with async_session(bind=conn) as session:
            yield session
            await session.flush()
            await session.rollback()


@pytest.fixture()
def override_get_async_session(db_session: AsyncSession) -> Callable:
    async def _override_get_db():
        yield db_session

    return _override_get_db


@pytest.fixture()
def app(setup_env, override_get_async_session: Callable) -> FastAPI:
    # Import only after setting up the environment
    from query_manager.db.config import get_async_session_gen  # noqa
    from query_manager.main import app  # noqa

    app.dependency_overrides[get_async_session_gen] = override_get_async_session
    return app


@pytest.fixture(scope="session", name="jwt_payload")
def mock_jwt_payload():
    return {
        "sub": "PN0CtJASlMDm9TEivb3izsDnIf5dcFYA@clients",
        "permissions": ["user:write", "user:read", "admin:read", "tenant:read", "query:*"],
        "iat": datetime.now(),
        "exp": datetime.now() + timedelta(hours=1),
        "scope": "user:write user:read admin:read tenant:read query:*",
        "tenant_id": 1,  # Include tenant_id if needed
        "external_id": "auth0_123",
    }


@pytest.fixture
def db_user_json(jwt_payload):
    return {
        "name": "test_name",
        "email": "test_email@test.com",
        "provider": "google",
        "tenant_org_id": jwt_payload["external_id"],
        "external_user_id": "auth0|001123",
        "profile_picture": "http://test.com",
    }


@pytest.fixture(name="token")
def mock_token(jwt_payload):
    # Create a mock JWT token
    payload = jwt_payload
    _token = jwt.encode(payload, "secret", algorithm="HS256")  # Use a secret key
    return _token


@pytest_asyncio.fixture()
async def async_client(app: FastAPI, mocker, jwt_payload, token):
    # Mock the JWKS client
    mocker.patch.object(Oauth2Auth, "verify_jwt", return_value=jwt_payload)
    # Mock get tenant config
    mocker.patch.object(
        InsightBackendClient,
        "get_tenant_config",
        AsyncMock(
            return_value={
                "cube_connection_config": {
                    "cube_api_url": "http://test-cube-api.com",
                    "cube_auth_type": "SECRET_KEY",
                    "cube_auth_secret_key": "test-secret-key",
                }
            }
        ),
    )
    # Setup auth headers
    headers = {"Authorization": f"Bearer {token}", "X-Tenant-Id": str(jwt_payload["tenant_id"])}
    # Create an async client
    async with AsyncClient(app=app, base_url="http://test", headers=headers) as ac:
        yield ac


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
