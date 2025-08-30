import asyncio
import importlib
import logging
import os
from collections.abc import Callable
from datetime import datetime, timedelta

import jwt
import pytest
import pytest_asyncio
from _pytest.monkeypatch import MonkeyPatch
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
from testing.postgresql import Postgresql

from commons.auth.auth import Oauth2Auth
from story_manager.db.config import MODEL_PATHS

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
def setup_env(session_monkeypatch, postgres):  # noqa
    """
    Setup test environment
    """

    db_sync_uri = postgres.url()
    db_async_uri = db_sync_uri.replace("postgresql://", "postgresql+asyncpg://")
    logger.info("Setting up test environment")
    session_monkeypatch.setenv("SERVER_HOST", "http://localhost:8002")
    session_monkeypatch.setenv("DEBUG", "true")
    session_monkeypatch.setenv("ENV", "dev")
    session_monkeypatch.setenv("SECRET_KEY", "secret")
    session_monkeypatch.setenv("BACKEND_CORS_ORIGINS", '["http://localhost"]')
    session_monkeypatch.setenv("QUERY_MANAGER_SERVER_HOST", "http://localhost:8001/v1/")
    session_monkeypatch.setenv("ANALYSIS_MANAGER_SERVER_HOST", "http://localhost:8000/v1/")
    session_monkeypatch.setenv("INSIGHTS_BACKEND_SERVER_HOST", "http://localhost:8004/v1/")
    session_monkeypatch.setenv("DATABASE_URL", db_async_uri)
    session_monkeypatch.setenv("DSENSEI_BASE_URL", "localhost:5001")
    session_monkeypatch.setenv("AUTH0_API_AUDIENCE", "https://some_auth0_audience")
    session_monkeypatch.setenv("AUTH0_ISSUER", "https://some_auth0_domain.com")
    session_monkeypatch.setenv("AUTH0_CLIENT_ID", "client_id")
    session_monkeypatch.setenv("AUTH0_CLIENT_SECRET", "client_secret")
    yield


@pytest.fixture(scope="session")
def event_loop(request):
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture()
async def db_session(postgres):
    db_sync_uri = postgres.url()
    db_async_uri = db_sync_uri.replace("postgresql://", "postgresql+asyncpg://")
    engine = create_async_engine(db_async_uri, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)  # type: ignore
    async with engine.begin() as conn:
        # Import all models
        for model_path in MODEL_PATHS:
            importlib.import_module(model_path)
        # Create schema
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS story_store"))
        await conn.run_sync(SQLModel.metadata.drop_all)
        await conn.run_sync(SQLModel.metadata.create_all)

        async with async_session(bind=conn) as session:
            yield session
            await session.flush()
            await session.rollback()


@pytest.fixture()
def override_get_async_session(db_session: AsyncSession):
    async def _override_get_db():
        yield db_session

    return _override_get_db


@pytest.fixture()
def app(setup_env, override_get_async_session: Callable) -> FastAPI:
    # Import only after setting up the environment
    from story_manager.db.config import get_async_session
    from story_manager.main import app  # noqa

    # Override the get_async_session function to use test database session
    # The manager parameter is ignored in tests
    async def override_async_session_with_manager(mgr=None):
        async for session in override_get_async_session():
            yield session

    app.dependency_overrides[get_async_session] = override_async_session_with_manager
    return app


@pytest.fixture(scope="session", name="jwt_payload")
def mock_jwt_payload():
    return {
        "sub": "PN0CtJASlMDm9TEivb3izsDnIf5dcFYA@clients",
        "permissions": ["user:read", "admin:read", "story:*"],
        "iat": datetime.now(),
        "exp": datetime.now() + timedelta(hours=1),
        "scope": "user:write user:read admin:read story:*",
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
    # Setup auth headers
    headers = {"Authorization": f"Bearer {token}", "X-Tenant-Id": str(jwt_payload["tenant_id"])}
    # Create an async client
    async with AsyncClient(app=app, base_url="http://test", headers=headers) as ac:
        yield ac
