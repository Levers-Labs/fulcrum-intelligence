import asyncio
import importlib
import logging
import os
from collections.abc import Callable
from datetime import datetime, timedelta
from unittest.mock import Mock

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
from insights_backend.db.config import MODEL_PATHS
from insights_backend.notifications.services.deployment_manager import PrefectDeploymentManager

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def postgres():
    os.environ["LANG"] = "en_US.UTF-8"
    os.environ["LC_ALL"] = "en_US.UTF-8"
    with Postgresql() as postgres:
        yield postgres


@pytest.fixture(scope="session")
def session_monkeypatch():
    m_patch = MonkeyPatch()
    yield m_patch
    m_patch.undo()


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
    engine = create_async_engine(db_async_uri, echo=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)  # type: ignore
    async with engine.begin() as conn:
        # recreate all models
        for model_path in MODEL_PATHS:
            importlib.import_module(model_path)
        # Create schema
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS insights_store"))
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
    from insights_backend.db.config import get_async_session
    from insights_backend.main import app  # noqa

    app.dependency_overrides[get_async_session] = override_get_async_session
    return app


@pytest.fixture(autouse=True, scope="session")
def setup_env(session_monkeypatch, postgres):  # noqa
    """
    Setup test environment
    """

    db_sync_uri = postgres.url()
    db_async_uri = db_sync_uri.replace("postgresql://", "postgresql+asyncpg://")
    logger.info(f"Setting up test environment {db_async_uri}")
    session_monkeypatch.setenv("SERVER_HOST", "http://localhost:8004")
    session_monkeypatch.setenv("DEBUG", "true")
    session_monkeypatch.setenv("ENV", "dev")
    session_monkeypatch.setenv("SECRET_KEY", "secret")
    session_monkeypatch.setenv("BACKEND_CORS_ORIGINS", '["http://localhost"]')
    session_monkeypatch.setenv("DATABASE_URL", db_async_uri)
    session_monkeypatch.setenv("AUTH0_API_AUDIENCE", "https://some_auth0_audience")
    session_monkeypatch.setenv("AUTH0_ISSUER", "https://some_auth0_domain.com")
    session_monkeypatch.setenv("AUTH0_CLIENT_ID", "client_id")
    session_monkeypatch.setenv("AUTH0_CLIENT_SECRET", "client_secret")
    session_monkeypatch.setenv("SLACK_CLIENT_ID", "slack_client_id")
    session_monkeypatch.setenv("SLACK_CLIENT_SECRET", "slack_client_secret")
    session_monkeypatch.setenv("SLACK_OAUTH_REDIRECT_PATH", "slack/oauth/callback")
    session_monkeypatch.setenv("PREFECT_API_URL", "http://localhost:4200")
    session_monkeypatch.setenv("PREFECT_API_TOKEN", "prefect_api_token")
    # Cache settings for tests - use InMemory backend (no REDIS_URL)
    session_monkeypatch.setenv("CACHE_PREFIX", "test-cache")
    session_monkeypatch.setenv("CACHE_TTL_SECONDS", "60")

    yield


@pytest.fixture(autouse=True, scope="session")
def setup_cache():
    """Mock all cache-related functionality for testing"""
    import sys
    from unittest.mock import AsyncMock, MagicMock

    # Make the decorator a no-op that returns the original function
    def cache_decorator(*args, **kwargs):
        def decorator(func):
            return func

        return decorator

    # Mock the ORIGINAL fastapi_cache (not fastapi_cache2)
    mock_cache = MagicMock()
    mock_decorator = MagicMock()
    mock_decorator.cache = cache_decorator
    mock_cache.decorator = mock_decorator
    mock_fastapi_cache = MagicMock()
    mock_fastapi_cache.get_prefix = MagicMock(return_value="test-prefix")
    mock_fastapi_cache.init = MagicMock()
    mock_fastapi_cache.clear = AsyncMock()
    mock_fastapi_cache._prefix = "test-prefix"  # Set the _prefix attribute
    mock_cache.FastAPICache = mock_fastapi_cache

    # Mock commons.cache functions that are awaited
    mock_commons_cache = MagicMock()
    mock_commons_cache.init_cache = AsyncMock()
    mock_commons_cache.close_cache = AsyncMock()
    mock_commons_cache.invalidate_namespace = AsyncMock()
    mock_commons_cache.tenant_cache_key_builder = MagicMock()
    mock_commons_cache.cache_settings = MagicMock()

    # Inject into sys.modules
    sys.modules["fastapi_cache"] = mock_cache
    sys.modules["fastapi_cache.decorator"] = mock_decorator
    sys.modules["fastapi_cache.backends"] = MagicMock()
    sys.modules["commons.cache"] = mock_commons_cache


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


@pytest.fixture
def mock_deployment_manager(monkeypatch):
    """Mock PrefectDeploymentManager for testing"""
    mock_manager = Mock(spec=PrefectDeploymentManager)
    mock_manager.create_deployment.return_value = "test-deployment-id"
    mock_manager.delete_deployment.return_value = None
    mock_manager.read_deployment_schedules.return_value = [{"id": "test-schedule-id", "active": True}]
    mock_manager.update_deployment_schedule.return_value = True

    # Patch the PrefectDeploymentManager class
    monkeypatch.setattr(
        "insights_backend.notifications.subscribers.deployment_handlers.PrefectDeploymentManager",
        Mock(return_value=mock_manager),
    )
    return mock_manager
