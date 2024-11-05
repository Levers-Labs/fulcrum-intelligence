import importlib
import logging
import os
from datetime import datetime, timedelta

import fastapi
import jwt
import pytest
from _pytest.monkeypatch import MonkeyPatch
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlmodel import Session, SQLModel
from testing.postgresql import Postgresql

from commons.auth.auth import Oauth2Auth
from insights_backend.db.config import MODEL_PATHS

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def postgres():
    os.environ["LANG"] = "en_US.UTF-8"
    os.environ["LC_ALL"] = "en_US.UTF-8"
    with Postgresql() as postgres:
        yield postgres


@pytest.fixture(scope="session")
def mock_security(*args, **kwargs):
    return {
        "name": "test_name",
        "email": "test_email@test.com",
        "provider": "google",
        "external_user_id": "auth0|001123",
        "profile_picture": "http://test.com",
    }


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
        # Create schema
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS insights_store"))

        # Create tenant role and grant permissions
        conn.execute(text("CREATE ROLE tenant_user"))

        # Grant schema permissions
        conn.execute(text("GRANT USAGE ON SCHEMA insights_store TO tenant_user"))
        conn.execute(text("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA insights_store TO tenant_user"))
        conn.execute(
            text(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA insights_store GRANT SELECT, INSERT, UPDATE,"
                " DELETE ON TABLES TO tenant_user"
            )
        )
        conn.execute(text("GRANT USAGE ON ALL SEQUENCES IN SCHEMA insights_store TO tenant_user"))
        conn.execute(text("ALTER DEFAULT PRIVILEGES IN SCHEMA insights_store GRANT USAGE ON SEQUENCES TO tenant_user"))

        # Grant tenant_user role to postgres
        conn.execute(text("GRANT tenant_user TO postgres"))

        conn.commit()
    # create all models
    for model_path in MODEL_PATHS:
        importlib.import_module(model_path)
    SQLModel.metadata.create_all(engine)
    engine.dispose()
    yield db_sync_uri


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
    yield


@pytest.fixture(scope="session", name="jwt_payload")
def mock_jwt_payload():
    return {
        "sub": "PN0CtJASlMDm9TEivb3izsDnIf5dcFYA@clients",
        "permissions": ["user:write", "user:read"],
        "iat": datetime.now(),
        "exp": datetime.now() + timedelta(hours=1),
        "scope": "user:write user:read",
        "tenant_id": 1,  # Include tenant_id if needed
    }


@pytest.fixture(name="token")
def mock_token(jwt_payload):
    # Create a mock JWT token
    payload = jwt_payload
    _token = jwt.encode(payload, "secret", algorithm="HS256")  # Use a secret key
    return _token


@pytest.fixture()
def client(setup_env, token, mock_security, jwt_payload, mocker):
    # Import only after setting up the environment
    from insights_backend.main import app  # noqa

    # Mock the JWKS client
    mocker.patch.object(Oauth2Auth, "verify_jwt", return_value=jwt_payload)

    fastapi.Security = mock_security
    headers = {"Authorization": f"Bearer {token}"}
    with TestClient(app, headers=headers) as client:
        yield client


@pytest.fixture(scope="module")
def db_session(setup_env, postgres):
    # Create an asynchronous engine
    engine = create_engine(postgres.url(), echo=True)

    # Create a new session
    with Session(engine) as session:
        # Start a transaction
        with session.begin():
            yield session  # Provide the session to the test

    # Close the engine
    engine.dispose()


@pytest.fixture
def db_user_json():
    return {
        "name": "test_name",
        "email": "test_email@test.com",
        "provider": "google",
        "external_user_id": "auth0|001123",
        "profile_picture": "http://test.com",
    }
