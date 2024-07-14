import os

import pytest
from _pytest.monkeypatch import MonkeyPatch
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlmodel import Session
from testing.postgresql import Postgresql


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


@pytest.fixture(autouse=True, scope="session")
def setup_env(session_monkeypatch, postgres):  # noqa
    """
    Setup test environment
    """

    db_sync_uri = postgres.url()
    db_async_uri = db_sync_uri.replace("postgresql://", "postgresql+asyncpg://")
    session_monkeypatch.setenv("SERVER_HOST", "http://localhost:8004")
    session_monkeypatch.setenv("DEBUG", "true")
    session_monkeypatch.setenv("ENV", "dev")
    session_monkeypatch.setenv("SECRET_KEY", "secret")
    session_monkeypatch.setenv("BACKEND_CORS_ORIGINS", '["http://localhost"]')
    session_monkeypatch.setenv("DATABASE_URL", db_async_uri)
    yield


@pytest.fixture(scope="session")
def client(setup_env):
    # Import only after setting up the environment
    from insights_backend.main import app  # noqa

    with TestClient(app) as client:
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
