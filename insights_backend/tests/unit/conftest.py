import importlib
import os

import pytest
from _pytest.monkeypatch import MonkeyPatch
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlmodel import Session, SQLModel
from testing.postgresql import Postgresql

from insights_backend.db.config import MODEL_PATHS


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


@pytest.fixture(scope="session", autouse=True)
def setup_db(postgres):
    db_sync_uri = postgres.url()
    # Ensure tables are created
    engine = create_engine(db_sync_uri)
    # create schema
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS insights_store"))
        conn.commit()
    # create all models
    for model_path in MODEL_PATHS:
        importlib.import_module(model_path)
    SQLModel.metadata.create_all(engine)
    engine.dispose()
    yield db_sync_uri


@pytest.fixture(scope="function")
def db_session(postgres):
    # Create an asynchronous engine
    engine = create_engine(postgres.url(), echo=True)

    # Create a new session
    with Session(engine) as session:
        # Start a transaction
        with session.begin():
            yield session  # Provide the session to the test

    # Close the engine
    engine.dispose()


@pytest.fixture()
def db_user():
    return {
        "provider": "google",
        "external_user_id": "auth0|001123",
        "name": "test_name",
        "id": 1,
        "email": "test_email@test.com",
        "profile_picture": "http://test.com",
    }
