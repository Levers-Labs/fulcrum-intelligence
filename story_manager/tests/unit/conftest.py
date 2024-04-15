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
    session_monkeypatch.setenv("SERVER_HOST", "http://localhost:8002")
    session_monkeypatch.setenv("DEBUG", "true")
    session_monkeypatch.setenv("ENV", "dev")
    session_monkeypatch.setenv("SECRET_KEY", "secret")
    session_monkeypatch.setenv("BACKEND_CORS_ORIGINS", '["http://localhost"]')
    session_monkeypatch.setenv("QUERY_MANAGER_SERVER_HOST", "http://localhost:8001/v1/")
    session_monkeypatch.setenv("ANALYSIS_MANAGER_SERVER_HOST", "http://localhost:8000/v1/")
    session_monkeypatch.setenv("DATABASE_URL", db_uri)
    yield


@pytest.fixture(scope="session")
def client(setup_env, test_db, session_monkeypatch):  # noqa
    # Import only after setting up the environment
    from story_manager.main import app  # noqa

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
