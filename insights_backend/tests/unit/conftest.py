import importlib
import logging
import os

import fastapi
import pytest
from _pytest.monkeypatch import MonkeyPatch
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlmodel import Session, SQLModel
from testing.postgresql import Postgresql

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
        "tenant_id": 1,
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

    # for multitenancy support, we need tenant_user role, creating that role and granting privileges to that role.
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS insights_store"))
        conn.execute(
            text(
                """
            DO $$
            BEGIN
            IF NOT EXISTS(SELECT * FROM pg_roles WHERE rolname = 'tenant_user') THEN
                CREATE ROLE tenant_user;
                GRANT USAGE ON SCHEMA insights_store TO tenant_user;
                GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA insights_store TO tenant_user;
                ALTER DEFAULT PRIVILEGES IN SCHEMA insights_store GRANT SELECT, INSERT, UPDATE, DELETE
                ON TABLES TO tenant_user;
                GRANT USAGE ON ALL SEQUENCES IN SCHEMA insights_store TO tenant_user;
                ALTER DEFAULT PRIVILEGES IN SCHEMA insights_store GRANT USAGE ON SEQUENCES TO tenant_user;
            END IF;
            END
            $$
            """
            )
        )
        conn.commit()

    # create all models
    for model_path in MODEL_PATHS:
        importlib.import_module(model_path)

    SQLModel.metadata.create_all(engine)
    with engine.connect() as conn:
        # Insert a record in tenant table, which is required for user model testing.
        conn.execute(
            text(
                """
                INSERT INTO insights_store.tenant (name, description, domains)
                VALUES (:tenant, :description, :domains)
                """
            ),
            {"tenant": "test_tenant", "description": "some desc", "domains": '["abc.com", "cds.com"]'},
        )
        conn.commit()

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


@pytest.fixture(scope="session")
def token():
    return (
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImZWT3owbEM0Q1lNTjduaVlpejRSViJ9.eyJpc3MiOiJodHRwczovL2luc2lna"
        "HRzLXdlYi1hcHAudXMuYXV0aDAuY29tLyIsInN1YiI6IlBOMEN0SkFTbE1EbTlURWl2YjNpenNEbklmNWRjRllBQGNsaWVudHMiLCJhd"
        "WQiOiJodHRwczovL2Z1bGNydW1fYXBpX2lkZW50aWZpZXIiLCJpYXQiOjE3MjExMjIxNzAsImV4cCI6MTcyMTIwODU3MCwic2NvcGUiOiJ"
        "1c2VyLXJlYWQgcXVlcnlfbWFuYWdlcjoqIGFuYWx5c2lzX21hbmFnZXI6KiBzdG9yeV9tYW5hZ2VyOioiLCJndHkiOiJjbGllbnQtY3Jl"
        "ZGVudGlhbHMiLCJhenAiOiJQTjBDdEpBU2xNRG05VEVpdmIzaXpzRG5JZjVkY0ZZQSJ9.tMph01mxSQJcPjvNUqVVkWbaThCcN2DqVMQe"
        "rmO3pFKX2lE2WeaFYTH11h1_xHR1HX2UPyytBYYnMQ9PYXhKqUlhVGnYtpYljPl_g0Qpoa4mtWzd5vNjjOG0flfCFRiOs7L_9BrzFHkcdF"
        "79C-CT00yqvym9scoqNuR1RdmQDdlW4wpsOBV7xlvJ5ualOD4nvRFIOohC496AxAwjA4FqOXUOGkFzyaDwvJaSBNIHKyVHv1YmQjcKATm"
        "m4n6J8s7lGaGkdi5ZCAAGafiecpYG65MDb4m2D4rGzVLPjd4aK-_I2hiZUgxHYJTdprbWyd8beX-lnaIBfkA_2PDVgK23dg"
    )


@pytest.fixture(scope="session")
def client(setup_env, token):
    # Import only after setting up the environment
    from insights_backend.main import app  # noqa

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
        "tenant_id": 1,
        "name": "test_name",
        "email": "test_email@test.com",
        "provider": "google",
        "external_user_id": "auth0|001123",
        "profile_picture": "http://test.com",
    }
