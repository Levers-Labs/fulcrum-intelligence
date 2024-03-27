import pytest
from fastapi.testclient import TestClient

from query_manager.main import app


@pytest.fixture
def client():
    client = TestClient(app)
    return client
