import pytest


@pytest.mark.skip(reason="Not implemented")
def test_health_unavailable(client):
    response = client.get("/v1/health")
    assert response.status_code == 200
