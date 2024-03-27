def test_health(client):
    response = client.get("/v1/health")
    res = response.json()
    assert response.status_code == 200
    assert res["graph_api_is_online"]
    assert res["cube_api_is_online"]
