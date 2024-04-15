import pytest

from story_manager.config import Settings


def test_assemble_cors_origins():
    assert Settings.assemble_cors_origins("http://localhost:8000") == ["http://localhost:8000"]
    assert Settings.assemble_cors_origins(["http://localhost:8000"]) == ["http://localhost:8000"]
    assert Settings.assemble_cors_origins("http://localhost:8000, http://localhost:8001") == [
        "http://localhost:8000",
        "http://localhost:8001",
    ]
    assert Settings.assemble_cors_origins(["http://localhost:8000", "http://localhost:8001"]) == [
        "http://localhost:8000",
        "http://localhost:8001",
    ]
    with pytest.raises(ValueError):
        Settings.assemble_cors_origins(123)
