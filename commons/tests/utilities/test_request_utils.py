from fastapi import Request

from commons.utilities.request_utils import get_referer


def test_get_referer_with_valid_url():
    mock_request = Request(
        scope={
            "type": "http",
            "headers": [
                (b"referer", b"https://example.com/path"),
            ],
        }
    )
    result = get_referer(mock_request)
    assert result == "https://example.com"


def test_get_referer_with_subdomain():
    mock_request = Request(
        scope={
            "type": "http",
            "headers": [
                (b"referer", b"https://sub.example.com/path"),
            ],
        }
    )
    result = get_referer(mock_request)
    assert result == "https://sub.example.com"


def test_get_referer_with_port():
    mock_request = Request(
        scope={
            "type": "http",
            "headers": [
                (b"referer", b"http://localhost:8000/path"),
            ],
        }
    )
    result = get_referer(mock_request)
    assert result == "http://localhost:8000"


def test_get_referer_with_query_params():
    mock_request = Request(
        scope={
            "type": "http",
            "headers": [
                (b"referer", b"https://example.com/path?param=value"),
            ],
        }
    )
    result = get_referer(mock_request)
    assert result == "https://example.com"


def test_get_referer_with_no_path():
    mock_request = Request(
        scope={
            "type": "http",
            "headers": [
                (b"referer", b"https://example.com"),
            ],
        }
    )
    result = get_referer(mock_request)
    assert result == "https://example.com"
