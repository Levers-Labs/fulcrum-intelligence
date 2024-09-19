from unittest.mock import AsyncMock, patch

import pytest
from fastapi import Request, Response

from commons.middleware import context_middleware, process_time_log_middleware, request_id_middleware


@pytest.mark.asyncio
async def test_request_id_middleware():
    # Create a mock request object
    request = Request(scope={"type": "http"})
    request.state._state = {}

    # Mock the call_next function which calls the next middleware or endpoint
    call_next = AsyncMock(return_value=Response())

    # Call the middleware
    response = await request_id_middleware(request, call_next)

    # Assert call_next was called
    call_next.assert_called_once_with(request)

    # Check if the 'X-Request-ID' is in the response headers
    assert "X-Request-ID" in response.headers, "Response should have 'X-Request-ID' header"

    # Check if the 'X-Request-ID' is the same in the state of the request and the header of the response
    request_id_from_request = request.state.request_id
    request_id_from_response = response.headers["X-Request-ID"]
    assert request_id_from_request == request_id_from_response

    # Optionally, assert the format of the UUID
    import uuid

    assert isinstance(uuid.UUID(request_id_from_request), uuid.UUID), "Request ID should be a valid UUID"


@pytest.mark.asyncio
async def test_process_time_log_middleware():
    # Mock Request
    request = Request(
        scope={
            "type": "http",
            "method": "GET",
            "path": "/test",
            "headers": [
                (b"host", b"testserver"),  # Example header; adjust as needed
            ],
        }
    )
    request.state._state = {}

    # Mock Response
    response = Response()

    # Mock call_next to always return our mock response
    call_next = AsyncMock(return_value=response)

    # Mock time and logging
    with patch("time.time") as mock_time, patch("commons.middleware.time_log.logger.info") as mock_logger:
        mock_time.side_effect = [100.0, 100.5]  # Simulates a 0.5 second processing time
        # Execute middleware
        result = await process_time_log_middleware(request, call_next)

        # Verify the response is the mocked response
        assert result is response, "Middleware should return response from call_next"

        # Check if X-Process-Time is set correctly
        assert result.headers["X-Process-Time"] == "0.5", "X-Process-Time should reflect the process duration"

        # Check logger calls
        mock_logger.assert_called_once_with(
            "Method=%s Path=%s StatusCode=%s ProcessTime=%s",
            request.method,
            request.url.path,
            result.status_code,
            "0.5",
        )


@pytest.mark.asyncio
async def test_context_middleware():
    # Create a mock request object
    headers = [("X-Tenant-Id", "1")]
    request = Request(scope={"type": "http", "headers": headers})
    request.state._state = {}

    # Mock the call_next function which calls the next middleware or endpoint
    call_next = AsyncMock(return_value=Response())

    # Call the middleware
    await context_middleware(request, call_next)

    # Assert call_next was called
    call_next.assert_called_once_with(request)
