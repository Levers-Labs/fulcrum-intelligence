from unittest.mock import AsyncMock, patch

import pytest
from botocore.exceptions import ClientError

from query_manager.services.s3 import NoSuchKeyError, S3Client


@pytest.mark.asyncio
@patch("query_manager.services.s3.aioboto3.Session.client")
async def test_upload_to_cloud_storage(mock_client):
    mock_s3 = AsyncMock(name="mock_s3")
    mock_client.return_value.__aenter__.return_value = mock_s3

    s3_client = S3Client("mybucket", "us-east-1")
    await s3_client.upload_to_cloud_storage("path/to/local/file", "s3/key")

    mock_s3.upload_file.assert_awaited_with("path/to/local/file", "mybucket", "s3/key")


@pytest.mark.asyncio
@patch("query_manager.services.s3.boto3.Session.client")
async def test_generate_presigned_url(mock_client):
    mock_s3 = mock_client.return_value
    mock_s3.generate_presigned_url.return_value = "http://mocked-url.com"

    s3_client = S3Client("mybucket", "us-east-1")
    url = await s3_client.generate_presigned_url("s3/key")

    assert url == "http://mocked-url.com"
    mock_s3.generate_presigned_url.assert_called_with(
        "get_object", Params={"Bucket": "mybucket", "Key": "s3/key"}, ExpiresIn=3600
    )


@pytest.mark.asyncio
@patch("query_manager.services.s3.aioboto3.Session.client")
async def test_query_s3_json(mock_client):
    mock_s3 = AsyncMock(name="mock_s3")
    mock_client.return_value.__aenter__.return_value = mock_s3

    # Prepare a more detailed mock response that simulates the actual event stream structure
    mock_event_stream = AsyncMock()
    mock_event_stream.__aiter__.return_value = iter(
        [
            {"Records": {"Payload": b'{"_1": [{"some": "json"}'}},
            {"Records": {"Payload": b',{"more": "data"}]}'}},
            {"End": {}},  # Mocked End event to signal the end of the event stream
        ]
    )

    # mock select_object_content
    mock_s3.select_object_content.return_value = {"Payload": mock_event_stream}

    s3_client = S3Client("mybucket", "us-east-1")
    result = await s3_client.query_s3_json("s3/key", "SQL expression")

    # Adjust the assertion to match the expected structure from the mock payload
    assert result == [{"some": "json"}, {"more": "data"}]
    mock_s3.select_object_content.assert_awaited()

    # without -1
    mock_event_stream.__aiter__.return_value = iter(
        [
            {"Records": {"Payload": b'[{"some": "json"}'}},
            {"Records": {"Payload": b',{"more": "data"}]'}},
            {"End": {}},  # Mocked End event to signal the end of the event stream
        ]
    )
    result = await s3_client.query_s3_json("s3/key", "SQL expression")
    assert result == [{"some": "json"}, {"more": "data"}]
    mock_s3.select_object_content.assert_awaited()


@pytest.mark.asyncio
@patch("query_manager.services.s3.aioboto3.Session.client")
async def test_query_s3_json_no_such_key(mock_client):
    mock_s3 = AsyncMock(name="mock_s3")
    mock_client.return_value.__aenter__.return_value = mock_s3

    # Simulate a ClientError for NoSuchKey
    error_response = {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}}
    mock_s3.select_object_content.side_effect = ClientError(error_response, "SelectObjectContent")

    s3_client = S3Client("mybucket", "us-east-1")

    # Call the method under test and expect NoSuchKeyError
    with pytest.raises(NoSuchKeyError) as exc_info:
        await s3_client.query_s3_json("nonexistent/key", "SQL expression")

    # Verify the exception message contains the nonexistent key
    assert "nonexistent/key" in str(exc_info.value)
    mock_s3.select_object_content.assert_awaited()

    # Optionally, verify the specifics of the ClientError raised
    assert exc_info.value.key == "nonexistent/key"

    # raise a different error
    mock_s3.select_object_content.side_effect = ClientError({"Error": {"Code": "OtherError"}}, "SelectObjectContent")
    with pytest.raises(ClientError):
        await s3_client.query_s3_json("nonexistent/key", "SQL expression")

    mock_s3.select_object_content.assert_awaited()
