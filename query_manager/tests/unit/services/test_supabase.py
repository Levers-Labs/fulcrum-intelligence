from unittest.mock import (
    ANY,
    AsyncMock,
    MagicMock,
    mock_open,
    patch,
)

import pytest

from query_manager.services.supabase_client import SupabaseClient


@pytest.mark.asyncio
async def test_upload_to_cloud_storage():
    mock_bucket = AsyncMock(name="mock_bucket")
    mock_get_bucket = AsyncMock(return_value=mock_bucket)
    mock_client = MagicMock()
    mock_client.return_value.storage.get_bucket = mock_get_bucket

    with patch("query_manager.services.supabase_client.AClient", mock_client):
        # Create SupabaseClient instance
        supabase_client = SupabaseClient("mybucket", "http://mock_api_url", "mock_api_key")

        # Mock file data
        file_data = b"Mock file content"

        # Patch open to mock the file opening
        with patch("builtins.open", mock_open(read_data=file_data)):
            await supabase_client.upload_to_cloud_storage("path/to/local/file", "supabase/key")

            # Assert that get_bucket method was called with correct bucket_name
            mock_get_bucket.assert_awaited_once_with("mybucket")

            # Assert that bucket.upload was called with correct parameters
            mock_bucket.upload.assert_awaited_once_with(
                file=ANY, path="supabase/key", file_options={"cache-control": "3600", "upsert": "true"}
            )


@pytest.mark.asyncio
@patch("query_manager.services.supabase_client.AClient")
async def test_generate_presigned_url(mock_client):
    # Mock objects setup
    mock_bucket = AsyncMock(name="mock_bucket")
    mock_get_bucket = AsyncMock(return_value=mock_bucket)
    mock_supabase = mock_client.return_value
    mock_supabase.storage.get_bucket = mock_get_bucket

    # Mock generate_presigned_url to return a specific URL
    expected_url = "http://mocked-url.com"
    mock_bucket.create_signed_url = AsyncMock(return_value={"signedURL": expected_url})

    # Instantiate SupabaseClient
    supabase_client = SupabaseClient("mybucket", "http://mock_api_url", "mock_api_key")

    # Call generate_presigned_url method
    file_key = "supabase/key"
    url = await supabase_client.generate_presigned_url(file_key)

    # Assertions
    assert url == expected_url
