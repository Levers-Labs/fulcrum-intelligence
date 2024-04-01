import pytest


@pytest.mark.asyncio
async def test_convert_and_upload(parquet_service):
    # Await the async fixture to get the ParquetService instance
    parquet_service_instance = await parquet_service

    # Define test data and parameters
    test_data = [{"column1": "value1", "column2": "value2"}]
    metric_id = "test_metric"
    request_id = "test_request"

    # Execute the method under test
    result_url = await parquet_service_instance.convert_and_upload(test_data, metric_id, request_id)

    # Assertions
    parquet_service_instance.s3_client.upload_to_s3.assert_called_once()
    parquet_service_instance.s3_client.generate_presigned_url.assert_called_once()
    assert result_url == "http://mocked-presigned-url.com", "The returned URL should match the mocked presigned URL."
