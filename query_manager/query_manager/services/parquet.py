import os
import tempfile

import pandas as pd

from query_manager.services.s3 import S3Client
from query_manager.services.supabase_client import SupabaseClient


class ParquetService:

    def __init__(self, client: S3Client | SupabaseClient):
        self.client = client

    async def convert_and_upload(self, data, metric_id, request_id, folder="values") -> str:
        """
        Convert the data to Parquet format and upload it to S3.

        :param data: The data to be converted and uploaded.
        :param metric_id: The ID of the metric.
        :param request_id: The ID of the request.
        :param folder: The folder where the data would be stored, it would be either values or targets.
        :return: The URL of the uploaded Parquet file.
        """
        # Use a temporary directory to store the Parquet file
        with tempfile.TemporaryDirectory() as tmpdir:
            # Convert data to Parquet
            df = pd.DataFrame(data)
            # Construct the file path within the temporary directory
            file_path = os.path.join(tmpdir, f"{metric_id}_{request_id}.parquet")
            df.to_parquet(file_path)

            # Upload to S3
            file_key = f"metric/{metric_id}/{folder}/{os.path.basename(file_path)}"
            await self.client.upload_to_cloud_storage(file_path, file_key)
            # Generate and return the S3 URL
            file_url = await self.client.generate_presigned_url(file_key)

        return file_url
