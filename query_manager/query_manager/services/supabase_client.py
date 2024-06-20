import logging

from supabase import AClient

logger = logging.getLogger(__name__)


class SupabaseClient:
    def __init__(self, bucket_name: str, supabase_api_url: str, supabase_api_key: str):
        self.supabase = AClient(supabase_url=supabase_api_url, supabase_key=supabase_api_key)
        self.bucket_name = bucket_name
        self.bucket_obj = None

    async def get_async_bucket_obj(self):
        """
        All the files related operations are available on bucket object in supabase library
        This method is used to create and return that bucket object
        """
        if self.bucket_obj is None:
            self.bucket_obj = await self.supabase.storage.get_bucket(self.bucket_name)

        return self.bucket_obj

    async def upload_to_cloud_storage(self, file_path: str, destination_path: str):
        """
        Upload a file to supabase storage.
        :param file_path: path of the file to be uploaded
        :param destination_path: destination path of the file to be uploaded, including file name
        """
        with open(file_path, "rb") as f:
            bucket = await self.get_async_bucket_obj()
            await bucket.upload(file=f, path=destination_path, file_options={"cache-control": "3600", "upsert": "true"})
            logger.info(
                f"File {file_path} uploaded to supabase bucket {self.bucket_name}, "
                f"path on bucket : {destination_path}"
            )

    async def generate_presigned_url(self, file_key: str, expiration: int = 3600):
        """
        Generate a presigned URL to share a file stored on supabase storage.

        :param file_key: The path of the file on supabase storage.
        :param expiration: Time in seconds for the presigned URL to expire.
        :return: A pre-signed URL as a string.
        """
        bucket = await self.get_async_bucket_obj()
        res = await bucket.create_signed_url(file_key, expires_in=expiration)
        return res["signedURL"]
