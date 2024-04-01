import json
import logging

import aioboto3
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class NoSuchKeyError(Exception):
    """Exception raised when an S3 object key does not exist."""

    def __init__(self, key: str):
        self.key = key
        self.message = f"No such key found in S3: {key}"
        super().__init__(self.message)


class S3Client:
    def __init__(self, bucket: str, region: str):
        self.bucket = bucket
        self.region = region
        self.session = aioboto3.Session()
        self.boto_session = boto3.Session()

    async def upload_to_s3(self, file_path: str, s3_key: str):
        """
        Asynchronously uploads a file to an S3 bucket.

        :param file_path: The local path to the file to be uploaded.
        :param s3_key: The destination key within the S3 bucket.
        """
        # Use the aioboto3 session to create an asynchronous S3 client
        async with self.session.client("s3") as s3_client:
            # Asynchronously upload the file to the specified S3 key
            await s3_client.upload_file(file_path, self.bucket, s3_key)
            logger.info("File %s uploaded to s3://%s/%s", file_path, self.bucket, s3_key)

    def generate_presigned_url(self, s3_key: str, expiration=3600) -> str:
        """
        Generate a presigned URL to share an S3 object.

        :param s3_key: The S3 object key for which to generate the presigned URL.
        :param expiration: Time in seconds for the presigned URL to expire.
        :return: A pre-signed URL as a string.
        """
        s3_client = self.boto_session.client("s3", region_name=self.region)
        return s3_client.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": s3_key}, ExpiresIn=expiration
        )

    async def query_s3_json(self, key: str, sql_expression: str) -> list[dict]:
        """
        Executes an SQL query on a JSON file stored in S3 and returns matching rows asynchronously.

        :param key: The S3 key for the JSON file.
        :param sql_expression: The SQL expression to filter the data.
        :return: A list containing the rows that match the query.
        :raises NoSuchKeyError: If the specified key does not exist in S3.
        """
        all_records = []
        all_payload_chunks = []
        async with self.session.client("s3") as s3_client:
            try:
                response = await s3_client.select_object_content(
                    Bucket=self.bucket,
                    Key=key,
                    ExpressionType="SQL",
                    Expression=sql_expression,
                    InputSerialization={"JSON": {"Type": "DOCUMENT"}},
                    OutputSerialization={"JSON": {}},
                )

                async for event in response["Payload"]:
                    if "Records" in event:
                        # Accumulate payload chunks
                        all_payload_chunks.append(event["Records"]["Payload"])
                    elif "End" in event:
                        # When the 'End' event is received, concatenate all chunks and decode
                        full_payload = b"".join(all_payload_chunks).decode("utf-8")
                        # Split the payload by newlines to process individual records
                        for record in full_payload.strip().split("\n"):
                            if record:  # Ensure the record is not empty
                                record_data = json.loads(record)
                                if "_1" in record_data:
                                    all_records.extend(record_data["_1"])
                                else:
                                    all_records.extend(record_data)
            except ClientError as e:
                error_code = e.response["Error"]["Code"]
                if error_code == "NoSuchKey":
                    raise NoSuchKeyError(key) from e
                else:
                    raise  # Re-raise the exception if it's not a NoSuchKey error

        return all_records
