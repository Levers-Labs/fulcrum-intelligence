from typing import Annotated

from fastapi import Depends

from query_manager.config import get_settings
from query_manager.services.parquet import ParquetService
from query_manager.services.query_client import QueryClient
from query_manager.services.s3 import S3Client


async def get_s3_client() -> S3Client:
    settings = get_settings()
    return S3Client(settings.AWS_BUCKET, region=settings.AWS_REGION)


S3ClientDep = Annotated[S3Client, Depends(get_s3_client)]


async def get_parquet_service(s3_client: S3ClientDep) -> ParquetService:
    return ParquetService(s3_client)


async def get_query_client(s3_client: S3ClientDep) -> QueryClient:
    return QueryClient(s3_client)


ParquetServiceDep = Annotated[ParquetService, Depends(get_parquet_service)]
QueryClientDep = Annotated[QueryClient, Depends(get_query_client)]
