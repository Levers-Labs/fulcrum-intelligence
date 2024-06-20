from typing import Annotated

from fastapi import Depends

from query_manager.config import Storage, get_settings
from query_manager.services.cube import CubeClient, CubeJWTAuthType
from query_manager.services.parquet import ParquetService
from query_manager.services.query_client import QueryClient
from query_manager.services.s3 import S3Client
from query_manager.services.supabase_client import SupabaseClient


async def get_s3_client() -> S3Client:
    settings = get_settings()
    return S3Client(settings.AWS_BUCKET, region=settings.AWS_REGION)


S3ClientDep = Annotated[S3Client, Depends(get_s3_client)]


async def get_cube_client() -> CubeClient:
    settings = get_settings()
    return CubeClient(
        str(settings.CUBE_API_URL),
        auth_type=CubeJWTAuthType.SECRET_KEY,
        auth_options={"secret_key": settings.SECRET_KEY},
    )


CubeClientDep = Annotated[CubeClient, Depends(get_cube_client)]


async def get_supabase_client() -> SupabaseClient:
    settings = get_settings()
    return SupabaseClient(settings.SUPABASE_BUCKET, settings.SUPABASE_API_URL, settings.SUPABASE_API_KEY)


SupabaseClientDep = Annotated[SupabaseClient, Depends(get_supabase_client)]


async def get_parquet_service(s3_client: S3ClientDep, supabase_client: SupabaseClientDep) -> ParquetService:
    settings = get_settings()
    if settings.STORAGE == Storage.SUPABASE:
        return ParquetService(supabase_client)
    else:
        return ParquetService(s3_client)


async def get_query_client(cube_client: CubeClientDep) -> QueryClient:
    return QueryClient(cube_client)


ParquetServiceDep = Annotated[ParquetService, Depends(get_parquet_service)]
QueryClientDep = Annotated[QueryClient, Depends(get_query_client)]
