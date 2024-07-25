from typing import Annotated

from fastapi import Depends

from query_manager.config import get_settings
from query_manager.core.crud import CRUDDimensions
from query_manager.core.models import Dimensions
from query_manager.db.config import AsyncSessionDep
from query_manager.services.cube import CubeClient, CubeJWTAuthType
from query_manager.services.parquet import ParquetService
from query_manager.services.query_client import QueryClient
from query_manager.services.s3 import S3Client


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


async def get_parquet_service(s3_client: S3ClientDep) -> ParquetService:
    return ParquetService(s3_client)


async def get_query_client(cube_client: CubeClientDep, session: AsyncSessionDep) -> QueryClient:
    return QueryClient(cube_client, session=session)


async def get_dimensions_crud(session: AsyncSessionDep) -> CRUDDimensions:
    return CRUDDimensions(model=Dimensions, session=session)


ParquetServiceDep = Annotated[ParquetService, Depends(get_parquet_service)]
QueryClientDep = Annotated[QueryClient, Depends(get_query_client)]
CRUDDimensionDep = Annotated[CRUDDimensions, Depends(get_dimensions_crud)]
