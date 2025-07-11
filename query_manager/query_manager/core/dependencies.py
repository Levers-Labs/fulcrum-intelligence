from typing import Annotated

from fastapi import Depends

from commons.auth.auth import Oauth2Auth
from commons.clients.auth import ClientCredsAuth
from commons.clients.insight_backend import InsightBackendClient
from commons.llm.provider import LLMProvider
from commons.llm.settings import LLMSettings, get_llm_settings
from query_manager.config import get_settings
from query_manager.core.crud import (
    CRUDDimensions,
    CRUDMetric,
    CRUDMetricCacheConfig,
    CRUDMetricCacheGrainConfig,
)
from query_manager.core.models import (
    Dimension,
    Metric,
    MetricCacheConfig,
    MetricCacheGrainConfig,
)
from query_manager.db.config import AsyncSessionDep
from query_manager.llm.services.expression_parser import ExpressionParserService
from query_manager.services.cube import CubeClient, CubeJWTAuthType
from query_manager.services.parquet import ParquetService
from query_manager.services.query_client import QueryClient
from query_manager.services.s3 import S3Client


async def get_s3_client() -> S3Client:
    settings = get_settings()
    return S3Client(settings.AWS_BUCKET, region=settings.AWS_REGION)


S3ClientDep = Annotated[S3Client, Depends(get_s3_client)]


async def get_insights_backend_client() -> InsightBackendClient:
    settings = get_settings()
    return InsightBackendClient(
        settings.INSIGHTS_BACKEND_SERVER_HOST,
        auth=ClientCredsAuth(
            auth0_issuer=settings.AUTH0_ISSUER,
            client_id=settings.AUTH0_CLIENT_ID,
            client_secret=settings.AUTH0_CLIENT_SECRET,
            api_audience=settings.AUTH0_API_AUDIENCE,
        ),
    )


InsightBackendClientDep = Annotated[InsightBackendClient, Depends(get_insights_backend_client)]


async def get_cube_client(insights_backend_client: InsightBackendClientDep) -> CubeClient:
    tenant_config = await insights_backend_client.get_tenant_config()
    if tenant_config["cube_connection_config"] is None:
        raise ValueError("Cube connection config is not configured for tenant")
    cube_connection_config = tenant_config["cube_connection_config"]
    auth_type = CubeJWTAuthType(cube_connection_config["cube_auth_type"])
    auth_options = (
        dict(secret_key=cube_connection_config["cube_auth_secret_key"])
        if auth_type == CubeJWTAuthType.SECRET_KEY
        else dict(token=cube_connection_config["cube_auth_token"])
    )
    return CubeClient(cube_connection_config["cube_api_url"], auth_type=auth_type, auth_options=auth_options)


CubeClientDep = Annotated[CubeClient, Depends(get_cube_client)]


async def get_parquet_service(s3_client: S3ClientDep) -> ParquetService:
    return ParquetService(s3_client)


async def get_dimensions_crud(session: AsyncSessionDep) -> CRUDDimensions:
    return CRUDDimensions(model=Dimension, session=session)


async def get_metric_crud(session: AsyncSessionDep) -> CRUDMetric:
    return CRUDMetric(model=Metric, session=session)


async def get_metric_cache_grain_config_crud(session: AsyncSessionDep) -> CRUDMetricCacheGrainConfig:
    """
    CRUD for MetricCacheGrainConfig Model.
    """
    return CRUDMetricCacheGrainConfig(model=MetricCacheGrainConfig, session=session)


async def get_metric_cache_config_crud(session: AsyncSessionDep) -> CRUDMetricCacheConfig:
    """
    CRUD for MetricCacheConfig Model.
    """
    return CRUDMetricCacheConfig(model=MetricCacheConfig, session=session)


CRUDDimensionDep = Annotated[CRUDDimensions, Depends(get_dimensions_crud)]
CRUDMetricDep = Annotated[CRUDMetric, Depends(get_metric_crud)]
CRUDMetricCacheGrainConfigDep = Annotated[CRUDMetricCacheGrainConfig, Depends(get_metric_cache_grain_config_crud)]
CRUDMetricCacheConfigDep = Annotated[CRUDMetricCacheConfig, Depends(get_metric_cache_config_crud)]


async def get_query_client(
    cube_client: CubeClientDep, dimensions_crud: CRUDDimensionDep, metric_crud: CRUDMetricDep
) -> QueryClient:
    return QueryClient(cube_client, dimensions_crud, metric_crud)


def oauth2_auth() -> Oauth2Auth:
    settings = get_settings()
    return Oauth2Auth(issuer=settings.AUTH0_ISSUER, api_audience=settings.AUTH0_API_AUDIENCE)


ParquetServiceDep = Annotated[ParquetService, Depends(get_parquet_service)]
QueryClientDep = Annotated[QueryClient, Depends(get_query_client)]


async def get_expression_parser_service(
    settings: Annotated[LLMSettings, Depends(get_llm_settings)], metric_crud: CRUDMetricDep
) -> ExpressionParserService:
    provider = LLMProvider.from_settings(settings)
    metric_ids = await metric_crud.get_all_metric_ids()
    return ExpressionParserService(provider=provider, metric_ids=metric_ids)


ExpressionParserServiceDep = Annotated[ExpressionParserService, Depends(get_expression_parser_service)]
