from typing import Annotated

from fastapi import Depends

from commons.auth.auth import Oauth2Auth
from commons.clients.auth import ClientCredsAuth
from commons.clients.insight_backend import InsightBackendClient
from query_manager.config import get_settings
from query_manager.core.crud import CRUDDimensions, CRUDMetric, CRUDMetricNotifications
from query_manager.core.models import Dimension, Metric, MetricNotifications
from query_manager.db.config import AsyncSessionDep
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
    cube_connection_config = tenant_config.get("cube_connection_config", {})
    auth_type = CubeJWTAuthType(cube_connection_config.get("cube_auth_type", ""))
    auth_options = (
        dict(secret_key=cube_connection_config.get("cube_auth_secret_key", ""))
        if auth_type == CubeJWTAuthType.SECRET_KEY
        else dict(token=cube_connection_config.get("cube_auth_token", ""))
    )
    return CubeClient(cube_connection_config.get("cube_api_url", ""), auth_type=auth_type, auth_options=auth_options)


CubeClientDep = Annotated[CubeClient, Depends(get_cube_client)]


async def get_parquet_service(s3_client: S3ClientDep) -> ParquetService:
    return ParquetService(s3_client)


async def get_dimensions_crud(session: AsyncSessionDep) -> CRUDDimensions:
    return CRUDDimensions(model=Dimension, session=session)


async def get_metric_crud(session: AsyncSessionDep) -> CRUDMetric:
    return CRUDMetric(model=Metric, session=session)


CRUDDimensionDep = Annotated[CRUDDimensions, Depends(get_dimensions_crud)]
CRUDMetricDep = Annotated[CRUDMetric, Depends(get_metric_crud)]


async def get_query_client(
    cube_client: CubeClientDep, dimensions_crud: CRUDDimensionDep, metric_crud: CRUDMetricDep
) -> QueryClient:
    return QueryClient(cube_client, dimensions_crud, metric_crud)


def oauth2_auth() -> Oauth2Auth:
    settings = get_settings()
    return Oauth2Auth(issuer=settings.AUTH0_ISSUER, api_audience=settings.AUTH0_API_AUDIENCE)


async def get_metric_notification_crud(session: AsyncSessionDep) -> CRUDMetricNotifications:
    return CRUDMetricNotifications(model=MetricNotifications, session=session)


ParquetServiceDep = Annotated[ParquetService, Depends(get_parquet_service)]
QueryClientDep = Annotated[QueryClient, Depends(get_query_client)]
CRUDMetricNotificationsDep = Annotated[CRUDMetricNotifications, Depends(get_metric_notification_crud)]
