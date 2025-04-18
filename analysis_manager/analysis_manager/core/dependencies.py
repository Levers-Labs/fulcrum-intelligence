from __future__ import annotations

from typing import Annotated

from fastapi import Depends

from analysis_manager.config import get_settings
from analysis_manager.core.services.component_drift import ComponentDriftService
from commons.auth.auth import Oauth2Auth
from commons.clients.auth import ClientCredsAuth
from commons.clients.query_manager import QueryManagerClient
from fulcrum_core.analysis_manager import AnalysisManager


async def get_query_manager_client() -> QueryManagerClient:
    settings = get_settings()
    return QueryManagerClient(
        settings.QUERY_MANAGER_SERVER_HOST,
        auth=ClientCredsAuth(
            auth0_issuer=settings.AUTH0_ISSUER,
            client_id=settings.AUTH0_CLIENT_ID,
            client_secret=settings.AUTH0_CLIENT_SECRET,
            api_audience=settings.AUTH0_API_AUDIENCE,
        ),
    )


async def get_analysis_manager() -> AnalysisManager:
    return AnalysisManager()


def oauth2_auth() -> Oauth2Auth:
    settings = get_settings()
    return Oauth2Auth(issuer=settings.AUTH0_ISSUER, api_audience=settings.AUTH0_API_AUDIENCE)


async def get_component_drift_service(
    analysis_manager: AnalysisManagerDep, query_manager: QueryManagerClientDep
) -> ComponentDriftService:
    return ComponentDriftService(analysis_manager, query_manager)


QueryManagerClientDep = Annotated[QueryManagerClient, Depends(get_query_manager_client)]
AnalysisManagerDep = Annotated[AnalysisManager, Depends(get_analysis_manager)]
ComponentDriftServiceDep = Annotated[ComponentDriftService, Depends(get_component_drift_service)]
