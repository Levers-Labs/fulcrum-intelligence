from __future__ import annotations

from typing import Annotated

from fastapi import Depends

from commons.auth.auth import Oauth2Auth
from commons.clients.analysis_manager import AnalysisManagerClient
from commons.clients.auth import ClientCredsAuth
from commons.clients.query_manager import QueryManagerClient
from fulcrum_core import AnalysisManager
from story_manager.config import get_settings
from story_manager.core.crud import CRUDStory
from story_manager.core.models import Story
from story_manager.db.config import AsyncSessionDep


async def get_stories_crud(session: AsyncSessionDep) -> CRUDStory:
    return CRUDStory(model=Story, session=session)


async def get_query_manager_client() -> QueryManagerClient:
    settings = get_settings()
    return QueryManagerClient(
        base_url=settings.QUERY_MANAGER_SERVER_HOST,
        auth=ClientCredsAuth(
            auth0_issuer=settings.AUTH0_ISSUER,
            client_id=settings.AUTH0_CLIENT_ID,
            client_secret=settings.AUTH0_CLIENT_SECRET,
            api_audience=settings.AUTH0_API_AUDIENCE,
        ),
    )


async def get_analysis_manager_client() -> AnalysisManagerClient:
    settings = get_settings()
    return AnalysisManagerClient(
        base_url=settings.ANALYSIS_MANAGER_SERVER_HOST,
        auth=ClientCredsAuth(
            auth0_issuer=settings.AUTH0_ISSUER,
            client_id=settings.AUTH0_CLIENT_ID,
            client_secret=settings.AUTH0_CLIENT_SECRET,
            api_audience=settings.AUTH0_API_AUDIENCE,
        ),
    )


def oauth2_auth() -> Oauth2Auth:
    settings = get_settings()
    return Oauth2Auth(
        issuer=settings.AUTH0_ISSUER,
        api_audience=settings.AUTH0_API_AUDIENCE,
        insights_backend_host=settings.INSIGHTS_BACKEND_SERVER_HOST,
    )


async def get_analysis_manager() -> AnalysisManager:
    return AnalysisManager()


CRUDStoryDep = Annotated[CRUDStory, Depends(get_stories_crud)]
QueryManagerClientDep = Annotated[QueryManagerClient, Depends(get_query_manager_client)]
AnalysisManagerClientDep = Annotated[AnalysisManagerClient, Depends(get_analysis_manager_client)]
AnalysisManagerDep = Annotated[AnalysisManager, Depends(get_analysis_manager)]
