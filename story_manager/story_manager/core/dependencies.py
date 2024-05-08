from __future__ import annotations

from typing import Annotated

from fastapi import Depends

from commons.clients.analysis_manager import AnalysisManagerClient
from commons.clients.query_manager import QueryManagerClient
from story_manager.config import get_settings
from story_manager.core.crud import CRUDStory
from story_manager.core.models import Story
from story_manager.db.config import AsyncSessionDep


async def get_stories_crud(session: AsyncSessionDep) -> CRUDStory:
    return CRUDStory(model=Story, session=session)


async def get_query_manager_client() -> QueryManagerClient:
    settings = get_settings()
    return QueryManagerClient(base_url=settings.QUERY_MANAGER_SERVER_HOST)


async def get_analysis_manager_client() -> AnalysisManagerClient:
    settings = get_settings()
    return AnalysisManagerClient(base_url=settings.ANALYSIS_MANAGER_SERVER_HOST)


CRUDStoryDep = Annotated[CRUDStory, Depends(get_stories_crud)]
QueryManagerClientDep = Annotated[QueryManagerClient, Depends(get_query_manager_client)]
AnalysisManagerClientDep = Annotated[AnalysisManagerClient, Depends(get_analysis_manager_client)]
