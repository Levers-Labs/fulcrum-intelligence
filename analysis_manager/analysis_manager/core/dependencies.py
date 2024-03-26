from typing import Annotated

from fastapi import Depends

from analysis_manager.config import settings
from analysis_manager.core.crud import CRUDUser
from analysis_manager.core.models import User
from analysis_manager.db.config import AsyncSessionDep
from analysis_manager.services.query_manager_client import QueryManagerClient
from fulcrum_core.analysis_manager import AnalysisManager


async def get_users_crud(session: AsyncSessionDep) -> CRUDUser:
    return CRUDUser(model=User, session=session)


async def get_query_manager_client() -> QueryManagerClient:
    return QueryManagerClient(settings.QUERY_MANAGER_SERVER_HOST)


async def get_analysis_manager() -> AnalysisManager:
    return AnalysisManager()


UsersCRUDDep = Annotated[CRUDUser, Depends(get_users_crud)]
QueryManagerClientDep = Annotated[QueryManagerClient, Depends(get_query_manager_client)]
AnalysisManagerDep = Annotated[AnalysisManager, Depends(get_analysis_manager)]
