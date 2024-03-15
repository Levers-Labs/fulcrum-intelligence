from typing import Annotated

from fastapi import Depends
from fulcrum_core.analysis_manager import AnalysisManager

from app.config import settings
from app.core.crud import CRUDUser
from app.core.models import User
from app.db.config import AsyncSessionDep
from app.services.query_manager_client import QueryManagerClient


async def get_users_crud(session: AsyncSessionDep) -> CRUDUser:
    return CRUDUser(model=User, session=session)


async def get_query_manager_client() -> QueryManagerClient:
    return QueryManagerClient(settings.QUERY_MANAGER_SERVER_HOST)


async def get_analysis_manager() -> AnalysisManager:
    return AnalysisManager()


UsersCRUDDep = Annotated[CRUDUser, Depends(get_users_crud)]
QueryManagerClientDep = Annotated[QueryManagerClient, Depends(get_query_manager_client)]
AnalysisManagerDep = Annotated[AnalysisManager, Depends(get_analysis_manager)]
