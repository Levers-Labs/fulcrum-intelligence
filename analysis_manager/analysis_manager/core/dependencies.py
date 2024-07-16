from __future__ import annotations

from typing import Annotated

from fastapi import Depends

from analysis_manager.config import settings
from analysis_manager.core.crud import CRUDUser
from analysis_manager.core.models import User
from analysis_manager.core.services.component_drift import ComponentDriftService
from analysis_manager.db.config import AsyncSessionDep
from commons.auth.auth import Auth
from commons.auth.machine_to_machine_auth import M2MAuth
from commons.clients.insight_backend import InsightBackendClient
from commons.clients.query_manager import QueryManagerClient
from fulcrum_core.analysis_manager import AnalysisManager


async def get_users_crud(session: AsyncSessionDep) -> CRUDUser:
    return CRUDUser(model=User, session=session)


async def get_query_manager_client() -> QueryManagerClient:
    return QueryManagerClient(settings.QUERY_MANAGER_SERVER_HOST, auth=M2MAuth(settings))


async def get_analysis_manager() -> AnalysisManager:
    return AnalysisManager()


def get_insight_backend_client() -> InsightBackendClient:
    return InsightBackendClient(settings.INSIGHTS_BACKEND_SERVER_HOST)


def get_security_obj() -> Auth:
    return Auth(settings, get_insight_backend_client())


async def get_component_drift_service(
    analysis_manager: AnalysisManagerDep, query_manager: QueryManagerClientDep
) -> ComponentDriftService:
    return ComponentDriftService(analysis_manager, query_manager)


UsersCRUDDep = Annotated[CRUDUser, Depends(get_users_crud)]
QueryManagerClientDep = Annotated[QueryManagerClient, Depends(get_query_manager_client)]
AnalysisManagerDep = Annotated[AnalysisManager, Depends(get_analysis_manager)]
ComponentDriftServiceDep = Annotated[ComponentDriftService, Depends(get_component_drift_service)]
