from __future__ import annotations

from typing import Annotated

from fastapi import Depends

from commons.auth.auth import Auth
from commons.clients.insight_backend import InsightBackendClient as InsightBackend
from insights_backend.config import get_settings
from insights_backend.core.crud import CRUDUser
from insights_backend.core.models import User
from insights_backend.db.config import AsyncSessionDep, get_async_session


class InsightsBackendClient(InsightBackend):
    """
    Extending the class InsightBackendClient from common, for overriding the behaviour of get_user method
    all the other services are using the get_user endpoint of insights_user for auth, which is an over the network
    call, but in case of insights backend we are directly fetching the details from DB, instead of calling its own
    rest api endpoint.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    async def get_user(self, user_id, token=None):
        async for session in get_async_session():
            user_crud_client = await get_users_crud(session=session)
            return await user_crud_client.get(id=user_id)


async def get_users_crud(session: AsyncSessionDep) -> CRUDUser:
    return CRUDUser(model=User, session=session)


def get_insight_backend_client() -> InsightBackend:
    settings = get_settings()
    insights_backend_client = InsightsBackendClient(settings.SERVER_HOST)
    return insights_backend_client


def get_security_obj() -> Auth:
    settings = get_settings()
    return Auth(settings, get_insight_backend_client())


UsersCRUDDep = Annotated[CRUDUser, Depends(get_users_crud)]
