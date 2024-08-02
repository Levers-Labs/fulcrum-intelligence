from __future__ import annotations

from typing import Annotated

from fastapi import Depends

from insights_backend.auth import InsightsBackendAuth
from insights_backend.config import get_settings
from insights_backend.core.crud import CRUDUser
from insights_backend.core.models import User
from insights_backend.db.config import AsyncSessionDep


async def get_users_crud(session: AsyncSessionDep) -> CRUDUser:
    return CRUDUser(model=User, session=session)


UsersCRUDDep = Annotated[CRUDUser, Depends(get_users_crud)]


def oauth2_auth(user_crud: UsersCRUDDep) -> InsightsBackendAuth:
    settings = get_settings()
    return InsightsBackendAuth(
        issuer=settings.AUTH0_ISSUER,
        api_audience=settings.AUTH0_API_AUDIENCE,
        user_crud=user_crud,
    )
