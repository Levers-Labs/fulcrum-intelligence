from __future__ import annotations

from typing import Annotated

from fastapi import Depends

from commons.auth.auth import Auth
from insights_backend.config import get_settings
from insights_backend.core.auth_extended import AuthExtended
from insights_backend.core.crud import CRUDUser
from insights_backend.core.models import User
from insights_backend.db.config import AsyncSessionDep


async def get_users_crud(session: AsyncSessionDep) -> CRUDUser:
    return CRUDUser(model=User, session=session)


def get_security_obj() -> Auth:
    settings = get_settings()
    return AuthExtended(
        auth0_domain=settings.AUTH0_DOMAIN,
        auth0_algorithms=settings.AUTH0_ALGORITHMS,
        auth0_issuer=settings.AUTH0_ISSUER,
        auth0_api_audience=settings.AUTH0_API_AUDIENCE,
        insights_backend_host=settings.SERVER_HOST,
    )


UsersCRUDDep = Annotated[CRUDUser, Depends(get_users_crud)]
