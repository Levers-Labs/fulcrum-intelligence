from __future__ import annotations

from typing import Annotated

from fastapi import Depends

from insights_backend.core.crud import CRUDUser
from insights_backend.core.models.users import User
from insights_backend.db.config import AsyncSessionDep


async def get_users_crud(session: AsyncSessionDep) -> CRUDUser:
    return CRUDUser(model=User, session=session)


UsersCRUDDep = Annotated[CRUDUser, Depends(get_users_crud)]
