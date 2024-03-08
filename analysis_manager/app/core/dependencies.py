from typing import Annotated

from fastapi import Depends

from app.core.crud import CRUDUser
from app.core.models import User
from app.db.config import AsyncSessionDep


async def get_users_crud(session: AsyncSessionDep) -> CRUDUser:
    return CRUDUser(model=User, session=session)


UsersCRUDDep = Annotated[CRUDUser, Depends(get_users_crud)]
