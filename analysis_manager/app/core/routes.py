from typing import Any

from fastapi import APIRouter

from app.core.dependencies import UsersCRUDDep
from app.core.models import User, UserRead
from app.core.schema import UserList

router = APIRouter(prefix="")


@router.get("/users", response_model=UserList)
async def list_users(users: UsersCRUDDep, offset: int = 0, limit: int = 100) -> Any:
    """
    Retrieve users.
    """
    count = await users.total_count()
    users: list[User] = await users.list(offset=offset, limit=limit)
    return UserList(results=users, count=count)


@router.get("/users/{user_id}", response_model=UserRead)
async def get_user(user_id: int, users: UsersCRUDDep) -> Any:
    """
    Retrieve a user by ID.
    """
    user: User = await users.get(user_id)
    return user
