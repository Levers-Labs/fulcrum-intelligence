import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from commons.utilities.pagination import PaginationParams
from insights_backend.core.dependencies import UsersCRUDDep
from insights_backend.core.models import User, UserCreate, UserList

user_router = APIRouter(prefix="/users", tags=["users"])
logger = logging.getLogger(__name__)


@user_router.post("/", response_model=User)
async def create_user(user: UserCreate, user_crud_client: UsersCRUDDep) -> User:
    """
    To create a new user in DB, this endpoint will be used by Auth0 for user registration.
    """
    return await user_crud_client.create(obj_in=user)


@user_router.get("/{user_id}", response_model=User)
async def get_user(user_id: int, user_crud_client: UsersCRUDDep) -> User:
    """
    Retrieve a user by ID.
    """
    user: User = await user_crud_client.get(user_id)
    return user


@user_router.get("/user-by-email/{email}", response_model=User)
async def get_user_by_email(email: str, user_crud_client: UsersCRUDDep) -> User:
    """
    Retrieve a user by email.
    """
    db_user = await user_crud_client.get_user_by_email(email)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user


@user_router.put("/{user_id}", response_model=User)
async def update_user(user_id: int, user: UserCreate, user_crud_client: UsersCRUDDep) -> User:
    """
    Update a user by ID.
    """
    old_user_obj: User = await user_crud_client.get(user_id)
    if old_user_obj is None:
        raise HTTPException(status_code=404, detail="User not found")

    return await user_crud_client.update(obj=old_user_obj, obj_in=user)


@user_router.get("/", response_model=UserList)
async def list_users(
    user_crud_client: UsersCRUDDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
) -> UserList:
    """
    Retrieve all the users in DB.
    """
    count = await user_crud_client.total_count()
    results: list[User] = [User.from_orm(user) for user in await user_crud_client.list_results(params=params)]
    return UserList(results=results, count=count)
