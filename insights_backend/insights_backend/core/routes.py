from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from commons.db.session import get_async_session

from .models import Users

router = APIRouter(prefix="/insights-backend", tags=["insights-backend"])


class UserBase(BaseModel):
    name: str
    provider: str
    external_user_id: str
    profile_pic: str | None = None


class UsersInDB(UserBase):
    id: int


@router.post("/create_user", response_model=UsersInDB)
async def create_user(user: UserBase):
    db: AsyncSession = Depends(get_async_session)
    db_user = Users(**user.dict())
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user


@router.get("/get_user_by_id/{user_id}", response_model=UsersInDB)
async def get_user_by_id(user_id: int):
    db: AsyncSession = Depends(get_async_session)
    statement = select(UsersInDB).where(UsersInDB.id == user_id)  # type: ignore
    result = await db.execute(statement)
    db_user = result.scalar_one_or_none()
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user


@router.get("/get_user_by_email/{user_email}", response_model=UsersInDB)
async def get_user_by_email(email: str):
    db: AsyncSession = Depends(get_async_session)
    statement = select(Users).where(UsersInDB.email == email)  # type: ignore
    result = await db.execute(statement)
    db_user = result.scalar_one_or_none()
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return db_user


# @router.put("/users/{user_id}", response_model=UsersInDB)
# async def update_user(user_id: int, user: UserBase, db: AsyncSession = Depends(get_async_session)):
#     db_user = await db.query(Users).filter(Users.id == user_id).first()
#     if db_user is None:
#         raise HTTPException(status_code=404, detail="User not found")
#     for key, value in user.dict(exclude_unset=True).items():
#         setattr(db_user, key, value)
#     await db.commit()
#     await db.refresh(db_user)
#     return db_user
#
#
# @router.get("/users/", response_model=list[UsersInDB])
# async def get_all_users(db: AsyncSession = Depends(get_async_session)):
#     db_users = await db.execute(AsyncSession.query(Users).all())
#     return db_users.fetchall()
