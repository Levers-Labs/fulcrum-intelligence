from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session, create_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from app.config import settings

# Used to load models for alembic migrations
MODEL_PATHS = ["app.db.models", "app.core.models"]

engine = create_engine(str(settings.DATABASE_URL), **settings.SQLALCHEMY_ENGINE_OPTIONS)
async_engine = create_async_engine(str(settings.DATABASE_URL), **settings.SQLALCHEMY_ENGINE_OPTIONS)


# sync session
def get_session() -> Session:
    with Session(engine) as session:
        yield session


# async session
async def get_async_session() -> AsyncSession:
    async_session = sessionmaker(bind=async_engine, class_=AsyncSession, expire_on_commit=False)  # noqa
    async with async_session() as session:
        yield session


# Session Dependency
SessionDep = Annotated[Session, Depends(get_session)]
AsyncSessionDep = Annotated[Session, Depends(get_async_session)]
