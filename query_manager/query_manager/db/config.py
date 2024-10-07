from collections.abc import AsyncGenerator, Generator
from typing import Annotated

from fastapi import Depends
from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.session import get_async_session as _get_async_session, get_session as _get_session
from query_manager.config import get_settings

# Used to load models for migrations
MODEL_PATHS = ["query_manager.core.models"]


# sync session
def get_session() -> Generator[Session, None, None]:
    settings = get_settings()
    with _get_session(settings.DATABASE_URL, settings.SQLALCHEMY_ENGINE_OPTIONS) as session:  # type: ignore
        yield session


# async session
async def get_async_session_gen() -> AsyncGenerator[AsyncSession, None]:
    settings = get_settings()
    async for session in _get_async_session(settings.DATABASE_URL, settings.SQLALCHEMY_ENGINE_OPTIONS):  # type: ignore
        yield session


async def get_async_session() -> AsyncSession | None:
    db_session = None
    async for _db_session in get_async_session_gen():
        db_session = _db_session
        continue
    return db_session


# Session Dependency
SessionDep = Annotated[Session, Depends(get_session)]
AsyncSessionDep = Annotated[AsyncSession, Depends(get_async_session_gen)]
