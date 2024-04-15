from collections.abc import AsyncGenerator, Generator
from typing import Annotated

from fastapi import Depends
from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.session import get_async_session as _get_async_session, get_session as _get_session
from story_manager.config import get_settings

# Used to load models for alembic migrations
MODEL_PATHS = ["story_manager.core.models"]


# sync session
def get_session() -> Generator[Session, None, None]:
    settings = get_settings()
    return _get_session(settings.DATABASE_URL, settings.SQLALCHEMY_ENGINE_OPTIONS)


# async session
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    settings = get_settings()
    return _get_async_session(settings.DATABASE_URL, settings.SQLALCHEMY_ENGINE_OPTIONS)


# Session Dependency
SessionDep = Annotated[Session, Depends(get_session)]
AsyncSessionDep = Annotated[AsyncSession, Depends(get_async_session)]
