from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends
from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession

from analysis_manager.config import get_settings
from commons.db.session import get_async_session as _get_async_session, get_session as _get_session

# Used to load models for migrations migrations
MODEL_PATHS = ["analysis_manager.patterns.models"]


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


@asynccontextmanager
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Get an async session
    """
    session = None
    try:
        async for _db_session in get_async_session_gen():
            session = _db_session
            yield session
            if session:
                await session.commit()
    except Exception:
        if session:
            await session.rollback()
        raise
    finally:
        if session:
            await session.close()


# Session Dependency
SessionDep = Annotated[Session, Depends(get_session)]
AsyncSessionDep = Annotated[AsyncSession, Depends(get_async_session_gen)]
