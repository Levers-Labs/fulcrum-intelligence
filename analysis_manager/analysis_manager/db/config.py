from asyncio import current_task
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import async_scoped_session, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session, create_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from analysis_manager.config import get_settings

# Used to load models for migrations migrations
MODEL_PATHS = ["analysis_manager.patterns.models"]


# sync session
def get_session() -> Generator[Session, None, None]:
    settings = get_settings()
    engine = create_engine(str(settings.DATABASE_URL), **settings.SQLALCHEMY_ENGINE_OPTIONS)
    with Session(engine) as session:
        yield session


# async session
async def get_async_session_gen() -> AsyncGenerator[AsyncSession, None]:
    settings = get_settings()
    async_engine = create_async_engine(str(settings.DATABASE_URL), **settings.SQLALCHEMY_ENGINE_OPTIONS)
    async_session = async_scoped_session(
        sessionmaker(bind=async_engine, autocommit=False, autoflush=False, class_=AsyncSession), scopefunc=current_task  # type: ignore[call-overload]
    )
    async with async_session() as session:
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
