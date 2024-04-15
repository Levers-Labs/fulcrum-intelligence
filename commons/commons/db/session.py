from asyncio import current_task
from collections.abc import AsyncGenerator, Generator
from typing import Any

from sqlalchemy.ext.asyncio import async_scoped_session
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.engine import get_async_engine, get_engine


def get_session(database_url: str, options: dict[str, Any]) -> Generator[Session, None, None]:
    engine = get_engine(database_url, options)
    with Session(engine) as session:
        yield session


async def get_async_session(database_url: str, options: dict[str, Any]) -> AsyncGenerator[AsyncSession, None]:
    engine = get_async_engine(database_url, options)
    async_session_factory = sessionmaker(bind=engine, class_=AsyncSession, autoflush=False, autocommit=False)  # type: ignore[call-overload]
    async_session = async_scoped_session(async_session_factory, scopefunc=current_task)
    async with async_session() as session:
        yield session
