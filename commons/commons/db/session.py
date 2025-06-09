import logging
from asyncio import current_task
from collections.abc import AsyncGenerator, Generator
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_scoped_session
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.engine import get_async_engine, get_engine
from commons.utilities.context import get_tenant_id

logger = logging.getLogger(__name__)


def get_session(database_url: str, options: dict[str, Any]) -> Generator[Session, None, None]:
    engine = get_engine(database_url, options)
    with Session(engine, expire_on_commit=False) as session:
        yield session


async def get_async_session(database_url: str, options: dict[str, Any]) -> AsyncGenerator[AsyncSession, None]:
    engine = get_async_engine(database_url, options)
    async_session_factory = sessionmaker(
        bind=engine, class_=AsyncSession, autoflush=False, autocommit=False, expire_on_commit=False
    )  # type: ignore[call-overload]
    async_session = async_scoped_session(async_session_factory, scopefunc=current_task)
    async with async_session() as session:
        # If tenant id is set, Set tenant id context in the db session
        if tenant_id := get_tenant_id():
            logger.debug("Setting tenant context in db session. Tenant ID: %s", tenant_id)
            query = text(f"SET app.current_tenant={tenant_id};")
            logger.debug("Setting session role to tenant_user")
            await session.execute(text("SET SESSION ROLE tenant_user;"))
            await session.execute(query)
        logger.debug("yield session for tenant_id: %s", tenant_id)
        yield session
        logger.debug("Session context exited for tenant_id: %s", tenant_id)
