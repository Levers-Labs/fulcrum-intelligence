from asyncio import current_task
from collections.abc import AsyncGenerator, Generator
from typing import Any

from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_scoped_session
from sqlalchemy.orm import sessionmaker
from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.engine import get_async_engine, get_engine

# def apply_global_filter(select_stmt: select) -> select:
#     # print("\n\n\n\n\n", select_stmt, "\n\n\n\n")
#     # if not hasattr(select_stmt, '_filter_applied'):
#     # '_where_criteria', '_whereclause'
#     # 'is_delete', 'is_derived_from', 'is_dml', 'is_insert', 'is_select'
#     # 'filter_by', 'from_statement', 'froms'
#     select_stmt = select_stmt.where(and_("id" == 1))
#         # select_stmt._filter_applied = True
#     return select_stmt


def before_execute(conn, clauseelement, multiparams, params):
    if clauseelement.is_select or clauseelement.is_delete:
        table = clauseelement.froms[0]
        id_column = table.columns.get("id")
        clauseelement = clauseelement.where(id_column == 1)

    elif clauseelement.is_insert:
        params["tenant_id"] = 2

    return clauseelement, multiparams, params


def get_session(database_url: str, options: dict[str, Any]) -> Generator[Session, None, None]:
    engine = get_engine(database_url, options)
    # event.listen(engine, "before_execute", before_execute)
    with Session(engine) as session:
        yield session


async def get_async_session(database_url: str, options: dict[str, Any]) -> AsyncGenerator[AsyncSession, None]:
    engine = get_async_engine(database_url, options)
    event.listen(engine.sync_engine, "before_execute", before_execute, retval=True)
    async_session_factory = sessionmaker(
        bind=engine, class_=AsyncSession, autoflush=False, autocommit=False
    )  # type: ignore[call-overload]
    async_session = async_scoped_session(async_session_factory, scopefunc=current_task)
    async with async_session() as session:
        yield session
