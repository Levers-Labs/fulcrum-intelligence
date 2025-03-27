"""Base CRUD operations for pattern results."""

import logging
from datetime import date
from typing import (
    Any,
    Generic,
    TypeVar,
    cast,
)

from sqlalchemy import (
    and_,
    delete,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import Select
from sqlmodel.ext.asyncio.session import AsyncSession

from analysis_manager.patterns.models.base import BasePatternResult as AnalysisPatternResult
from commons.utilities.context import get_tenant_id
from levers.models.common import BasePattern

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=AnalysisPatternResult)
P = TypeVar("P", bound=BasePattern)


class PatternCRUD(Generic[T, P]):
    """Base CRUD operations for pattern results."""

    BATCH_SIZE = 100

    def __init__(self, model: type[T], session: AsyncSession, pattern_model: type[P]):
        self.model = model
        self.session = session
        self.pattern_model = pattern_model

    def get_select_query(self) -> Select:
        """Get the base select query."""
        return select(self.model)

    async def store_pattern_result(self, pattern_result: P) -> T:
        """
        Store a pattern result.

        Args:
            pattern_result: The pattern result from levers

        Returns:
            The created database model
        """
        # Store raw pattern result for future-proofing
        raw_result = pattern_result.model_dump()

        # Get tenant_id from context if not provided
        tenant_id = get_tenant_id()
        metric_id = raw_result["metric_id"]
        pattern = raw_result["pattern"]
        analysis_date = raw_result.get("analysis_date", date.today())

        # Prepare data for upsert
        raw_result["tenant_id"] = tenant_id

        try:
            # Create an upsert statement using PostgreSQL's INSERT ... ON CONFLICT
            stmt = insert(self.model).values(**raw_result)

            # Define the conflict target (unique constraint columns)
            stmt = stmt.on_conflict_do_update(
                index_elements=["metric_id", "tenant_id", "pattern", "analysis_date"],
                set_={k: getattr(stmt.excluded, k) for k in raw_result if hasattr(self.model, k)},
            )

            # Execute the upsert
            await self.session.execute(stmt)
            await self.session.commit()

            # Fetch the upserted record
            query = self.get_select_query().where(
                and_(
                    self.model.tenant_id == tenant_id,  # type: ignore
                    self.model.metric_id == metric_id,  # type: ignore
                    self.model.pattern == pattern,  # type: ignore
                    self.model.analysis_date == analysis_date,  # type: ignore
                )
            )
            result = await self.session.execute(query)
            db_obj = result.scalar_one()

            return cast(T, db_obj)
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error("Error storing pattern result: %s", e)
            raise

    async def to_pattern_result(self, db_model: T) -> P:
        """
        Convert a database model to a pattern result.

        Args:
            db_model: The database model

        Returns:
            The pattern result
        """
        return db_model.to_pattern_model()

    async def get_by_metric(self, metric_id: str, limit: int = 20, offset: int = 0) -> list[T]:
        """Get pattern results for a specific metric."""
        query = (
            self.get_select_query()
            .where(self.model.metric_id == metric_id)  # type: ignore
            .order_by(self.model.analysis_date.desc())  # type: ignore
            .offset(offset)
            .limit(limit)
        )

        result = await self.session.execute(query)
        return cast(list[T], result.scalars().all())

    async def get_latest_for_metric(self, metric_id: str) -> T | None:
        """Get the latest pattern result for a metric."""
        query = (
            self.get_select_query()
            .where(self.model.metric_id == metric_id)  # type: ignore
            .order_by(self.model.analysis_date.desc())  # type: ignore
            .limit(1)
        )

        result = await self.session.execute(query)
        result = result.scalar_one_or_none()  # type: ignore
        return cast(T | None, result)

    async def get_by_metric_and_date(self, metric_id: str, analysis_date: date) -> T | None:
        """Get a pattern result for a specific metric and date."""
        query = (
            self.get_select_query()
            .where(self.model.metric_id == metric_id)  # type: ignore
            .where(self.model.analysis_date == analysis_date)  # type: ignore
        )

        result = await self.session.execute(query)
        result = result.scalar_one_or_none()  # type: ignore
        return cast(T | None, result)

    async def clear_data(
        self,
        metric_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """Clear pattern results data for a metric."""
        logger.info(f"Clearing pattern data for metric {metric_id} " f"date range: {start_date} to {end_date}")

        try:
            conditions = [self.model.metric_id == metric_id]

            if start_date:
                conditions.append(self.model.analysis_date >= start_date)
            if end_date:
                conditions.append(self.model.analysis_date <= end_date)

            stmt = delete(self.model).where(and_(*conditions))  # type: ignore
            result = await self.session.execute(stmt)
            await self.session.commit()

            rows_deleted = result.rowcount  # type: ignore
            logger.info("Successfully cleared %d records", rows_deleted)

            return {
                "success": True,
                "rows_deleted": rows_deleted,
                "message": f"Successfully cleared {rows_deleted} records",
            }
        except SQLAlchemyError as e:
            logger.error("Failed to clear data: %s", e)
            await self.session.rollback()
            return {"success": False, "rows_deleted": 0, "error": str(e)}

    async def bulk_upsert(self, objects: list[dict[str, Any]], batch_size: int | None = None) -> dict[str, Any]:
        """
        Bulk upsert pattern results.

        Args:
            objects: List of pattern result objects to upsert
            batch_size: Optional custom batch size

        Returns:
            Dict with operation statistics
        """
        if not objects:
            return {"processed": 0, "failed": 0, "total": 0}

        # Inject tenant_id
        tenant_id = get_tenant_id()
        objects = [{"tenant_id": tenant_id, **obj} for obj in objects]

        # Initialize stats
        stats = {"processed": 0, "failed": 0, "total": len(objects)}

        # Set batch size
        batch_size = batch_size or self.BATCH_SIZE

        try:
            for i in range(0, len(objects), batch_size):
                batch = objects[i : i + batch_size]
                try:
                    # Define unique constraint fields
                    unique_fields = [
                        "tenant_id",
                        "metric_id",
                        "pattern",
                        "analysis_date",
                    ]

                    # Create the insert statement
                    stmt = insert(self.model).values(batch)

                    # Define update dict
                    update_dict = {
                        c.name: stmt.excluded[c.name]
                        for c in self.model.__table__.columns  # type: ignore
                        if c.name not in unique_fields and c.name != "id"
                    }
                    update_dict["updated_at"] = func.now()

                    # Define the on_conflict_do_update
                    stmt = stmt.on_conflict_do_update(index_elements=unique_fields, set_=update_dict)

                    # Execute the statement
                    await self.session.execute(stmt)
                    stats["processed"] += len(batch)
                except SQLAlchemyError as e:
                    stats["failed"] += len(batch)
                    logger.error("Failed to process batch: %s", e)
                    continue

            await self.session.flush()
            return stats
        except Exception as e:
            logger.error("Bulk upsert operation failed: %s", e)
            await self.session.rollback()
            raise
