"""Generic pattern CRUD operations."""

import logging
from datetime import date
from typing import Any, TypeVar, cast

from sqlalchemy import (
    and_,
    delete,
    desc,
    select,
)
from sqlalchemy.exc import SQLAlchemyError

from analysis_manager.patterns.models import PatternResult
from commons.db.crud import CRUDBase
from commons.utilities.context import get_tenant_id
from levers.models import BasePattern

logger = logging.getLogger(__name__)

PatternResultType = TypeVar("PatternResultType", bound=BasePattern)


class PatternCRUD(CRUDBase[PatternResult, PatternResultType, PatternResultType, Any]):
    """CRUD operations for pattern results."""

    async def store_pattern_result(self, pattern_name: str, pattern_result: PatternResultType) -> PatternResult:
        """
        Store a pattern result in the database (upsert operation).

        Args:
            pattern_name: The name of the pattern
            pattern_result: The pattern result pydantic model from levers

        Returns:
            The created or updated pattern result database model
        """
        # Convert the pattern result to a dictionary for JSON storage
        run_result = pattern_result.model_dump(mode="json")
        dimension_name = run_result.get("dimension_name")

        try:
            # Get tenant_id from the current context to match the unique constraint
            tenant_id = get_tenant_id()

            logger.debug(
                "Checking for existing pattern result: metric=%s, tenant=%s, pattern=%s, version=%s, date=%s, "
                "grain=%s, dimension=%s",
                pattern_result.metric_id,
                tenant_id,
                pattern_name,
                pattern_result.version,
                pattern_result.analysis_date,
                run_result["analysis_window"]["grain"],
                dimension_name,
            )

            # Check if a record with the same unique constraint values already exists
            existing_query = (
                select(self.model)
                .where(self.model.metric_id == pattern_result.metric_id)  # type: ignore
                .where(self.model.tenant_id == tenant_id)  # type: ignore
                .where(self.model.pattern == pattern_name)  # type: ignore
                .where(self.model.version == pattern_result.version)  # type: ignore
                .where(self.model.analysis_date == pattern_result.analysis_date)  # type: ignore
                .where(self.model.grain == run_result["analysis_window"]["grain"])  # type: ignore
                .where(self.model.dimension_name == dimension_name)  # type: ignore
            )

            result = await self.session.execute(existing_query)
            existing_record = cast(PatternResult | None, result.scalars().first())
            logger.debug(f"Existing record found: {existing_record is not None}")

            if existing_record:
                # Update existing record
                existing_record.analysis_window = run_result["analysis_window"]
                existing_record.error = pattern_result.error
                existing_record.run_result = run_result
                # updated_at will be handled automatically by the base model

                db_result = existing_record
                logger.info(
                    "Updated existing pattern result for metric %s, pattern %s, date %s",
                    pattern_result.metric_id,
                    pattern_name,
                    pattern_result.analysis_date,
                )
            else:
                # Create a new pattern result
                db_result = self.model(
                    metric_id=pattern_result.metric_id,
                    pattern=pattern_name,
                    version=pattern_result.version,
                    grain=run_result["analysis_window"]["grain"],
                    dimension_name=dimension_name,
                    analysis_date=pattern_result.analysis_date,
                    analysis_window=run_result["analysis_window"],
                    error=pattern_result.error,
                    run_result=run_result,
                )

                self.session.add(db_result)
                logger.info(
                    "Created new pattern result for metric %s, pattern %s, date %s",
                    pattern_result.metric_id,
                    pattern_name,
                    pattern_result.analysis_date,
                )

            await self.session.commit()
            await self.session.refresh(db_result)
            logger.debug("Successfully committed transaction")

            return db_result

        except Exception as e:
            logger.error("Error in store_pattern_result: %s", str(e), exc_info=True)
            # Only rollback if the session is not already in a bad state
            if self.session.is_active:
                await self.session.rollback()
            raise

    async def get_latest_for_metric(self, pattern_name: str, metric_id: str) -> PatternResult | None:
        """
        Get latest pattern result for a metric.

        Args:
            pattern_name: The name of the pattern
            metric_id: The metric ID

        Returns:
            The latest pattern result or None
        """
        stmt = (
            select(self.model)
            .where(self.model.metric_id == metric_id)  # type: ignore
            .where(self.model.pattern == pattern_name)  # type: ignore
            .order_by(desc(self.model.analysis_date))  # type: ignore
            .limit(1)
        )

        result = await self.session.execute(stmt)
        return cast(PatternResult, result.scalars().first())

    async def get_results_for_metric(
        self, pattern_name: str, metric_id: str, limit: int = 100, offset: int = 0
    ) -> list[PatternResult]:
        """
        Get multiple pattern results for a metric.

        Args:
            pattern_name: The name of the pattern
            metric_id: The metric ID
            limit: Maximum number of results to return
            offset: Number of results to skip

        Returns:
            List of pattern results ordered by analysis date descending
        """
        # Prepare the query
        stmt = (
            select(self.model)
            .where(self.model.metric_id == metric_id)  # type: ignore
            .where(self.model.pattern == pattern_name)  # type: ignore
            .order_by(desc(self.model.analysis_date))  # type: ignore
            .limit(limit)
            .offset(offset)
        )

        result = await self.session.execute(stmt)
        return cast(list[PatternResult], result.scalars().all())

    def to_pattern_model(self, db_result: PatternResult) -> Any:
        """
        Convert database model to pattern model.

        Args:
            db_result: The database model

        Returns:
            The pattern model instance
        """
        return db_result.to_pattern_model()

    async def clear_data(
        self,
        metric_id: str,
        dimension_name: str | None = None,
        pattern_name: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """
        Clear pattern results data for a metric.

        Args:
            metric_id: The metric ID
            dimension_name: Optional dimension name to filter by
            pattern_name: Optional pattern name to filter by
            start_date: Optional start date range
            end_date: Optional end date range

        Returns:
            Dictionary with operation results
        """
        logger.info(
            "Clearing pattern data for metric %s, pattern %s, date range: %s to %s",
            metric_id,
            pattern_name,
            start_date,
            end_date,
        )

        try:
            conditions = [self.model.metric_id == metric_id]  # type: ignore

            if dimension_name:
                conditions.append(self.model.dimension_name == dimension_name)  # type: ignore

            if pattern_name:
                conditions.append(self.model.pattern == pattern_name)  # type: ignore
            if start_date:
                conditions.append(self.model.analysis_date >= start_date)  # type: ignore
            if end_date:
                conditions.append(self.model.analysis_date <= end_date)  # type: ignore

            # Create a delete statement based on the same conditions
            delete_stmt = delete(self.model).where(and_(*conditions))  # type: ignore
            result = await self.session.execute(delete_stmt)
            rows_deleted = result.rowcount  # type: ignore

            await self.session.commit()

            logger.info("Successfully cleared %d records", rows_deleted)

            return {"success": True, "rows_deleted": rows_deleted}
        except SQLAlchemyError as e:
            logger.error("Failed to clear data: %s", str(e))
            await self.session.rollback()
            return {"success": False, "rows_deleted": 0, "error": str(e)}
