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
from levers.models import BasePattern

logger = logging.getLogger(__name__)

PatternResultType = TypeVar("PatternResultType", bound=BasePattern)


class PatternCRUD(CRUDBase[PatternResult, PatternResultType, PatternResultType, Any]):
    """CRUD operations for pattern results."""

    async def store_pattern_result(self, pattern_name: str, pattern_result: PatternResultType) -> PatternResult:
        """
        Store a pattern result in the database.

        Args:
            pattern_name: The name of the pattern
            pattern_result: The pattern result pydantic model from levers

        Returns:
            The created pattern result database model
        """
        # Convert the pattern result to a dictionary for JSON storage
        run_result = pattern_result.model_dump(mode="json")

        try:
            # Create a new pattern result
            db_result = self.model(
                metric_id=pattern_result.metric_id,
                pattern=pattern_name,
                version=pattern_result.version,
                analysis_date=pattern_result.analysis_date,
                analysis_window=run_result["analysis_window"],
                error=pattern_result.error,
                run_result=run_result,
            )

            self.session.add(db_result)
            await self.session.commit()
            await self.session.refresh(db_result)

            return db_result
        except SQLAlchemyError as e:
            await self.session.rollback()
            logger.error("Error storing pattern result: %s", str(e))
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
        pattern_name: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """
        Clear pattern results data for a metric.

        Args:
            metric_id: The metric ID
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
