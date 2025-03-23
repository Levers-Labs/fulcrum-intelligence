"""CRUD operations for performance status pattern results."""

import logging
from typing import cast

from analysis_manager.patterns.crud.base import PatternCRUD
from analysis_manager.patterns.models.performance_status import PerformanceStatus as PerformanceStatusModel
from levers.models.patterns.performance_status import MetricGVAStatus, MetricPerformance

logger = logging.getLogger(__name__)


class PerformanceStatusCRUD(PatternCRUD[PerformanceStatusModel, MetricPerformance]):
    """CRUD operations for performance status pattern results."""

    def __init__(self, session):
        super().__init__(PerformanceStatusModel, session, MetricPerformance)

    async def get_by_status(
        self, status: MetricGVAStatus, limit: int = 20, offset: int = 0
    ) -> list[PerformanceStatusModel]:
        """Get metrics with a specific status."""
        query = (
            self.get_select_query()
            .where(self.model.status == status)  # type: ignore
            .order_by(self.model.evaluation_time.desc())  # type: ignore
            .offset(offset)
            .limit(limit)
        )

        result = await self.session.execute(query)
        return cast(list[PerformanceStatusModel], result.scalars().all())
