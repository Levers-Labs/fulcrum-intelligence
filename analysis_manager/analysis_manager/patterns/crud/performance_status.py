"""CRUD operations for performance status pattern results."""

import logging

from analysis_manager.patterns.crud.base import PatternCRUD
from analysis_manager.patterns.models.performance_status import PerformanceStatus as PerformanceStatusModel
from levers.models.patterns.performance_status import MetricPerformance

logger = logging.getLogger(__name__)


class PerformanceStatusCRUD(PatternCRUD[PerformanceStatusModel, MetricPerformance]):
    """CRUD operations for performance status pattern results."""

    def __init__(self, session):
        super().__init__(PerformanceStatusModel, session, MetricPerformance)
