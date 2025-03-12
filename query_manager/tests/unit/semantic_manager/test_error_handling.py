"""Tests for error handling in the semantic manager."""

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from query_manager.semantic_manager.crud import (
    CRUDMetricDimensionalTimeSeries,
    CRUDMetricTarget,
    CRUDMetricTimeSeries,
    SemanticManager,
)
from query_manager.semantic_manager.models import MetricDimensionalTimeSeries, MetricTarget, MetricTimeSeries

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def semantic_manager(db_session: AsyncSession) -> SemanticManager:
    """Fixture for SemanticManager instance."""
    return SemanticManager(db_session)


@pytest_asyncio.fixture
async def metric_time_series_crud(db_session: AsyncSession) -> CRUDMetricTimeSeries:
    """Fixture for CRUDMetricTimeSeries instance."""
    return CRUDMetricTimeSeries(MetricTimeSeries, db_session)


@pytest_asyncio.fixture
async def metric_dimensional_time_series_crud(db_session: AsyncSession) -> CRUDMetricDimensionalTimeSeries:
    """Fixture for CRUDMetricDimensionalTimeSeries instance."""
    return CRUDMetricDimensionalTimeSeries(MetricDimensionalTimeSeries, db_session)


@pytest_asyncio.fixture
async def metric_target_crud(db_session: AsyncSession) -> CRUDMetricTarget:
    """Fixture for CRUDMetricTarget instance."""
    return CRUDMetricTarget(MetricTarget, db_session)


async def test_clear_data_sql_error(metric_time_series_crud: CRUDMetricTimeSeries):
    """Test error handling during clear_data when a SQL error occurs."""
    # Mock the execute method to raise a SQLAlchemyError
    with patch.object(metric_time_series_crud.session, "execute", side_effect=SQLAlchemyError("Test SQL error")):
        # Mock the rollback method to avoid actual rollback
        with patch.object(metric_time_series_crud.session, "rollback", AsyncMock()):
            result = await metric_time_series_crud.clear_data(
                tenant_id=1,
                metric_id="test_metric",
                grain=Granularity.DAY,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
            )

            # Verify error is captured in the result
            assert result["success"] is False
            assert "Test SQL error" in result["error"]


async def test_delete_targets_sql_error(metric_target_crud: CRUDMetricTarget):
    """Test error handling during delete_targets when a SQL error occurs."""
    set_tenant_id(1)

    # Mock the execute method to raise a SQLAlchemyError
    with patch.object(metric_target_crud.session, "execute", side_effect=SQLAlchemyError("Test SQL error")):
        # Mock the rollback method to avoid actual rollback
        with patch.object(metric_target_crud.session, "rollback", AsyncMock()):
            with pytest.raises(SQLAlchemyError):
                await metric_target_crud.delete_targets(
                    metric_id="test_metric",
                    grain=Granularity.DAY,
                )
